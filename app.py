from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
)
import json
import math
import subprocess
import sys
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import yfinance as yf

# ------------------------------
# Paths & constants
# ------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Prefer venv python if available, otherwise use current interpreter
VENV_PYTHON = os.path.join(BASE_DIR, 'venv', 'Scripts', 'python.exe')
if not os.path.exists(VENV_PYTHON):
    # fallback to current python interpreter (this ensures we run with an interpreter that has deps)
    VENV_PYTHON = sys.executable

MAIN_SCRIPT = os.path.join(BASE_DIR, 'main.py')
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')
PORTFOLIO_FILE = os.path.join(BASE_DIR, 'portfolio.json')

app = Flask(__name__)
log_output_raw = []   # raw stdout lines from main.py
log_output_table = [] # parsed table (list of dicts) built from Excel result

# ------------------------------
# Ensure config file exists
# ------------------------------
if not os.path.exists(CONFIG_FILE):
    default_config = {
        "DATA_PERIOD": "1y",
        "CUSTOM_START_DATE": "2024-01-01",
        "STOP_LOSS_PERCENTAGE_RANGE": [1, 2],
        "STOP_LOSS_STEP": 0.2,
        "NUM_SIMULATIONS": 10000,
        "CONFIDENCE_LEVEL": 0.95,
        "SPAN_EWMA": 60
    }
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=4)
    print(f"Created default config file: {CONFIG_FILE}")

if not os.path.exists(PORTFOLIO_FILE):
    default_portfolio = {
        "holdings": [
            {
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "quantity": 25,
                "average_cost": 142.35,
                "logo_url": "https://logo.clearbit.com/apple.com"
            },
            {
                "ticker": "MSFT",
                "name": "Microsoft Corp.",
                "quantity": 18,
                "average_cost": 265.40,
                "logo_url": "https://logo.clearbit.com/microsoft.com"
            },
            {
                "ticker": "GOOGL",
                "name": "Alphabet Inc.",
                "quantity": 12,
                "average_cost": 125.15,
                "logo_url": "https://logo.clearbit.com/abc.xyz"
            }
        ]
    }
    with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
        json.dump(default_portfolio, f, indent=4)
    print(f"Created default portfolio file: {PORTFOLIO_FILE}")

# ------------------------------
# Helper functions
# ------------------------------
def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4)


def load_portfolio_state() -> Dict[str, Any]:
    with open(PORTFOLIO_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_portfolio_state(data: Dict[str, Any]) -> None:
    with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_index(series: pd.Series) -> pd.Series:
    """Ensure the index is a timezone-naive DatetimeIndex for comparisons."""
    idx = series.index
    if not isinstance(idx, pd.DatetimeIndex):
        return series
    if idx.tz is not None:
        series.index = idx.tz_localize(None)
    return series


def _historical_close(series: pd.Series, days_back: int) -> float | None:
    """Return the closing price from *approximately* days_back calendar days ago."""
    if series.empty:
        return None

    series = _normalize_index(series)
    last_idx = series.index[-1] if isinstance(series.index, pd.DatetimeIndex) else None
    if last_idx is None:
        # Fallback to positional lookup
        pos = max(len(series) - (days_back + 1), 0)
        return float(series.iloc[pos])

    target_date = last_idx - pd.Timedelta(days=days_back)
    historical = series.loc[series.index <= target_date]
    if not historical.empty:
        return float(historical.iloc[-1])
    return float(series.iloc[0])


def _get_market_snapshot(ticker: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "current_price": None,
        "previous_close": None,
        "week_close": None,
        "month_close": None,
    }
    try:
        ticker_obj = yf.Ticker(ticker)
        fast_info = getattr(ticker_obj, "fast_info", None)
        if fast_info:
            result["current_price"] = fast_info.get("last_price") or fast_info.get("lastPrice")
            result["previous_close"] = fast_info.get("previous_close") or fast_info.get("previousClose")

        history = ticker_obj.history(period="6mo", interval="1d")
        if not history.empty:
            closes = history.get("Close")
            if closes is not None:
                closes = closes.dropna()
                if not closes.empty:
                    closes = _normalize_index(closes)
                    if result["current_price"] is None:
                        result["current_price"] = float(closes.iloc[-1])
                    if result["previous_close"] is None:
                        if len(closes) > 1:
                            result["previous_close"] = float(closes.iloc[-2])
                        else:
                            result["previous_close"] = float(closes.iloc[-1])
                    result["week_close"] = _historical_close(closes, 7)
                    result["month_close"] = _historical_close(closes, 30)
    except Exception as exc:
        print(f"Warning: failed to fetch market data for {ticker}: {exc}")

    return result


def build_portfolio_snapshot(holdings: List[Dict[str, Any]]) -> Dict[str, Any]:
    computed_holdings: List[Dict[str, Any]] = []
    total_cost = 0.0
    total_prev_value = 0.0
    total_week_reference_value = 0.0
    total_month_reference_value = 0.0
    total_current_value = 0.0
    top_mover: Dict[str, Any] | None = None

    for holding in holdings:
        ticker = str(holding.get("ticker", "")).upper().strip()
        quantity = _safe_float(holding.get("quantity"))
        avg_cost = _safe_float(holding.get("average_cost"))

        if not ticker or quantity <= 0:
            continue

        market = _get_market_snapshot(ticker)
        current_price = _safe_float(market.get("current_price"), default=0.0)
        previous_close = _safe_float(market.get("previous_close"), default=current_price)
        week_close = _safe_float(market.get("week_close"), default=previous_close)
        month_close = _safe_float(market.get("month_close"), default=previous_close)

        logo_url = holding.get("logo_url") or None
        name = holding.get("name") or ticker

        total_cost_value = quantity * avg_cost
        current_value = quantity * current_price
        prev_value = quantity * previous_close if previous_close else 0.0
        todays_gain = current_value - prev_value
        todays_gain_pct = (todays_gain / prev_value * 100) if prev_value else 0.0

        weekly_value = quantity * week_close if week_close else 0.0
        weekly_gain = current_value - weekly_value
        weekly_gain_pct = (weekly_gain / weekly_value * 100) if weekly_value else 0.0

        monthly_value = quantity * month_close if month_close else 0.0
        monthly_gain = current_value - monthly_value
        monthly_gain_pct = (monthly_gain / monthly_value * 100) if monthly_value else 0.0

        computed_holdings.append(
            {
                "ticker": ticker,
                "name": name,
                "logo_url": logo_url,
                "quantity": quantity,
                "average_cost": avg_cost,
                "current_price": current_price,
                "total_cost": total_cost_value,
                "current_value": current_value,
                "todays_gain": todays_gain,
                "todays_gain_pct": todays_gain_pct,
                "weekly_gain": weekly_gain,
                "weekly_gain_pct": weekly_gain_pct,
                "monthly_gain": monthly_gain,
                "monthly_gain_pct": monthly_gain_pct,
            }
        )

        total_cost += total_cost_value
        total_prev_value += prev_value
        total_current_value += current_value
        total_week_reference_value += weekly_value
        total_month_reference_value += monthly_value

        change_value = todays_gain
        change_pct = ((current_price - previous_close) / previous_close * 100) if previous_close else 0.0
        mover_metric = abs(change_value)
        if top_mover is None or mover_metric > top_mover.get("metric", 0):
            top_mover = {
                "ticker": ticker,
                "name": name,
                "change_value": change_value,
                "change_pct": change_pct,
                "metric": mover_metric,
            }

    allocation_denominator = total_current_value if total_current_value else 1
    for holding in computed_holdings:
        holding["allocation_pct"] = (
            holding["current_value"] / allocation_denominator * 100 if allocation_denominator else 0.0
        )

    dod_value = total_current_value - total_prev_value
    dod_pct = (dod_value / total_prev_value * 100) if total_prev_value else 0.0
    weekly_change_value = (
        total_current_value - total_week_reference_value
        if total_week_reference_value
        else 0.0
    )
    weekly_change_pct = (
        (weekly_change_value / total_week_reference_value) * 100
        if total_week_reference_value
        else 0.0
    )
    monthly_change_value = (
        total_current_value - total_month_reference_value
        if total_month_reference_value
        else 0.0
    )
    monthly_change_pct = (
        (monthly_change_value / total_month_reference_value) * 100
        if total_month_reference_value
        else 0.0
    )

    summary = {
        "total_cost": total_cost,
        "current_value": total_current_value,
        "dod_value": dod_value,
        "dod_pct": dod_pct,
        "weekly_change_value": weekly_change_value,
        "weekly_change_pct": weekly_change_pct,
        "monthly_change_value": monthly_change_value,
        "monthly_change_pct": monthly_change_pct,
        "top_mover": None,
    }

    if top_mover:
        summary["top_mover"] = {
            "ticker": top_mover.get("ticker"),
            "name": top_mover.get("name"),
            "change_value": top_mover.get("change_value"),
            "change_pct": top_mover.get("change_pct"),
        }

    return {
        "summary": summary,
        "holdings": computed_holdings,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


# ------------------------------
# Portfolio routes & APIs
# ------------------------------
@app.route('/')
def portfolio_analysis():
    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    snapshot = build_portfolio_snapshot(holdings)
    return render_template(
        'portfolio_analysis.html',
        snapshot=snapshot,
        holdings_raw=holdings,
    )


@app.route('/api/portfolio', methods=['GET'])
def api_get_portfolio():
    portfolio_state = load_portfolio_state()
    snapshot = build_portfolio_snapshot(portfolio_state.get('holdings', []))
    return jsonify(snapshot)


@app.route('/api/portfolio', methods=['POST'])
def api_update_portfolio():
    payload = request.get_json(silent=True)
    if not payload or 'holdings' not in payload:
        return jsonify({'error': 'Invalid payload'}), 400

    normalized_holdings: List[Dict[str, Any]] = []
    for entry in payload.get('holdings', []):
        ticker = str(entry.get('ticker', '')).upper().strip()
        quantity = _safe_float(entry.get('quantity'))
        avg_cost = _safe_float(entry.get('average_cost'))
        name = entry.get('name')
        logo_url = entry.get('logo_url')

        if not ticker or quantity <= 0:
            continue

        holding_record: Dict[str, Any] = {
            'ticker': ticker,
            'quantity': quantity,
            'average_cost': avg_cost,
        }
        if name:
            holding_record['name'] = name
        if logo_url:
            holding_record['logo_url'] = logo_url
        # Preserve explicit current price if client provides it
        if entry.get('current_price') is not None:
            holding_record['current_price'] = _safe_float(entry.get('current_price'))

        normalized_holdings.append(holding_record)

    save_portfolio_state({'holdings': normalized_holdings})
    snapshot = build_portfolio_snapshot(normalized_holdings)
    return jsonify({'status': 'ok', 'snapshot': snapshot})

def run_main_script():
    """
    Run main.py synchronously using the venv/python specified (VENV_PYTHON).
    Capture stdout/stderr into log_output_raw and after completion attempt to read the results Excel
    into log_output_table (list of dicts).
    """
    global log_output_raw, log_output_table
    log_output_raw = []
    log_output_table = []

    if not os.path.exists(MAIN_SCRIPT):
        log_output_raw.append(f"ERROR: main script not found at {MAIN_SCRIPT}")
        return

    # Execute main.py
    try:
        process = subprocess.Popen(
            [VENV_PYTHON, MAIN_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=BASE_DIR
        )
    except Exception as e:
        log_output_raw.append(f"Failed to start process: {e}")
        return

    # Stream stdout lines
    for line in process.stdout:
        # store lines (strip trailing newlines for template)
        log_output_raw.append(line.rstrip('\n'))

    process.wait()

    # Once done, attempt to load the result Excel file (if created)
    try:
        config = load_config()
        data_period = config.get('DATA_PERIOD', '1y')
        results_file = os.path.join(BASE_DIR, f'trailing_stop_analysis_ewma_{data_period}.xlsx')
        if os.path.exists(results_file):
            df = pd.read_excel(results_file, engine='openpyxl')
            # convert column names to strings (safe for template)
            df.columns = [str(c) for c in df.columns]
            log_output_table = df.to_dict(orient='records')
        else:
            # no results file found; optionally include note
            log_output_raw.append(f"Note: results file not found: {results_file}")
    except Exception as e:
        log_output_raw.append(f"Error reading results file: {e}")

# ------------------------------
# Routes
# ------------------------------
@app.route('/risk-analysis', methods=['GET', 'POST'])
def risk_analysis():
    config = load_config()
    if request.method == 'POST':
        # update config from form
        # Note: some basic input sanitization/typing
        config['DATA_PERIOD'] = request.form.get('DATA_PERIOD', config.get('DATA_PERIOD', '1y'))
        config['CUSTOM_START_DATE'] = request.form.get('CUSTOM_START_DATE', config.get('CUSTOM_START_DATE', '2024-01-01'))

        try:
            minp = float(request.form.get('STOP_LOSS_MIN', config.get('STOP_LOSS_PERCENTAGE_RANGE', [1,2])[0]))
            maxp = float(request.form.get('STOP_LOSS_MAX', config.get('STOP_LOSS_PERCENTAGE_RANGE', [1,2])[1]))
            config['STOP_LOSS_PERCENTAGE_RANGE'] = [minp, maxp]
        except Exception:
            # keep old values on error
            pass

        try:
            config['STOP_LOSS_STEP'] = float(request.form.get('STOP_LOSS_STEP', config.get('STOP_LOSS_STEP', 0.2)))
        except Exception:
            pass

        try:
            config['NUM_SIMULATIONS'] = int(request.form.get('NUM_SIMULATIONS', config.get('NUM_SIMULATIONS', 10000)))
        except Exception:
            pass

        try:
            config['CONFIDENCE_LEVEL'] = float(request.form.get('CONFIDENCE_LEVEL', config.get('CONFIDENCE_LEVEL', 0.95)))
        except Exception:
            pass

        try:
            config['SPAN_EWMA'] = int(request.form.get('SPAN_EWMA', config.get('SPAN_EWMA', 60)))
        except Exception:
            pass

        save_config(config)
        return redirect(url_for('risk_analysis'))

    # GET
    return render_template(
        'risk_analysis.html',
        config=config,
        log_output_raw=log_output_raw,
        log_output_table=log_output_table,
    )

@app.route('/run', methods=['POST'])
def run():
    # Make sure any previous logs are cleared and show starting message immediately
    global log_output_raw
    log_output_raw = ["Starting calculations..."]
    # Run synchronously (Option 1). Will block until main.py completes.
    run_main_script()
    return redirect(url_for('risk_analysis'))

# ------------------------------
# Run app
# ------------------------------
if __name__ == '__main__':
    app.run(debug=True)
