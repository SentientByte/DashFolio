from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
)
import json
import subprocess
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd

from Calculations.allocations import normalize_target_allocations
from Calculations.snapshot_cache import get_portfolio_snapshot as get_cached_portfolio_snapshot
from Calculations.storage import connect, ensure_risk_results_table
from Calculations.transactions import (
    append_transactions,
    fetch_holdings_with_market_values,
    load_current_holdings,
    load_transactions,
    parse_transactions_csv,
    preview_holdings,
    replace_transactions,
)
from Calculations.utils import safe_float

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
DATA_STORE = os.path.join(BASE_DIR, 'dashfolio.db')

USD_TO_BHD = 0.376081

app = Flask(__name__)
log_output_raw: List[str] = []   # raw stdout lines from main.py
log_output_table: List[Dict[str, Any]] = [] # parsed table (list of dicts) built from database results


def get_currency_context(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if config is None:
        config = load_config()

    currency = str(config.get("CURRENCY", "USD")).upper()
    if currency not in {"USD", "BHD"}:
        currency = "USD"

    rate = USD_TO_BHD if currency == "BHD" else 1.0
    symbol = "BD" if currency == "BHD" else "$"
    return {
        "code": currency,
        "symbol": symbol,
        "rate": rate,
        "symbol_first": True,
    }


def format_currency_value(value: Any, currency_context: Dict[str, Any]) -> str:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        numeric_value = 0.0

    converted = numeric_value * currency_context.get("rate", 1.0)
    symbol = currency_context.get("symbol", "$")
    decimals = currency_context.get("decimals", 2)
    formatted = f"{converted:,.{decimals}f}"
    return f"{symbol}{formatted}" if currency_context.get("symbol_first", True) else f"{formatted}{symbol}"


def format_signed_currency_value(value: Any, currency_context: Dict[str, Any]) -> str:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        numeric_value = 0.0

    prefix = "+" if numeric_value > 0 else ("-" if numeric_value < 0 else "")
    absolute = abs(numeric_value)
    formatted = format_currency_value(absolute, currency_context)
    if prefix:
        return f"{prefix}{formatted}"
    return formatted


def format_snapshot_update(timestamp: Any) -> str:
    try:
        if not timestamp:
            raise ValueError("missing timestamp")
        if isinstance(timestamp, datetime):
            parsed = timestamp.astimezone(timezone.utc)
        else:
            parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
    except Exception:
        return "Updated: Recently"

    now = datetime.now(timezone.utc)
    delta = now - parsed

    if delta.total_seconds() < 60:
        return "Updated: Recently"

    if delta.total_seconds() < 3600:
        minutes = max(int(delta.total_seconds() // 60), 1)
        return f"Updated: {minutes} min ago"

    if delta.total_seconds() < 86400:
        hours = max(int(delta.total_seconds() // 3600), 1)
        suffix = "hour" if hours == 1 else "hours"
        return f"Updated: {hours} {suffix} ago"

    utc_plus_three = timezone(timedelta(hours=3))
    localized = parsed.astimezone(utc_plus_three)
    formatted = localized.strftime("%Y-%m-%d %H:%M:%S")
    return f"Updated: {formatted} UTC+3"


@app.context_processor
def inject_global_helpers():
    config = load_config()
    currency_context = get_currency_context(config)
    return {
        "datetime": datetime,
        "currency_context": currency_context,
        "format_currency": lambda value, ctx=currency_context: format_currency_value(value, ctx),
        "format_signed_currency": lambda value, ctx=currency_context: format_signed_currency_value(value, ctx),
        "format_snapshot_update": format_snapshot_update,
    }

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
        "SPAN_EWMA": 60,
        "BENCHMARK_TICKER": "SPY",
        "CURRENCY": "USD",
        "AUTO_REFRESH_INTERVAL": 60,
    }
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=4)
    print(f"Created default config file: {CONFIG_FILE}")

if not os.path.exists(PORTFOLIO_FILE):
    default_portfolio = {
        "target_allocations": {}
    }
    with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
        json.dump(default_portfolio, f, indent=4)
    print(f"Created default portfolio file: {PORTFOLIO_FILE}")

# ------------------------------
# Helper functions
# ------------------------------
def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)

    defaults = {
        "BENCHMARK_TICKER": "SPY",
        "CURRENCY": "USD",
        "AUTO_REFRESH_INTERVAL": 60,
    }
    updated = False
    for key, value in defaults.items():
        if key not in config:
            config[key] = value
            updated = True

    try:
        interval = int(config.get("AUTO_REFRESH_INTERVAL", 60))
    except (TypeError, ValueError):
        interval = 60
    if interval < 5:
        interval = 5
        updated = True
    elif interval > 900:
        interval = 900
        updated = True
    config["AUTO_REFRESH_INTERVAL"] = interval

    if updated:
        save_config(config)

    return config


def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4)


def load_portfolio_state() -> Dict[str, Any]:
    raw: Dict[str, Any] = {}
    if os.path.exists(PORTFOLIO_FILE):
        with open(PORTFOLIO_FILE, 'r', encoding='utf-8') as f:
            try:
                raw = json.load(f)
            except json.JSONDecodeError:
                raw = {}

    holdings = load_current_holdings(DATA_STORE)
    normalized_targets = normalize_target_allocations(
        holdings,
        raw.get('target_allocations'),
    )
    return {
        'holdings': holdings,
        'target_allocations': normalized_targets,
    }


def save_portfolio_state(data: Dict[str, Any]) -> None:
    payload = {
        'target_allocations': data.get('target_allocations', {}),
    }
    with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=4)


# ------------------------------
# Portfolio routes & APIs
# ------------------------------
@app.route('/')
def portfolio_analysis():
    config = load_config()
    currency_settings = get_currency_context(config)
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')

    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
    )
    return render_template(
        'portfolio_analysis.html',
        snapshot=snapshot,
        active_page='portfolio',
        page_title='Portfolio Overview',
        page_subtitle='Live performance & allocations',
        config=config,
        currency_settings=currency_settings,
        benchmark_ticker=benchmark_ticker,
    )


@app.route('/allocation')
def allocation_planner():
    config = load_config()
    currency_settings = get_currency_context(config)
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')

    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
    )
    return render_template(
        'allocation.html',
        snapshot=snapshot,
        target_allocations=snapshot.get('target_allocations', {}),
        active_page='allocation',
        page_title='Allocation Planner',
        page_subtitle='Rebalance towards your target mix',
        config=config,
        currency_settings=currency_settings,
    )


@app.route('/transactions')
def transactions_page():
    config = load_config()
    currency_settings = get_currency_context(config)
    transactions = load_transactions(DATA_STORE)
    holdings = load_current_holdings(DATA_STORE)
    holdings_summary = fetch_holdings_with_market_values(holdings)

    return render_template(
        'transactions.html',
        transactions=transactions,
        holdings_summary=holdings_summary,
        currency_settings=currency_settings,
        active_page='transactions',
        page_title='Transactions',
        page_subtitle='Upload trades & review derived holdings',
        snapshot=None,
    )


@app.route('/settings')
def settings():
    config = load_config()
    currency_settings = get_currency_context(config)
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')

    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
    )

    return render_template(
        'settings.html',
        snapshot=snapshot,
        target_allocations=targets,
        config=config,
        currency_settings=currency_settings,
        benchmark_ticker=benchmark_ticker,
        log_output_raw=log_output_raw,
        active_page='settings',
        page_title='Settings',
        page_subtitle='Manage portfolio configuration & preferences',
    )


@app.route('/api/portfolio', methods=['GET'])
def api_get_portfolio():
    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    force_refresh = str(request.args.get('force', '')).lower() in {'1', 'true', 'yes'}
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
        refresh_async=not force_refresh,
        force_recompute=force_refresh,
    )
    return jsonify(snapshot)


@app.route('/api/transactions', methods=['GET'])
def api_get_transactions():
    transactions = load_transactions(DATA_STORE)
    holdings = load_current_holdings(DATA_STORE)
    holdings_summary = fetch_holdings_with_market_values(holdings)
    return jsonify({
        'transactions': transactions,
        'holdings': holdings_summary,
    })


@app.route('/api/transactions/save', methods=['POST'])
def api_save_transactions():
    payload = request.get_json(silent=True) or {}
    records = payload.get('transactions')
    if not isinstance(records, list):
        return jsonify({'error': 'Transactions payload must be a list.'}), 400

    try:
        holdings = replace_transactions(DATA_STORE, records)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    # Persist updated allocation targets with the new holdings universe.
    state = load_portfolio_state()
    save_portfolio_state({'target_allocations': state.get('target_allocations', {})})

    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        state.get('holdings', []),
        state.get('target_allocations', {}),
        benchmark_ticker,
        refresh_async=True,
        force_recompute=True,
    )

    holdings_summary = fetch_holdings_with_market_values(state.get('holdings', []))
    return jsonify({
        'status': 'ok',
        'transactions': load_transactions(DATA_STORE),
        'holdings': holdings_summary,
        'snapshot': snapshot,
    })


@app.route('/api/transactions/upload', methods=['POST'])
def api_upload_transactions():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded.'}), 400

    file_storage = request.files['file']
    mode = request.form.get('mode', 'append').lower()
    if mode not in {'append', 'replace'}:
        return jsonify({'error': "Mode must be either 'append' or 'replace'."}), 400

    try:
        content = file_storage.read()
        parsed_records = parse_transactions_csv(content)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    if not parsed_records:
        return jsonify({'error': 'No valid transactions found in CSV.'}), 400

    _, preview_holdings = preview_holdings(DATA_STORE, parsed_records, mode)
    holdings_with_values = fetch_holdings_with_market_values(preview_holdings)

    return jsonify({
        'status': 'preview',
        'mode': mode,
        'uploaded_transactions': parsed_records,
        'preview_holdings': holdings_with_values,
    })


@app.route('/api/transactions/apply', methods=['POST'])
def api_apply_transactions():
    payload = request.get_json(silent=True) or {}
    mode = str(payload.get('mode', 'append')).lower()
    transactions_payload = payload.get('transactions')
    if not isinstance(transactions_payload, list) or not transactions_payload:
        return jsonify({'error': 'No transactions provided for commit.'}), 400

    if mode not in {'append', 'replace'}:
        return jsonify({'error': "Mode must be either 'append' or 'replace'."}), 400

    try:
        if mode == 'replace':
            holdings = replace_transactions(DATA_STORE, transactions_payload)
        else:
            holdings = append_transactions(DATA_STORE, transactions_payload)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    state = load_portfolio_state()
    save_portfolio_state({'target_allocations': state.get('target_allocations', {})})

    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        state.get('holdings', []),
        state.get('target_allocations', {}),
        benchmark_ticker,
        refresh_async=True,
        force_recompute=True,
    )

    holdings_summary = fetch_holdings_with_market_values(state.get('holdings', []))

    return jsonify({
        'status': 'ok',
        'mode': mode,
        'transactions': load_transactions(DATA_STORE),
        'holdings': holdings_summary,
        'snapshot': snapshot,
    })


@app.route('/api/targets', methods=['POST'])
def api_update_targets():
    payload = request.get_json(silent=True) or {}
    target_entries = payload.get('targets', [])

    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    state = load_portfolio_state()
    holdings = state.get('holdings', [])
    if not holdings:
        return jsonify({'error': 'No holdings available to assign targets'}), 400

    valid_tickers = {str(h.get('ticker', '')).upper().strip() for h in holdings if h.get('ticker')}
    proposed_targets: Dict[str, float] = {}

    for entry in target_entries:
        ticker = str(entry.get('ticker', '')).upper().strip()
        if ticker not in valid_tickers:
            continue
        value = safe_float(entry.get('target_pct'))
        if value < 0:
            value = 0.0
        proposed_targets[ticker] = value

    if not proposed_targets:
        return jsonify({'error': 'No valid targets provided'}), 400

    total_target = sum(proposed_targets.values())
    if total_target <= 0:
        return jsonify({'error': 'Total target allocation must be greater than zero'}), 400

    for ticker in valid_tickers:
        proposed_targets.setdefault(ticker, 0.0)

    normalized = normalize_target_allocations(holdings, proposed_targets)
    state['target_allocations'] = normalized
    save_portfolio_state(state)

    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        normalized,
        benchmark_ticker,
        refresh_async=True,
        force_recompute=True,
    )
    return jsonify({'status': 'ok', 'targets': normalized, 'snapshot': snapshot})


@app.route('/api/config', methods=['POST'])
def api_update_config():
    payload = request.get_json(silent=True) or {}
    config = load_config()

    errors: List[str] = []

    if 'benchmark_ticker' in payload:
        ticker = str(payload.get('benchmark_ticker', '')).upper().strip()
        if ticker:
            config['BENCHMARK_TICKER'] = ticker
        else:
            errors.append('Benchmark ticker must not be empty.')

    if 'num_simulations' in payload:
        try:
            value = int(payload.get('num_simulations'))
            if value <= 0:
                raise ValueError
            config['NUM_SIMULATIONS'] = value
        except (TypeError, ValueError):
            errors.append('Num simulations must be a positive integer.')

    if 'confidence_level' in payload:
        try:
            value = float(payload.get('confidence_level'))
            if not (0 < value < 1):
                raise ValueError
            config['CONFIDENCE_LEVEL'] = value
        except (TypeError, ValueError):
            errors.append('Confidence level must be a decimal between 0 and 1.')

    if 'span_ewma' in payload:
        try:
            value = int(payload.get('span_ewma'))
            if value <= 0:
                raise ValueError
            config['SPAN_EWMA'] = value
        except (TypeError, ValueError):
            errors.append('EWMA span must be a positive integer.')

    if 'currency' in payload:
        currency = str(payload.get('currency', '')).upper()
        if currency in {'USD', 'BHD'}:
            config['CURRENCY'] = currency
        else:
            errors.append('Currency must be either USD or BHD.')

    if 'auto_refresh_interval' in payload:
        try:
            value = int(payload.get('auto_refresh_interval'))
            if value < 5 or value > 900:
                raise ValueError
            config['AUTO_REFRESH_INTERVAL'] = value
        except (TypeError, ValueError):
            errors.append('Auto refresh interval must be between 5 seconds and 900 seconds.')

    if errors:
        return jsonify({'status': 'error', 'errors': errors}), 400

    save_config(config)
    currency_settings = get_currency_context(config)

    return jsonify({
        'status': 'ok',
        'config': {
            'BENCHMARK_TICKER': config.get('BENCHMARK_TICKER'),
            'NUM_SIMULATIONS': config.get('NUM_SIMULATIONS'),
            'CONFIDENCE_LEVEL': config.get('CONFIDENCE_LEVEL'),
            'SPAN_EWMA': config.get('SPAN_EWMA'),
            'CURRENCY': config.get('CURRENCY'),
            'AUTO_REFRESH_INTERVAL': config.get('AUTO_REFRESH_INTERVAL'),
        },
        'currency': currency_settings,
    })

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

    # Once done, load the latest results from the SQLite database
    try:
        config = load_config()
        data_period = config.get('DATA_PERIOD', '1y')
        with connect(DATA_STORE) as conn:
            ensure_risk_results_table(conn)
            query = """
                SELECT
                    ticker AS "Ticker",
                    trailing_stop_pct AS "Trailing Stop (%)",
                    likelihood_pct AS "Likelihood of Activation (%)",
                    potential_loss AS "Potential Loss ($)",
                    ewma_var AS "EWMA VaR ($)"
                FROM risk_analysis_results
                WHERE data_period = ?
                ORDER BY ticker, trailing_stop_pct
            """
            df = pd.read_sql_query(query, conn, params=(data_period,))
        if not df.empty:
            df.columns = [str(c) for c in df.columns]
            log_output_table = df.to_dict(orient='records')
        else:
            log_output_raw.append(
                f"Note: no risk analysis results found in database for period {data_period}."
            )
    except Exception as e:
        log_output_raw.append(f"Error reading results from database: {e}")

# ------------------------------
# Routes
# ------------------------------
@app.route('/risk-analysis', methods=['GET'])
def risk_analysis():
    config = load_config()
    return render_template(
        'risk_analysis.html',
        config=config,
        log_output_raw=log_output_raw,
        log_output_table=log_output_table,
        active_page='risk',
        page_title='Portfolio Risk Analysis',
        page_subtitle='Stop-loss simulations & VaR insights',
        snapshot=None,
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
