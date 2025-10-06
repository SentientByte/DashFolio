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
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from Calculations.allocations import normalize_target_allocations
from Calculations.snapshot import build_portfolio_snapshot
from Calculations.storage import connect, ensure_risk_results_table
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

app = Flask(__name__)
log_output_raw = []   # raw stdout lines from main.py
log_output_table = [] # parsed table (list of dicts) built from database results


@app.context_processor
def inject_global_helpers():
    return {"datetime": datetime}

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
    if default_portfolio["holdings"]:
        even = 100.0 / len(default_portfolio["holdings"])
        default_portfolio["target_allocations"] = {
            holding["ticker"]: even for holding in default_portfolio["holdings"]
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
        data = json.load(f)

    holdings = data.get('holdings', [])
    data['target_allocations'] = normalize_target_allocations(
        holdings,
        data.get('target_allocations'),
    )
    return data


def save_portfolio_state(data: Dict[str, Any]) -> None:
    with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


# ------------------------------
# Portfolio routes & APIs
# ------------------------------
@app.route('/')
def portfolio_analysis():
    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    snapshot = build_portfolio_snapshot(holdings, targets)
    return render_template(
        'portfolio_analysis.html',
        snapshot=snapshot,
        holdings_raw=holdings,
        active_page='portfolio',
        page_title='Portfolio Overview',
        page_subtitle='Live performance & allocations',
    )


@app.route('/allocation')
def allocation_planner():
    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    snapshot = build_portfolio_snapshot(holdings, targets)
    return render_template(
        'allocation.html',
        snapshot=snapshot,
        holdings_raw=holdings,
        target_allocations=snapshot.get('target_allocations', {}),
        active_page='allocation',
        page_title='Allocation Planner',
        page_subtitle='Rebalance towards your target mix',
    )


@app.route('/api/portfolio', methods=['GET'])
def api_get_portfolio():
    portfolio_state = load_portfolio_state()
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    snapshot = build_portfolio_snapshot(holdings, targets)
    return jsonify(snapshot)


@app.route('/api/portfolio', methods=['POST'])
def api_update_portfolio():
    payload = request.get_json(silent=True)
    if not payload or 'holdings' not in payload:
        return jsonify({'error': 'Invalid payload'}), 400

    normalized_holdings: List[Dict[str, Any]] = []
    for entry in payload.get('holdings', []):
        ticker = str(entry.get('ticker', '')).upper().strip()
        quantity = safe_float(entry.get('quantity'))
        avg_cost = safe_float(entry.get('average_cost'))
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
            holding_record['current_price'] = safe_float(entry.get('current_price'))

        normalized_holdings.append(holding_record)

    current_state = load_portfolio_state()
    updated_targets = normalize_target_allocations(
        normalized_holdings,
        current_state.get('target_allocations'),
    )

    save_portfolio_state({
        'holdings': normalized_holdings,
        'target_allocations': updated_targets,
    })
    snapshot = build_portfolio_snapshot(normalized_holdings, updated_targets)
    return jsonify({'status': 'ok', 'snapshot': snapshot})


@app.route('/api/targets', methods=['POST'])
def api_update_targets():
    payload = request.get_json(silent=True) or {}
    target_entries = payload.get('targets', [])

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

    snapshot = build_portfolio_snapshot(holdings, normalized)
    return jsonify({'status': 'ok', 'targets': normalized, 'snapshot': snapshot})

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
