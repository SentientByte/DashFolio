from flask import (
    Flask,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from app_paths import ASSETS_DIR, BASE_DIR, DATA_STORE, MAIN_SCRIPT, VENV_PYTHON
from Calculations.allocations import normalize_target_allocations
from Calculations.snapshot_cache import get_portfolio_snapshot as get_cached_portfolio_snapshot
from Calculations.market_data import get_market_snapshot
from Calculations.storage import (
    connect,
    ensure_risk_results_table,
    ensure_user_table,
    insert_single_user,
)
from Calculations.transactions import (
    add_cash_adjustment,
    append_transactions,
    fetch_holdings_with_market_values,
    load_cash_adjustments,
    load_cash_balance,
    load_current_holdings,
    load_transactions,
    parse_transactions_csv,
    preview_holdings as build_preview_holdings,
    remove_cash_adjustment,
    replace_transactions,
)
from Calculations.utils import safe_float
from services.activity_log import append_log, get_log_entries
from services.auth import complete_onboarding, load_user_record, login_user_session
from services.configuration import (
    DEFAULT_SESSION_DURATION,
    SESSION_DURATION_CHOICES,
    apply_session_duration,
    ensure_default_config_file,
    get_currency_context,
    load_config,
    save_config,
)
from services.notifications import (
    NotificationError,
    configure_notification_scheduler,
    send_test_notification,
)
from services.formatting import (
    format_currency_value,
    format_signed_currency_value,
    format_snapshot_update,
)
from services.market_hours import get_market_status
from services.portfolio import (
    ensure_default_portfolio_file,
    load_portfolio_file,
    load_portfolio_state,
    save_portfolio_file,
    save_portfolio_state,
)
from werkzeug.security import check_password_hash, generate_password_hash


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dashfolio-secret-key")

log_output_table: List[Dict[str, Any]] = [] # parsed table (list of dicts) built from database results


@app.context_processor
def inject_global_helpers():
    config = load_config()
    currency_context = get_currency_context(config)
    market_status = get_market_status()
    return {
        "datetime": datetime,
        "currency_context": currency_context,
        "format_currency": lambda value, ctx=currency_context: format_currency_value(value, ctx),
        "format_signed_currency": lambda value, ctx=currency_context: format_signed_currency_value(value, ctx),
        "format_snapshot_update": format_snapshot_update,
        "current_user": getattr(g, 'user', None),
        "is_authenticated": getattr(g, 'is_authenticated', False),
        "market_status": market_status,
    }

# ------------------------------
# Application bootstrap
# ------------------------------
ensure_default_config_file()
ensure_default_portfolio_file()
_initial_config = load_config()
apply_session_duration(app, _initial_config)
configure_notification_scheduler(DATA_STORE, _initial_config)


# ------------------------------
# Static asset serving
# ------------------------------


@app.route('/assets/<path:filename>')
def theme_assets(filename: str):
    """Serve bundled Sneat theme assets locally."""

    return send_from_directory(ASSETS_DIR, filename)


# ------------------------------
# Authentication guards
# ------------------------------


@app.before_request
def enforce_single_user_access() -> Optional[Any]:
    endpoint = request.endpoint or ""
    public_endpoints = {"theme_assets"}
    if endpoint.startswith("static") or endpoint in public_endpoints:
        return None

    user = load_user_record(DATA_STORE)
    g.user = user
    session_user_id = session.get('user_id')
    g.is_authenticated = bool(user and session_user_id == user.get('id'))

    if user is None:
        if endpoint != 'register':
            return redirect(url_for('register'))
        return None

    if not g.is_authenticated:
        if endpoint == 'register':
            return redirect(url_for('login'))
        if endpoint != 'login':
            return redirect(url_for('login'))
        return None

    if endpoint in {'login', 'register'}:
        if user.get('onboarding_completed'):
            return redirect(url_for('portfolio_analysis'))
        return redirect(url_for('onboarding_deposits'))

    if not user.get('onboarding_completed'):
        allowed = {'onboarding_deposits', 'onboarding_upload', 'logout'}
        if endpoint not in allowed:
            return redirect(url_for('onboarding_deposits'))

    return None


# ------------------------------
# Authentication & onboarding routes
# ------------------------------


def _registration_form_data() -> Dict[str, str]:
    return {
        'first_name': request.form.get('first_name', '').strip(),
        'last_name': request.form.get('last_name', '').strip(),
        'username': request.form.get('username', '').strip(),
        'email': request.form.get('email', '').strip(),
    }


@app.route('/register', methods=['GET', 'POST'])
def register():
    if g.get('user'):
        if g.get('is_authenticated'):
            if g.user.get('onboarding_completed'):
                return redirect(url_for('portfolio_analysis'))
            return redirect(url_for('onboarding_deposits'))
        return redirect(url_for('login'))

    errors: List[str] = []
    form_data = _registration_form_data() if request.method == 'POST' else {
        'first_name': '',
        'last_name': '',
        'username': '',
        'email': '',
    }

    if request.method == 'POST':
        first_name = form_data['first_name']
        last_name = form_data['last_name']
        username = form_data['username']
        email = form_data['email']
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')

        if not first_name:
            errors.append('First name is required.')
        if not last_name:
            errors.append('Last name is required.')
        if not username:
            errors.append('Username is required.')
        if not email or '@' not in email:
            errors.append('A valid email address is required.')
        if not password or len(password) < 8:
            errors.append('Password must be at least 8 characters long.')
        if password != confirm_password:
            errors.append('Password confirmation does not match.')

        if not errors:
            password_hash = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
            try:
                with connect(DATA_STORE) as conn:
                    ensure_user_table(conn)
                    insert_single_user(
                        conn,
                        first_name=first_name,
                        last_name=last_name,
                        username=username,
                        email=email,
                        password_hash=password_hash,
                    )
            except sqlite3.IntegrityError:
                errors.append('A user account has already been created. Please log in instead.')
            else:
                user = load_user_record(DATA_STORE)
                if user:
                    login_user_session(app, DATA_STORE, user)
                    g.user = user
                    g.is_authenticated = True
                    return redirect(url_for('onboarding_deposits'))
                return redirect(url_for('login'))

    return render_template(
        'register.html',
        errors=errors,
        form_data=form_data,
        snapshot=None,
        active_page=None,
        page_title='Create account',
        page_subtitle='Step 1 of 3: Register your administrator account',
    )


@app.route('/login', methods=['GET', 'POST'])
def login():
    if g.user is None:
        return redirect(url_for('register'))
    if g.get('is_authenticated'):
        if g.user.get('onboarding_completed'):
            return redirect(url_for('portfolio_analysis'))
        return redirect(url_for('onboarding_deposits'))

    errors: List[str] = []
    username_value = ''

    if request.method == 'POST':
        username_value = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        if not username_value:
            errors.append('Username is required.')
        if not password:
            errors.append('Password is required.')

        if not errors:
            user = g.user or load_user_record(DATA_STORE)
            if user and user.get('username', '').lower() == username_value.lower() and check_password_hash(user.get('password_hash', ''), password):
                login_user_session(app, DATA_STORE, user)
                g.user = user
                g.is_authenticated = True
                if user.get('onboarding_completed'):
                    return redirect(url_for('portfolio_analysis'))
                return redirect(url_for('onboarding_deposits'))
            errors.append('Invalid username or password.')

    return render_template(
        'login.html',
        errors=errors,
        username=username_value,
        snapshot=None,
        active_page=None,
        page_title='Welcome back',
        page_subtitle='Log in to continue to DashFolio',
    )


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/onboarding/deposits', methods=['GET', 'POST'])
def onboarding_deposits():
    if not g.get('is_authenticated'):
        return redirect(url_for('login'))

    errors: List[str] = []
    success_message: Optional[str] = None
    form_timestamp = request.form.get('timestamp') if request.method == 'POST' else None
    form_amount = request.form.get('amount') if request.method == 'POST' else None

    default_timestamp = datetime.now().replace(microsecond=0).strftime('%Y-%m-%dT%H:%M')

    deposits = [adj for adj in load_cash_adjustments(DATA_STORE) if adj.get('type') == 'deposit']
    cash_balance = load_cash_balance(DATA_STORE)

    if request.method == 'POST':
        raw_amount = (form_amount or '').strip()
        raw_timestamp = (form_timestamp or '').strip()

        try:
            amount_value = float(raw_amount)
        except (TypeError, ValueError):
            errors.append('Enter a numeric amount for the deposit.')
            amount_value = 0.0
        else:
            if amount_value <= 0:
                errors.append('Deposit amount must be greater than zero.')

        if raw_timestamp:
            try:
                timestamp_value = datetime.fromisoformat(raw_timestamp)
            except ValueError:
                errors.append('Provide a valid deposit date and time.')
                timestamp_value = None
        else:
            timestamp_value = datetime.now()

        if not errors and timestamp_value is not None:
            try:
                adjustments, cash_balance = add_cash_adjustment(
                    DATA_STORE,
                    {
                        'timestamp': timestamp_value.replace(microsecond=0).isoformat(),
                        'amount': amount_value,
                        'type': 'deposit',
                    },
                )
            except ValueError as exc:
                errors.append(str(exc))
            else:
                deposits = [adj for adj in adjustments if adj.get('type') == 'deposit']
                success_message = 'Deposit recorded successfully.'
                form_timestamp = None
                form_amount = None

    deposit_total = sum(safe_float(entry.get('amount')) for entry in deposits)

    return render_template(
        'onboarding_deposits.html',
        deposits=deposits,
        deposit_total=deposit_total,
        cash_balance=cash_balance,
        errors=errors,
        success_message=success_message,
        form_timestamp=form_timestamp or default_timestamp,
        form_amount=form_amount or '',
        snapshot=None,
        active_page=None,
        page_title='Record initial deposits',
        page_subtitle='Step 2 of 3: Capture your starting cash position',
        onboarding_step=2,
        onboarding_total=3,
    )


@app.route('/onboarding/upload', methods=['GET', 'POST'])
def onboarding_upload():
    if not g.get('is_authenticated'):
        return redirect(url_for('login'))

    error: Optional[str] = None

    if request.method == 'POST':
        action = request.form.get('action', 'upload')
        if action == 'skip':
            complete_onboarding(DATA_STORE)
            return redirect(url_for('portfolio_analysis'))

        file_storage = request.files.get('csv_file')
        mode = request.form.get('mode', 'replace').lower()
        if mode not in {'append', 'replace'}:
            mode = 'replace'

        if not file_storage or not file_storage.filename:
            error = 'Choose a CSV file to upload.'
        else:
            try:
                records = parse_transactions_csv(file_storage.read())
            except ValueError as exc:
                error = str(exc)
            else:
                if not records:
                    error = 'No valid transactions found in the uploaded file.'
                else:
                    try:
                        if mode == 'append':
                            append_transactions(DATA_STORE, records)
                        else:
                            replace_transactions(DATA_STORE, records)
                    except ValueError as exc:
                        error = str(exc)
                    else:
                        state = load_portfolio_state(DATA_STORE)
                        save_portfolio_state(DATA_STORE, {'target_allocations': state.get('target_allocations', {})})
                        complete_onboarding(DATA_STORE)
                        return redirect(url_for('portfolio_analysis'))

    existing_transactions = load_transactions(DATA_STORE)

    return render_template(
        'onboarding_upload.html',
        error=error,
        transaction_count=len(existing_transactions),
        snapshot=None,
        active_page=None,
        page_title='Upload transaction history',
        page_subtitle='Step 3 of 3: Import your trades or skip for later',
        onboarding_step=3,
        onboarding_total=3,
    )


# ------------------------------
# Portfolio routes & APIs
# ------------------------------
@app.route('/')
def portfolio_analysis():
    config = load_config()
    currency_settings = get_currency_context(config)
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')

    portfolio_state = load_portfolio_state(DATA_STORE)
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    cash_balance = portfolio_state.get('cash_balance', 0.0)
    transactions = portfolio_state.get('transactions', [])
    cash_adjustments = portfolio_state.get('cash_adjustments', [])
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
        cash_balance,
        transactions=transactions,
        cash_adjustments=cash_adjustments,
        holdings_metadata=portfolio_state.get('metadata', []),
    )
    market_status = get_market_status()
    if snapshot is not None:
        snapshot = dict(snapshot)
        snapshot['market_status'] = market_status
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

    portfolio_state = load_portfolio_state(DATA_STORE)
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    cash_balance = portfolio_state.get('cash_balance', 0.0)
    transactions = portfolio_state.get('transactions', [])
    cash_adjustments = portfolio_state.get('cash_adjustments', [])
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
        cash_balance,
        transactions=transactions,
        cash_adjustments=cash_adjustments,
        holdings_metadata=portfolio_state.get('metadata', []),
    )
    market_status = get_market_status()
    if snapshot is not None:
        snapshot = dict(snapshot)
        snapshot['market_status'] = market_status
    return render_template(
        'allocation.html',
        snapshot=snapshot,
        target_allocations=snapshot.get('target_allocations', {}) if snapshot else {},
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
    portfolio_state = load_portfolio_state(DATA_STORE)
    holdings = portfolio_state.get('holdings', [])
    holdings_summary = fetch_holdings_with_market_values(holdings)
    cash_balance = portfolio_state.get('cash_balance', 0.0)

    return render_template(
        'transactions.html',
        transactions=transactions,
        holdings_summary=holdings_summary,
        cash_balance=cash_balance,
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

    portfolio_state = load_portfolio_state(DATA_STORE)
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    cash_balance = portfolio_state.get('cash_balance', 0.0)
    transactions = portfolio_state.get('transactions', [])
    cash_adjustments = portfolio_state.get('cash_adjustments', [])
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
        cash_balance,
        transactions=transactions,
        cash_adjustments=cash_adjustments,
        holdings_metadata=portfolio_state.get('metadata', []),
    )
    market_status = get_market_status()
    if snapshot is not None:
        snapshot = dict(snapshot)
        snapshot['market_status'] = market_status

    return render_template(
        'settings.html',
        snapshot=snapshot,
        target_allocations=targets,
        cash_balance=cash_balance,
        holdings_metadata=portfolio_state.get('metadata', []),
        cash_adjustments=cash_adjustments,
        config=config,
        currency_settings=currency_settings,
        benchmark_ticker=benchmark_ticker,
        activity_log=get_log_entries(),
        session_durations=SESSION_DURATION_CHOICES,
        active_page='settings',
        page_title='Settings',
        page_subtitle='Manage portfolio configuration & preferences',
    )


@app.route('/api/portfolio', methods=['GET'])
def api_get_portfolio():
    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    portfolio_state = load_portfolio_state(DATA_STORE)
    holdings = portfolio_state.get('holdings', [])
    targets = portfolio_state.get('target_allocations', {})
    cash_balance = portfolio_state.get('cash_balance', 0.0)
    transactions = portfolio_state.get('transactions', [])
    cash_adjustments = portfolio_state.get('cash_adjustments', [])
    force_refresh = str(request.args.get('force', '')).lower() in {'1', 'true', 'yes'}
    append_log(
        "Portfolio snapshot API requested "
        f"(force={'yes' if force_refresh else 'no'})"
    )
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        targets,
        benchmark_ticker,
        cash_balance,
        transactions=transactions,
        cash_adjustments=cash_adjustments,
        refresh_async=not force_refresh,
        force_recompute=force_refresh,
        holdings_metadata=portfolio_state.get('metadata', []),
    )
    market_status = get_market_status()
    payload = dict(snapshot) if snapshot else {}
    payload['market_status'] = market_status
    return jsonify(payload)


@app.route('/api/market/status', methods=['GET'])
def api_market_status():
    return jsonify(get_market_status())


@app.route('/api/market/<ticker>', methods=['GET'])
def api_market_lookup(ticker: str):
    symbol = str(ticker or '').strip().upper()
    if not symbol:
        return jsonify({'error': 'Ticker is required.'}), 400

    snapshot = get_market_snapshot(symbol)
    price = safe_float(snapshot.get('current_price'))
    if price is None or price <= 0:
        return jsonify({'error': 'Unable to fetch price for the selected ticker.'}), 404

    response = {
        'ticker': symbol,
        'price': price,
    }
    name = snapshot.get('short_name') or snapshot.get('long_name') or snapshot.get('symbol')
    if name:
        response['name'] = name
    logo = snapshot.get('logo_url')
    if logo:
        response['logo_url'] = logo
    return jsonify(response)


@app.route('/api/transactions', methods=['GET'])
def api_get_transactions():
    transactions = load_transactions(DATA_STORE)
    portfolio_state = load_portfolio_state(DATA_STORE)
    holdings = portfolio_state.get('holdings', [])
    holdings_summary = fetch_holdings_with_market_values(holdings)
    cash_balance = portfolio_state.get('cash_balance', 0.0)
    return jsonify({
        'transactions': transactions,
        'holdings': holdings_summary,
        'cash_balance': cash_balance,
    })


@app.route('/api/transactions/save', methods=['POST'])
def api_save_transactions():
    payload = request.get_json(silent=True) or {}
    records = payload.get('transactions')
    if not isinstance(records, list):
        return jsonify({'error': 'Transactions payload must be a list.'}), 400

    try:
        replace_transactions(DATA_STORE, records)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    # Persist updated allocation targets with the new holdings universe.
    state = load_portfolio_state(DATA_STORE)
    save_portfolio_state(DATA_STORE, {'target_allocations': state.get('target_allocations', {})})

    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    cash_balance = state.get('cash_balance', 0.0)
    transactions_state = state.get('transactions', [])
    cash_adjustments = state.get('cash_adjustments', [])
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        state.get('holdings', []),
        state.get('target_allocations', {}),
        benchmark_ticker,
        cash_balance,
        transactions=transactions_state,
        cash_adjustments=cash_adjustments,
        refresh_async=True,
        force_recompute=True,
        holdings_metadata=state.get('metadata', []),
    )
    market_status = get_market_status()
    snapshot_payload = dict(snapshot) if snapshot else {}
    snapshot_payload['market_status'] = market_status

    holdings_summary = fetch_holdings_with_market_values(state.get('holdings', []))
    return jsonify({
        'status': 'ok',
        'transactions': load_transactions(DATA_STORE),
        'holdings': holdings_summary,
        'cash_balance': cash_balance,
        'snapshot': snapshot_payload,
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

    _, preview_holdings, preview_cash_balance = build_preview_holdings(DATA_STORE, parsed_records, mode)
    holdings_with_values = fetch_holdings_with_market_values(preview_holdings)

    return jsonify({
        'status': 'preview',
        'mode': mode,
        'uploaded_transactions': parsed_records,
        'preview_holdings': holdings_with_values,
        'preview_cash_balance': preview_cash_balance,
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
            replace_transactions(DATA_STORE, transactions_payload)
        else:
            append_transactions(DATA_STORE, transactions_payload)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    state = load_portfolio_state(DATA_STORE)
    save_portfolio_state(DATA_STORE, {'target_allocations': state.get('target_allocations', {})})

    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    cash_balance = state.get('cash_balance', 0.0)
    transactions_state = state.get('transactions', [])
    cash_adjustments = state.get('cash_adjustments', [])
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        state.get('holdings', []),
        state.get('target_allocations', {}),
        benchmark_ticker,
        cash_balance,
        transactions=transactions_state,
        cash_adjustments=cash_adjustments,
        refresh_async=True,
        force_recompute=True,
        holdings_metadata=state.get('metadata', []),
    )
    market_status = get_market_status()
    snapshot_payload = dict(snapshot) if snapshot else {}
    snapshot_payload['market_status'] = market_status

    holdings_summary = fetch_holdings_with_market_values(state.get('holdings', []))

    return jsonify({
        'status': 'ok',
        'mode': mode,
        'transactions': load_transactions(DATA_STORE),
        'holdings': holdings_summary,
        'cash_balance': cash_balance,
        'snapshot': snapshot_payload,
    })


@app.route('/api/targets', methods=['POST'])
def api_update_targets():
    payload = request.get_json(silent=True) or {}
    target_entries = payload.get('targets', [])

    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    state = load_portfolio_state(DATA_STORE)
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
    save_portfolio_state(DATA_STORE, {
        'target_allocations': normalized,
        'holdings': state.get('metadata', []),
    })

    cash_balance = state.get('cash_balance', 0.0)
    transactions_state = state.get('transactions', [])
    cash_adjustments = state.get('cash_adjustments', [])
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        holdings,
        normalized,
        benchmark_ticker,
        cash_balance,
        transactions=transactions_state,
        cash_adjustments=cash_adjustments,
        refresh_async=True,
        force_recompute=True,
    )
    market_status = get_market_status()
    snapshot_payload = dict(snapshot) if snapshot else {}
    snapshot_payload['market_status'] = market_status
    return jsonify({'status': 'ok', 'targets': normalized, 'snapshot': snapshot_payload, 'cash_balance': cash_balance})


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
            if value < 1 or value > 60:
                raise ValueError
            config['AUTO_REFRESH_INTERVAL'] = value
        except (TypeError, ValueError):
            errors.append('Auto refresh interval must be between 1 second and 60 seconds.')

    if 'session_duration_hours' in payload:
        try:
            value = int(payload.get('session_duration_hours'))
            if value not in SESSION_DURATION_CHOICES:
                raise ValueError
            config['SESSION_DURATION_HOURS'] = value
        except (TypeError, ValueError):
            errors.append('Session duration must be one of 0, 4, 12, 24, or 48 hours.')

    if 'notifications_enabled' in payload:
        config['NOTIFICATIONS_ENABLED'] = bool(payload.get('notifications_enabled'))

    if 'notify_end_of_day' in payload:
        config['NOTIFY_END_OF_DAY'] = bool(payload.get('notify_end_of_day'))

    if 'notify_beginning_of_day' in payload:
        config['NOTIFY_BEGINNING_OF_DAY'] = bool(payload.get('notify_beginning_of_day'))

    if 'telegram_bot_token' in payload:
        config['TELEGRAM_BOT_TOKEN'] = str(payload.get('telegram_bot_token', '') or '').strip()

    if 'telegram_chat_id' in payload:
        config['TELEGRAM_CHAT_ID'] = str(payload.get('telegram_chat_id', '') or '').strip()

    if config.get('NOTIFICATIONS_ENABLED'):
        if not config.get('TELEGRAM_BOT_TOKEN') or not config.get('TELEGRAM_CHAT_ID'):
            errors.append('Telegram bot token and chat ID are required when notifications are enabled.')

    if errors:
        return jsonify({'status': 'error', 'errors': errors}), 400

    save_config(config)
    apply_session_duration(app, config)
    configure_notification_scheduler(DATA_STORE, config)
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
            'SESSION_DURATION_HOURS': config.get('SESSION_DURATION_HOURS'),
            'NOTIFICATIONS_ENABLED': config.get('NOTIFICATIONS_ENABLED'),
            'TELEGRAM_BOT_TOKEN': config.get('TELEGRAM_BOT_TOKEN'),
            'TELEGRAM_CHAT_ID': config.get('TELEGRAM_CHAT_ID'),
            'NOTIFY_END_OF_DAY': config.get('NOTIFY_END_OF_DAY'),
            'NOTIFY_BEGINNING_OF_DAY': config.get('NOTIFY_BEGINNING_OF_DAY'),
        },
        'currency': currency_settings,
    })


@app.route('/api/notifications/test', methods=['POST'])
def api_send_test_notification():
    payload = request.get_json(silent=True) or {}
    bot_token = str(payload.get('bot_token', '') or '').strip()
    chat_id = str(payload.get('chat_id', '') or '').strip()
    mode = str(payload.get('mode', 'end_of_day') or 'end_of_day')

    if not bot_token or not chat_id:
        return jsonify({'status': 'error', 'error': 'Bot token and chat ID are required.'}), 400
    if mode not in {'end_of_day', 'beginning_of_day'}:
        return jsonify({'status': 'error', 'error': 'Invalid notification mode requested.'}), 400

    config = load_config()
    config_override = dict(config)
    config_override['TELEGRAM_BOT_TOKEN'] = bot_token
    config_override['TELEGRAM_CHAT_ID'] = chat_id

    try:
        message = send_test_notification(config_override, bot_token=bot_token, chat_id=chat_id, mode=mode)
    except NotificationError as exc:
        return jsonify({'status': 'error', 'error': str(exc)}), 400
    except Exception as exc:  # pragma: no cover - defensive logging
        append_log(f'Test notification failed: {exc}')
        return jsonify({'status': 'error', 'error': 'Unable to send test notification right now.'}), 500

    return jsonify({'status': 'ok', 'mode': mode, 'message': message})


@app.route('/api/settings/logos', methods=['POST'])
def api_update_logos():
    payload = request.get_json(silent=True) or {}
    entries = payload.get('logos', [])
    if not isinstance(entries, list):
        return jsonify({'error': 'Logos payload must be a list.'}), 400

    portfolio_payload = load_portfolio_file()
    metadata_lookup: Dict[str, Dict[str, Any]] = {}
    for entry in portfolio_payload.get('holdings', []):
        ticker = str(entry.get('ticker', '')).upper().strip()
        if not ticker:
            continue
        metadata_lookup[ticker] = {
            'ticker': ticker,
            'logo_url': entry.get('logo_url'),
            'name': entry.get('name'),
        }

    for entry in entries:
        ticker = str(entry.get('ticker', '')).upper().strip()
        if not ticker:
            continue
        record = metadata_lookup.get(ticker, {'ticker': ticker})
        if 'logo_url' in entry:
            logo_url = str(entry.get('logo_url', '') or '').strip()
            if logo_url:
                record['logo_url'] = logo_url
            else:
                record.pop('logo_url', None)
        if 'name' in entry:
            name_value = str(entry.get('name', '') or '').strip()
            if name_value:
                record['name'] = name_value
            else:
                record.pop('name', None)
        metadata_lookup[ticker] = record

    updated_metadata = sorted(metadata_lookup.values(), key=lambda item: item['ticker'])
    portfolio_payload['holdings'] = updated_metadata
    save_portfolio_file(portfolio_payload)

    state = load_portfolio_state(DATA_STORE)
    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    cash_balance = state.get('cash_balance', 0.0)
    transactions_state = state.get('transactions', [])
    cash_adjustments = state.get('cash_adjustments', [])
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        state.get('holdings', []),
        state.get('target_allocations', {}),
        benchmark_ticker,
        cash_balance,
        transactions=transactions_state,
        cash_adjustments=cash_adjustments,
        refresh_async=True,
        force_recompute=True,
        holdings_metadata=state.get('metadata', []),
    )

    return jsonify({
        'status': 'ok',
        'holdings_metadata': state.get('metadata', []),
        'holdings': state.get('holdings', []),
        'cash_balance': cash_balance,
        'snapshot': snapshot,
    })


@app.route('/api/cash-adjustments', methods=['GET'])
def api_get_cash_adjustments():
    adjustments = load_cash_adjustments(DATA_STORE)
    state = load_portfolio_state(DATA_STORE)
    return jsonify({
        'adjustments': adjustments,
        'cash_balance': state.get('cash_balance', 0.0),
    })


@app.route('/api/cash-adjustments', methods=['POST'])
def api_add_cash_adjustment_route():
    payload = request.get_json(silent=True) or {}
    try:
        add_cash_adjustment(DATA_STORE, payload)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    state = load_portfolio_state(DATA_STORE)
    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    cash_balance = state.get('cash_balance', 0.0)
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        state.get('holdings', []),
        state.get('target_allocations', {}),
        benchmark_ticker,
        cash_balance,
        transactions=state.get('transactions', []),
        cash_adjustments=state.get('cash_adjustments', []),
        refresh_async=True,
        force_recompute=True,
    )

    return jsonify({
        'status': 'ok',
        'adjustments': state.get('cash_adjustments', []),
        'cash_balance': cash_balance,
        'snapshot': snapshot,
    })


@app.route('/api/cash-adjustments/<int:adjustment_id>', methods=['DELETE'])
def api_delete_cash_adjustment_route(adjustment_id: int):
    try:
        remove_cash_adjustment(DATA_STORE, adjustment_id)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    state = load_portfolio_state(DATA_STORE)
    config = load_config()
    benchmark_ticker = config.get('BENCHMARK_TICKER', 'SPY')
    cash_balance = state.get('cash_balance', 0.0)
    snapshot = get_cached_portfolio_snapshot(
        DATA_STORE,
        state.get('holdings', []),
        state.get('target_allocations', {}),
        benchmark_ticker,
        cash_balance,
        transactions=state.get('transactions', []),
        cash_adjustments=state.get('cash_adjustments', []),
        refresh_async=True,
        force_recompute=True,
    )

    return jsonify({
        'status': 'ok',
        'adjustments': state.get('cash_adjustments', []),
        'cash_balance': cash_balance,
        'snapshot': snapshot,
    })


def run_main_script():
    """
    Run main.py synchronously using the venv/python specified (VENV_PYTHON).
    Capture stdout/stderr into the in-memory activity log and after completion attempt to read the
    results into ``log_output_table`` (list of dicts).
    """
    global log_output_table
    log_output_table = []

    if not os.path.exists(MAIN_SCRIPT):
        append_log(f"ERROR: main script not found at {MAIN_SCRIPT}")
        return

    # Execute main.py
    append_log("Launching risk analysis script (main.py)")
    try:
        process = subprocess.Popen(
            [VENV_PYTHON, MAIN_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=BASE_DIR
        )
    except Exception as e:
        append_log(f"Failed to start risk analysis process: {e}")
        return

    # Stream stdout lines
    if process.stdout:
        for line in process.stdout:
            message = line.rstrip('\n')
            if message:
                append_log(f"[main.py] {message}")

    exit_code = process.wait()
    append_log(f"Risk analysis script completed with exit code {exit_code}")

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
            append_log(
                f"Loaded {len(log_output_table)} risk analysis rows for period {data_period}"
            )
        else:
            append_log(
                f"Note: no risk analysis results found in database for period {data_period}."
            )
    except Exception as e:
        append_log(f"Error reading results from database: {e}")

# ------------------------------
# Routes
# ------------------------------
@app.route('/risk-analysis', methods=['GET'])
def risk_analysis():
    config = load_config()
    return render_template(
        'risk_analysis.html',
        config=config,
        log_output_table=log_output_table,
        activity_log=get_log_entries(),
        active_page='risk',
        page_title='Portfolio Risk Analysis',
        page_subtitle='Stop-loss simulations & VaR insights',
        snapshot=None,
    )

@app.route('/run', methods=['POST'])
def run():
    append_log("Starting risk analysis calculations...")
    # Run synchronously (Option 1). Will block until main.py completes.
    run_main_script()
    return redirect(url_for('risk_analysis'))

# ------------------------------
# Run app
# ------------------------------
if __name__ == '__main__':
    app.run(debug=True)
