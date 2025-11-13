"""Entry point for running DashFolio calculations."""

from datetime import datetime
from importlib.util import find_spec
from zoneinfo import ZoneInfo

from app_paths import CONFIG_FILE, DATA_STORE, PORTFOLIO_FILE
from services.configuration import ensure_default_config_file
from services.portfolio import ensure_default_portfolio_file


REQUIRED_RUNTIME_PACKAGES = (
    "pandas",
    "numpy",
    "yfinance",
)


def ensure_runtime_dependencies() -> None:
    """Exit with actionable guidance when optional dependencies are missing."""

    missing = sorted(
        package for package in REQUIRED_RUNTIME_PACKAGES if find_spec(package) is None
    )
    if not missing:
        return

    missing_list = ", ".join(missing)
    message = (
        "DashFolio requires the following Python packages, but they are not installed: "
        f"{missing_list}\n\n"
        "Create and activate the project's virtual environment, then install the "
        "dependencies:\n"
        "    python -m venv .venv\n"
        "    # Windows PowerShell\n"
        "    .\\.venv\\Scripts\\Activate.ps1\n"
        "    # Windows Command Prompt\n"
        "    .\\.venv\\Scripts\\activate.bat\n"
        "    # macOS/Linux\n"
        "    source .venv/bin/activate\n"
        "    pip install -r requirements.txt\n\n"
        "If you prefer to use the current interpreter, install the same packages "
        "directly:\n"
        "    pip install -r requirements.txt\n"
    )
    raise SystemExit(message)


ensure_runtime_dependencies()

from Calculations import (  # noqa: E402  (import after dependency validation)
    calculate_statistics,
    determine_start_date,
    load_config,
    load_portfolio,
    load_price_data,
    normalize_config,
    run_trailing_stop_analysis,
    update_portfolio_prices,
)


def main() -> None:
    ensure_default_config_file()
    ensure_default_portfolio_file()

    today = datetime.now(ZoneInfo("America/New_York"))

    raw_config = load_config(CONFIG_FILE)
    config = normalize_config(raw_config)

    start_date, period_reason = determine_start_date(
        config["DATA_PERIOD"],
        config["CUSTOM_START_DATE"],
        today,
    )
    start_date_str = start_date.strftime("%Y-%m-%d")

    print(
        f"DATA_PERIOD requested: '{config['DATA_PERIOD']}' -> "
        f"using start date {start_date_str} ({period_reason})"
    )

    df_portfolio = load_portfolio(PORTFOLIO_FILE, DATA_STORE)
    df_portfolio = update_portfolio_prices(df_portfolio, PORTFOLIO_FILE)
    tickers = df_portfolio["Ticker"].unique()

    all_data = load_price_data(tickers, start_date, today, DATA_STORE)

    calculate_statistics(
        all_data,
        tickers,
        config["SPAN_EWMA"],
        config["DATA_PERIOD"],
    )

    run_trailing_stop_analysis(
        df_portfolio,
        all_data,
        config["STOP_LOSS_PERCENTAGE_RANGE"],
        config["STOP_LOSS_STEP"],
        config["NUM_SIMULATIONS"],
        config["SPAN_EWMA"],
        config["CONFIDENCE_LEVEL"],
        config["DATA_PERIOD"],
        DATA_STORE,
    )


if __name__ == "__main__":
    main()
