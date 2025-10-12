"""Entry point for running DashFolio calculations."""

from datetime import datetime
from zoneinfo import ZoneInfo

from Calculations import (
    calculate_statistics,
    determine_start_date,
    load_config,
    load_portfolio,
    load_price_data,
    normalize_config,
    run_trailing_stop_analysis,
    update_portfolio_prices,
)

from app_paths import CONFIG_FILE, DATA_STORE, PORTFOLIO_FILE


def main() -> None:
    portfolio_file = PORTFOLIO_FILE
    database_path = DATA_STORE
    config_file = CONFIG_FILE

    today = datetime.now(ZoneInfo("America/New_York"))

    raw_config = load_config(config_file)
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

    df_portfolio = load_portfolio(portfolio_file, database_path)
    df_portfolio = update_portfolio_prices(df_portfolio, portfolio_file)
    tickers = df_portfolio["Ticker"].unique()

    all_data = load_price_data(tickers, start_date, today, database_path)

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
        database_path,
    )


if __name__ == "__main__":
    main()
