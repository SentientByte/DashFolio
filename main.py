"""Entry point for running DashFolio calculations."""

import os
from datetime import datetime

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


def main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    portfolio_file = os.path.join(base_dir, "portfolio.json")
    database_path = os.path.join(base_dir, "dashfolio.db")
    config_file = os.path.join(base_dir, "config.json")

    today = datetime.now()

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

    df_portfolio = load_portfolio(portfolio_file)
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
