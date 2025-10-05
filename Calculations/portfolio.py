"""Portfolio loading and price update helpers."""

from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd
import yfinance as yf


def load_portfolio(excel_file: str) -> pd.DataFrame:
    """Load the portfolio Excel file."""
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Missing portfolio file at {excel_file}")
    return pd.read_excel(excel_file, engine="openpyxl")


def _ensure_price_column(df_portfolio: pd.DataFrame) -> None:
    if "Current Price" not in df_portfolio.columns:
        df_portfolio["Current Price"] = np.nan


def _fetch_current_price(ticker: str) -> Tuple[bool, float]:
    try:
        ticker_info = yf.Ticker(ticker)
        current_price = None
        try:
            current_price = ticker_info.info.get("regularMarketPrice", None)
        except Exception:
            current_price = None
        if current_price is None:
            hist = ticker_info.history(period="1d")
            if not hist.empty:
                current_price = hist["Close"].iloc[-1]
        if current_price is not None:
            return True, float(current_price)
    except Exception as exc:
        print(f"Error fetching {ticker}: {exc}")
    return False, float("nan")


def update_portfolio_prices(df_portfolio: pd.DataFrame, excel_file: str) -> pd.DataFrame:
    """Update portfolio with current prices and persist to disk."""
    _ensure_price_column(df_portfolio)

    for idx, row in df_portfolio.iterrows():
        ticker = row["Ticker"]
        success, current_price = _fetch_current_price(ticker)
        if success:
            df_portfolio.at[idx, "Current Price"] = current_price
            print(f"Updated {ticker} current price: {current_price}")
        else:
            print(f"Could not fetch current price for {ticker}")

    df_portfolio.to_excel(excel_file, index=False)
    print(f"\nPortfolio updated with current prices in {excel_file}")
    return df_portfolio
