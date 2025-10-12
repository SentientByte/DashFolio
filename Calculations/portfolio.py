"""Portfolio loading and price update helpers."""

from __future__ import annotations

import json
import os
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from .transactions import load_current_holdings


def _read_portfolio_file(portfolio_file: str) -> Dict:
    if not os.path.exists(portfolio_file):
        raise FileNotFoundError(f"Missing portfolio file at {portfolio_file}")
    with open(portfolio_file, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_portfolio_file(portfolio_file: str, payload: Dict) -> None:
    directory = os.path.dirname(portfolio_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(portfolio_file, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=4)


def load_portfolio(portfolio_file: str, database_path: str | None = None) -> pd.DataFrame:
    """Load holdings from transactions when available, otherwise fall back to JSON."""
    holdings = []
    if database_path:
        try:
            holdings = load_current_holdings(database_path)
        except Exception:
            holdings = []

    if not holdings:
        data = _read_portfolio_file(portfolio_file)
        holdings = data.get("holdings", [])

    df = pd.DataFrame(holdings)
    if df.empty:
        return df

    # Normalise column names expected by downstream calculations
    column_mapping = {
        "ticker": "Ticker",
        "quantity": "Quantity",
        "average_cost": "Average Cost",
        "current_price": "Current Price",
        "position": "Position",
    }
    df = df.rename(columns=column_mapping)

    if "Ticker" not in df.columns:
        raise ValueError("Portfolio JSON must include a 'ticker' for each holding.")

    if "Quantity" not in df.columns:
        df["Quantity"] = df.get("Position", 0)

    if "Position" not in df.columns:
        df["Position"] = df["Quantity"]

    if "Average Cost" not in df.columns:
        df["Average Cost"] = np.nan

    if "Quantity" in df.columns:
        df = df[df["Quantity"].abs() > 1e-9]

    return df


def _ensure_price_column(df_portfolio: pd.DataFrame) -> None:
    if "Current Price" not in df_portfolio.columns:
        df_portfolio["Current Price"] = np.nan


def _fetch_current_price(ticker: str) -> Tuple[bool, float]:
    try:
        ticker_info = yf.Ticker(ticker)
        current_price = None
        try:
            fast_info = getattr(ticker_info, "fast_info", {})
            current_price = fast_info.get("last_price")
        except Exception:
            current_price = None
        if current_price is None:
            try:
                current_price = ticker_info.info.get("regularMarketPrice")
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


def update_portfolio_prices(df_portfolio: pd.DataFrame, portfolio_file: str) -> pd.DataFrame:
    """Update portfolio with current prices and persist to disk."""
    _ensure_price_column(df_portfolio)

    updated_holdings = []
    for idx, row in df_portfolio.iterrows():
        ticker = row["Ticker"]
        success, current_price = _fetch_current_price(ticker)
        if success:
            df_portfolio.at[idx, "Current Price"] = current_price
            print(f"Updated {ticker} current price: {current_price}")
        else:
            print(f"Could not fetch current price for {ticker}")

        updated_holdings.append(
            {
                "ticker": ticker,
                "quantity": float(row.get("Quantity", row.get("Position", 0)) or 0),
                "position": float(row.get("Position", row.get("Quantity", 0)) or 0),
                "average_cost": float(row.get("Average Cost", 0) or 0),
                "current_price": float(df_portfolio.at[idx, "Current Price"])
                if not pd.isna(df_portfolio.at[idx, "Current Price"])
                else None,
            }
        )

    existing_payload = _read_portfolio_file(portfolio_file)
    # Preserve optional metadata (e.g. name/logo) if present in the JSON file
    existing_lookup = {
        holding.get("ticker", "").upper(): holding for holding in existing_payload.get("holdings", [])
    }
    merged_holdings = []
    for holding in updated_holdings:
        ticker = holding["ticker"].upper()
        preserved = existing_lookup.get(ticker, {})
        merged = {**preserved, **holding}
        merged_holdings.append(merged)

    _write_portfolio_file(portfolio_file, {"holdings": merged_holdings})
    print(f"\nPortfolio updated with current prices in {portfolio_file}")
    return df_portfolio
