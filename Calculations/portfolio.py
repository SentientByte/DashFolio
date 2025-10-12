"""Portfolio loading and price update helpers."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from .transactions import load_current_holdings

_DEFAULT_PORTFOLIO: Dict[str, Any] = {"holdings": [], "target_allocations": {}}


def _empty_portfolio_frame() -> pd.DataFrame:
    """Return an empty portfolio DataFrame with the expected schema."""

    return pd.DataFrame(
        columns=["Ticker", "Quantity", "Average Cost", "Current Price", "Position"]
    )


def _default_portfolio_snapshot() -> Dict[str, Any]:
    """Return a copy of the built-in portfolio defaults."""

    return deepcopy(_DEFAULT_PORTFOLIO)


def _read_portfolio_file(portfolio_file: str) -> Dict[str, Any]:
    try:
        with open(portfolio_file, "r", encoding="utf-8") as fh:
            data: Dict[str, Any] = json.load(fh)
    except (FileNotFoundError, PermissionError, json.JSONDecodeError) as exc:
        print(
            "Warning: falling back to in-memory defaults because portfolio file "
            f"'{portfolio_file}' could not be read. {exc}"
        )
        return _default_portfolio_snapshot()

    if not isinstance(data, dict):
        return _default_portfolio_snapshot()

    data.setdefault("holdings", [])
    data.setdefault("target_allocations", {})
    return data


def _write_portfolio_file(portfolio_file: str, payload: Dict[str, Any]) -> bool:
    try:
        with open(portfolio_file, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=4)
    except OSError as exc:
        print(f"Warning: unable to persist portfolio to '{portfolio_file}'. {exc}")
        return False
    return True


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
        # Return an empty frame with the expected schema so downstream callers can
        # safely access columns like ``df["Ticker"]`` without triggering ``KeyError``.
        return _empty_portfolio_frame()

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
        print(
            "Warning: portfolio data is missing required ticker information; "
            "using in-memory defaults instead."
        )
        return _empty_portfolio_frame()

    df = df.dropna(subset=["Ticker"])
    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    df = df[df["Ticker"] != ""]
    if df.empty:
        return _empty_portfolio_frame()

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
    if "Ticker" not in df_portfolio.columns:
        print(
            "Warning: portfolio snapshot missing 'Ticker' column; skipping price "
            "refresh."
        )
        return _empty_portfolio_frame()

    if df_portfolio.empty:
        _ensure_price_column(df_portfolio)
        print("No holdings available to refresh prices; skipping persistence.")
        return df_portfolio

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

    if merged_holdings:
        if _write_portfolio_file(portfolio_file, {"holdings": merged_holdings}):
            print(f"\nPortfolio updated with current prices in {portfolio_file}")
        else:
            print(
                "\nUnable to record updated prices on disk; continuing with in-memory data."
            )
    else:
        print("\nNo holdings detected after refresh; nothing was written to disk.")
    return df_portfolio
