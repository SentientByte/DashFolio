"""Market data retrieval helpers."""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd
import yfinance as yf

from .utils import historical_close, normalize_index


BENCHMARK_TICKER = "SPY"


def get_market_snapshot(ticker: str) -> Dict[str, Any]:
    """Fetch current and recent pricing information for ``ticker``."""

    result: Dict[str, Any] = {
        "current_price": None,
        "previous_close": None,
        "week_close": None,
        "month_close": None,
        "price_history": None,
    }

    try:
        ticker_obj = yf.Ticker(ticker)
        fast_info = getattr(ticker_obj, "fast_info", None)
        if fast_info:
            result["current_price"] = fast_info.get("last_price") or fast_info.get("lastPrice")
            result["previous_close"] = fast_info.get("previous_close") or fast_info.get("previousClose")

        history = ticker_obj.history(period="1y", interval="1d")
        if not history.empty:
            closes = history.get("Close")
            if closes is not None:
                closes = closes.dropna()
                if not closes.empty:
                    closes = normalize_index(closes)
                    result["price_history"] = closes
                    if result["current_price"] is None:
                        result["current_price"] = float(closes.iloc[-1])
                    if result["previous_close"] is None:
                        if len(closes) > 1:
                            result["previous_close"] = float(closes.iloc[-2])
                        else:
                            result["previous_close"] = float(closes.iloc[-1])
                    result["week_close"] = historical_close(closes, 7)
                    result["month_close"] = historical_close(closes, 30)
    except Exception as exc:
        print(f"Warning: failed to fetch market data for {ticker}: {exc}")

    return result


def get_benchmark_history(period: str = "1y", benchmark: str | None = BENCHMARK_TICKER) -> pd.Series:
    """Return closing prices for the benchmark ticker."""

    if not benchmark:
        benchmark = BENCHMARK_TICKER

    try:
        ticker = yf.Ticker(benchmark)
        history = ticker.history(period=period, interval="1d")
        if history.empty:
            return pd.Series(dtype=float)
        closes = history.get("Close")
        if closes is None:
            return pd.Series(dtype=float)
        closes = closes.dropna()
        if closes.empty:
            return pd.Series(dtype=float)
        return normalize_index(closes)
    except Exception as exc:
        print(f"Warning: failed to fetch benchmark history for {benchmark}: {exc}")
        return pd.Series(dtype=float)


def get_benchmark_returns(period: str = "1y", benchmark: str | None = BENCHMARK_TICKER) -> pd.Series:
    """Return daily percentage returns for the benchmark ticker."""

    history = get_benchmark_history(period=period, benchmark=benchmark)
    if history.empty:
        return pd.Series(dtype=float)
    returns = history.pct_change().dropna()
    return returns if not returns.empty else pd.Series(dtype=float)
