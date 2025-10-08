"""Market data retrieval helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd
import yfinance as yf

from .utils import historical_close, normalize_index, safe_float


def _previous_close_from_history(closes: pd.Series) -> float | None:
    """Return the latest close strictly prior to today from ``closes``."""

    if closes is None or closes.empty:
        return None

    series = normalize_index(closes.dropna())
    if series.empty:
        return None

    if isinstance(series.index, pd.DatetimeIndex):
        today = datetime.now(timezone.utc).date()
        eligible = series.loc[series.index.date < today]
        if eligible.empty:
            return None
        return float(eligible.iloc[-1])

    if len(series) < 2:
        return None
    return float(series.iloc[-2])


BENCHMARK_TICKER = "SPY"


def get_market_snapshot(ticker: str) -> Dict[str, Any]:
    """Fetch current and recent pricing information for ``ticker``."""

    result: Dict[str, Any] = {
        "current_price": None,
        "previous_close": None,
        "week_close": None,
        "month_close": None,
        "price_history": None,
        "open_price": None,
        "close_price": None,
        "adj_close_price": None,
        "day_high": None,
        "day_low": None,
        "market_cap": None,
        "ema_50": None,
        "ema_200": None,
        "rolling_high_250": None,
        "rolling_low_250": None,
    }

    try:
        ticker_obj = yf.Ticker(ticker)
        fast_info = getattr(ticker_obj, "fast_info", None)
        if fast_info:
            last_price = fast_info.get("last_price") or fast_info.get("lastPrice")
            previous_close = fast_info.get("previous_close") or fast_info.get("previousClose")
            open_price = fast_info.get("open") or fast_info.get("openPrice")
            day_high = fast_info.get("day_high") or fast_info.get("dayHigh")
            day_low = fast_info.get("day_low") or fast_info.get("dayLow")
            market_cap = fast_info.get("market_cap") or fast_info.get("marketCap")

            if last_price is not None:
                result["current_price"] = safe_float(last_price)
            if previous_close is not None:
                result["previous_close"] = safe_float(previous_close)
            if open_price is not None:
                result["open_price"] = safe_float(open_price)
            if day_high is not None:
                result["day_high"] = safe_float(day_high)
            if day_low is not None:
                result["day_low"] = safe_float(day_low)
            if market_cap is not None:
                result["market_cap"] = safe_float(market_cap)

        history = ticker_obj.history(period="1y", interval="1d")
        if not history.empty:
            closes = history.get("Close")
            if closes is not None:
                closes = closes.dropna()
                if not closes.empty:
                    closes = normalize_index(closes)
                    result["price_history"] = closes
                    last_close = float(closes.iloc[-1])
                    if result["current_price"] is None or result["current_price"] <= 0:
                        result["current_price"] = last_close
                    previous_close = _previous_close_from_history(closes)
                    if previous_close is not None:
                        result["previous_close"] = previous_close
                    result["week_close"] = historical_close(closes, 7)
                    result["month_close"] = historical_close(closes, 30)

            opens = history.get("Open")
            highs = history.get("High")
            lows = history.get("Low")
            adj_close = history.get("Adj Close")

            if opens is not None and not opens.dropna().empty:
                result["open_price"] = safe_float(opens.dropna().iloc[-1])
            if highs is not None and not highs.dropna().empty:
                latest_high = float(highs.dropna().iloc[-1])
                result["day_high"] = safe_float(latest_high)
            if lows is not None and not lows.dropna().empty:
                latest_low = float(lows.dropna().iloc[-1])
                result["day_low"] = safe_float(latest_low)

            price_for_ema = None
            if adj_close is not None and not adj_close.dropna().empty:
                adj_close = adj_close.dropna()
                result["adj_close_price"] = safe_float(adj_close.iloc[-1])
                price_for_ema = adj_close

            if (price_for_ema is None or price_for_ema.empty) and closes is not None:
                raw_closes = closes.dropna()
                if not raw_closes.empty:
                    price_for_ema = raw_closes

            if price_for_ema is not None and not price_for_ema.empty:
                ema_50 = price_for_ema.ewm(span=50, adjust=False).mean().iloc[-1]
                result["ema_50"] = safe_float(ema_50)

                ema_200 = price_for_ema.ewm(span=200, adjust=False).mean().iloc[-1]
                result["ema_200"] = safe_float(ema_200)

            if closes is not None and not closes.empty:
                trimmed = closes.tail(250)
                if not trimmed.empty:
                    result["close_price"] = safe_float(trimmed.iloc[-1])
                    result["rolling_high_250"] = safe_float(trimmed.max())
                    result["rolling_low_250"] = safe_float(trimmed.min())
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
