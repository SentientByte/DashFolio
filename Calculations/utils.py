"""Utility helpers shared across calculation modules."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd


def safe_float(value: Any, default: float = 0.0) -> float:
    """Best-effort conversion of ``value`` to a ``float``.

    ``math.nan`` and ``None`` are converted to ``default`` so downstream
    calculations do not need to handle those edge cases repeatedly.
    """

    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_index(series: pd.Series) -> pd.Series:
    """Return a timezone-naive series to ensure consistent comparisons."""

    idx = series.index
    if not isinstance(idx, pd.DatetimeIndex):
        return series
    if idx.tz is not None:
        series = series.copy()
        series.index = idx.tz_localize(None)
    return series


def historical_close(series: pd.Series, days_back: int) -> float | None:
    """Return the closing price from approximately ``days_back`` days ago."""

    if series.empty:
        return None

    series = normalize_index(series)
    last_idx = series.index[-1] if isinstance(series.index, pd.DatetimeIndex) else None
    if last_idx is None:
        pos = max(len(series) - (days_back + 1), 0)
        return float(series.iloc[pos])

    target_date = last_idx - pd.Timedelta(days=days_back)
    historical = series.loc[series.index <= target_date]
    if not historical.empty:
        return float(historical.iloc[-1])
    return float(series.iloc[0])
