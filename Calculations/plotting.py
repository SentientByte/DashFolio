"""Plotting helpers for performance visualisations."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Literal

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from Calculations.performance import PerformanceFrame

try:  # pragma: no cover - optional dependency
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - fallback
    pd = None  # type: ignore


def _get_column(df, name: str) -> Iterable:
    if pd is not None and isinstance(df, pd.DataFrame):  # pragma: no cover - runtime dependent
        return df[name]
    if isinstance(df, PerformanceFrame):
        return df.column(name)
    raise TypeError("Unsupported dataframe type for plotting")


def plot_performance(df: pd.DataFrame, mode: Literal["twr", "equity"] = "twr"):
    """Plot the portfolio performance according to ``mode``.

    Parameters
    ----------
    df:
        DataFrame containing the performance indices.  Expected columns are
        ``date`` in addition to ``index_twr`` or ``index_equity``.
    mode:
        ``"twr"`` for the time-weighted return index, ``"equity"`` for the
        equity (include-flows) index.

    Returns
    -------
    matplotlib.figure.Figure
        The generated figure instance for further tweaking or saving.
    """

    mode = str(mode).lower()
    if mode not in {"twr", "equity"}:
        raise ValueError("Mode must be either 'twr' or 'equity'.")

    index_col = "index_twr" if mode == "twr" else "index_equity"
    dates_raw = list(_get_column(df, "date"))
    values_raw = list(_get_column(df, index_col))

    if len(dates_raw) != len(values_raw):
        raise ValueError("Date and index columns must have the same length.")

    def _to_datetime(value):
        if isinstance(value, datetime):
            return value
        if pd is not None and isinstance(value, pd.Timestamp):  # pragma: no cover
            return value.to_pydatetime()
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        raise ValueError(f"Unsupported date value: {value!r}")

    dates = [_to_datetime(value) for value in dates_raw]
    values = [float(v) if v is not None else 0.0 for v in values_raw]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, values, color="#1f77b4", linewidth=2)

    above = [value >= 100 for value in values]
    below = [not flag for flag in above]
    ax.fill_between(dates, 100, values, where=above, color="#2ca02c", alpha=0.2)
    ax.fill_between(dates, 100, values, where=below, color="#d62728", alpha=0.2)

    ax.axhline(100, color="#6c757d", linewidth=1, linestyle="--", alpha=0.7)

    ending_value = values[-1]
    total_return = ending_value / 100.0 - 1.0
    ax.set_title(f"Total return since inception: {total_return:+.2%}")
    ax.set_ylabel("Index (100 = start)")
    ax.set_xlabel("Date")

    formatter = FuncFormatter(lambda y, _: f"{y:.0f}")
    ax.yaxis.set_major_formatter(formatter)

    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


__all__ = ["plot_performance"]

