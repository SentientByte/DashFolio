"""Statistical analysis helpers."""

from __future__ import annotations

from typing import Dict, Iterable

import numpy as np
import pandas as pd


def calculate_statistics(
    all_data: Dict[str, pd.DataFrame],
    tickers: Iterable[str],
    span_ewma: int,
    data_period: str,
) -> pd.DataFrame:
    """Calculate EWMA-based statistics for the provided tickers."""
    stats_list = []

    for ticker in tickers:
        ticker_data = all_data.get(ticker)
        if ticker_data is None or "Daily Return" not in ticker_data.columns:
            continue
        returns = ticker_data["Daily Return"].dropna() / 100
        if returns.empty:
            print(f"{ticker}: no returns in requested period -> skipping stats.")
            continue

        ewma_mu = returns.ewm(span=span_ewma).mean().iloc[-1]
        ewma_sigma = returns.ewm(span=span_ewma).std().iloc[-1]

        stats_list.append(
            {
                "Ticker": ticker,
                "EWMA Avg Daily Return (%)": ewma_mu * 100,
                "EWMA Annualized Volatility (%)": ewma_sigma * np.sqrt(252) * 100,
                "Max Daily Return (%)": returns.max() * 100,
                "Min Daily Return (%)": returns.min() * 100,
            }
        )

    df_stats = pd.DataFrame(stats_list)
    print(f"\nStatistics ({data_period}) using EWMA:")
    print(df_stats if not df_stats.empty else "No statistics available for selected period.")
    return df_stats
