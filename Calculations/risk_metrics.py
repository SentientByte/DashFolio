"""Risk and performance metric calculations."""

from __future__ import annotations

import math
from typing import Tuple

import pandas as pd

from .utils import normalize_index


def compute_risk_metrics(
    price_history: pd.Series | None,
    benchmark_returns: pd.Series,
) -> Tuple[float, float, float, float, float]:
    """Return volatility, Sharpe ratio, maximum drawdown, beta, and EWMA VaR."""

    if price_history is None or price_history.empty:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    closes = price_history.dropna()
    if closes.empty or len(closes) < 2:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    closes = normalize_index(closes)
    returns = closes.pct_change().dropna()
    if returns.empty:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    daily_vol = returns.std()
    annualized_vol = daily_vol * math.sqrt(252) * 100 if daily_vol and not math.isnan(daily_vol) else 0.0

    avg_daily_return = returns.mean()
    if daily_vol and not math.isnan(daily_vol) and daily_vol != 0:
        sharpe = (avg_daily_return * 252) / (daily_vol * math.sqrt(252))
    else:
        sharpe = 0.0

    running_max = closes.cummax()
    drawdowns = (closes / running_max) - 1.0
    max_drawdown = drawdowns.min() * 100 if not drawdowns.empty else 0.0

    beta = 0.0
    if not benchmark_returns.empty:
        aligned = pd.concat([returns, benchmark_returns], axis=1, join="inner").dropna()
        if not aligned.empty:
            asset_returns = aligned.iloc[:, 0]
            bench_returns = aligned.iloc[:, 1]
            variance = bench_returns.var()
            covariance = asset_returns.cov(bench_returns)
            if variance and not math.isnan(variance):
                beta = covariance / variance if variance != 0 else 0.0

    ewma_var_pct = 0.0
    if not returns.empty:
        lambda_factor = 0.94
        squared_returns = returns.pow(2)
        ewma_variance = squared_returns.ewm(alpha=1 - lambda_factor, adjust=False).mean().iloc[-1]
        if ewma_variance and not math.isnan(ewma_variance) and ewma_variance > 0:
            z_score = 1.65  # 95% confidence
            ewma_sigma = math.sqrt(ewma_variance)
            ewma_var_pct = z_score * ewma_sigma * 100

    return annualized_vol, sharpe, max_drawdown, beta, ewma_var_pct
