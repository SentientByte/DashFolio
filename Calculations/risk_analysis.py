"""Trailing stop likelihood and VaR calculations."""

from __future__ import annotations

import os
from typing import Dict, Tuple

import numpy as np
import pandas as pd


def _simulate_trailing_stop(
    ticker_data: pd.DataFrame,
    stop_loss_pct: float,
    num_sim: int,
    span: int,
) -> float:
    returns = ticker_data["Daily Return"].dropna() / 100
    if returns.empty:
        return float("nan")

    last_price = ticker_data["Adj Close"].iloc[-1]
    stop_price = last_price * (1 - stop_loss_pct / 100)

    mu = returns.ewm(span=span).mean().iloc[-1]
    sigma = returns.ewm(span=span).std().iloc[-1]

    if pd.isna(sigma) or sigma == 0:
        return 0.0

    simulations = np.random.normal(loc=mu, scale=sigma, size=(num_sim, 30))
    price_paths = last_price * np.cumprod(1 + simulations, axis=1)
    hit_stop = np.any(price_paths <= stop_price, axis=1)
    return float(np.mean(hit_stop))


def run_trailing_stop_analysis(
    df_portfolio: pd.DataFrame,
    all_data: Dict[str, pd.DataFrame],
    stop_range: Tuple[float, float],
    stop_step: float,
    num_simulations: int,
    span_ewma: int,
    confidence_level: float,
    data_period: str,
    output_dir: str,
) -> Tuple[pd.DataFrame, str]:
    """Run trailing stop likelihood simulation and save results."""
    results = []

    for _, row in df_portfolio.iterrows():
        ticker = row["Ticker"]
        ticker_data = all_data.get(ticker)
        if ticker_data is None or "Daily Return" not in ticker_data.columns:
            print(f"Skipping {ticker}: no data for risk analysis.")
            continue

        returns = ticker_data["Daily Return"].dropna() / 100
        if returns.empty:
            print(f"Skipping {ticker}: no returns in selected period.")
            continue

        last_price = ticker_data["Adj Close"].iloc[-1]
        position = row.get("Position", 1)

        stop_values = np.arange(
            stop_range[0],
            stop_range[1] + stop_step / 2,
            stop_step,
        )

        for stop_pct in stop_values:
            stop_pct = round(float(stop_pct), 2)
            likelihood = _simulate_trailing_stop(
                ticker_data,
                stop_pct,
                num_sim=num_simulations,
                span=span_ewma,
            )
            if np.isnan(likelihood):
                print(
                    f"{ticker} - stop {stop_pct}%: cannot compute likelihood (no returns). Skipping."
                )
                continue

            stop_price = last_price * (1 - stop_pct / 100)
            potential_loss = (last_price - stop_price) * position

            var_pct = -np.percentile(returns, (1 - confidence_level) * 100)
            var_value = var_pct * last_price * position

            results.append(
                {
                    "Ticker": ticker,
                    "Trailing Stop (%)": stop_pct,
                    "Likelihood of Activation (%)": likelihood * 100,
                    "Potential Loss ($)": potential_loss,
                    "EWMA VaR ($)": var_value,
                }
            )

    df_results = pd.DataFrame(results)
    print(f"\nTrailing Stop & Risk Analysis ({data_period}, EWMA):")
    print(df_results if not df_results.empty else "No results to display for selected period.")

    output_name = f"trailing_stop_analysis_ewma_{data_period.replace(' ', '_')}.xlsx"
    output_path = os.path.join(output_dir, output_name)
    df_results.to_excel(output_path, index=False)
    print(f"\nResults saved to {output_name}")

    return df_results, output_path
