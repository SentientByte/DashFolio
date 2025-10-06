"""Portfolio snapshot construction."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from .allocations import normalize_target_allocations
from .market_data import (
    get_benchmark_history,
    get_benchmark_returns,
    get_market_snapshot,
)
from .risk_metrics import compute_risk_metrics
from .utils import safe_float


def build_portfolio_snapshot(
    holdings: List[Dict[str, Any]],
    target_allocations: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    computed_holdings: List[Dict[str, Any]] = []
    total_cost = 0.0
    total_prev_value = 0.0
    total_week_reference_value = 0.0
    total_month_reference_value = 0.0
    total_current_value = 0.0
    top_mover: Dict[str, Any] | None = None

    benchmark_returns = get_benchmark_returns()
    benchmark_history = get_benchmark_history()
    portfolio_history: pd.Series | None = None

    for holding in holdings:
        ticker = str(holding.get("ticker", "")).upper().strip()
        quantity = safe_float(holding.get("quantity"))
        avg_cost = safe_float(holding.get("average_cost"))

        if not ticker or quantity <= 0:
            continue

        market = get_market_snapshot(ticker)
        current_price = safe_float(market.get("current_price"), default=0.0)
        previous_close = safe_float(market.get("previous_close"), default=current_price)
        week_close = safe_float(market.get("week_close"), default=previous_close)
        month_close = safe_float(market.get("month_close"), default=previous_close)

        logo_url = holding.get("logo_url") or None
        name = holding.get("name") or ticker

        total_cost_value = quantity * avg_cost
        current_value = quantity * current_price
        prev_value = quantity * previous_close if previous_close else 0.0
        todays_gain = current_value - prev_value
        todays_gain_pct = (todays_gain / prev_value * 100) if prev_value else 0.0

        weekly_value = quantity * week_close if week_close else 0.0
        weekly_gain = current_value - weekly_value
        weekly_gain_pct = (weekly_gain / weekly_value * 100) if weekly_value else 0.0

        monthly_value = quantity * month_close if month_close else 0.0
        monthly_gain = current_value - monthly_value
        monthly_gain_pct = (monthly_gain / monthly_value * 100) if monthly_value else 0.0

        pl_value = current_value - total_cost_value
        pl_pct = (pl_value / total_cost_value * 100) if total_cost_value else 0.0
        yield_on_cost_pct = (pl_value / total_cost_value * 100) if total_cost_value else 0.0

        annualized_vol, sharpe, max_drawdown, beta = compute_risk_metrics(
            market.get("price_history"),
            benchmark_returns,
        )

        price_history = market.get("price_history")
        if price_history is not None and not price_history.empty:
            value_series = price_history.astype(float) * quantity
            portfolio_history = (
                value_series
                if portfolio_history is None
                else portfolio_history.add(value_series, fill_value=0)
            )

        computed_holdings.append(
            {
                "ticker": ticker,
                "name": name,
                "logo_url": logo_url,
                "quantity": quantity,
                "average_cost": avg_cost,
                "current_price": current_price,
                "total_cost": total_cost_value,
                "current_value": current_value,
                "todays_gain": todays_gain,
                "todays_gain_pct": todays_gain_pct,
                "weekly_gain": weekly_gain,
                "weekly_gain_pct": weekly_gain_pct,
                "monthly_gain": monthly_gain,
                "monthly_gain_pct": monthly_gain_pct,
                "pl_value": pl_value,
                "pl_pct": pl_pct,
                "volatility_pct": annualized_vol,
                "sharpe_ratio": sharpe,
                "max_drawdown_pct": max_drawdown,
                "beta_vs_benchmark": beta,
                "yield_on_cost_pct": yield_on_cost_pct,
            }
        )

        total_cost += total_cost_value
        total_prev_value += prev_value
        total_current_value += current_value
        total_week_reference_value += weekly_value
        total_month_reference_value += monthly_value

        change_value = todays_gain
        change_pct = ((current_price - previous_close) / previous_close * 100) if previous_close else 0.0
        mover_metric = abs(change_value)
        if top_mover is None or mover_metric > top_mover.get("metric", 0):
            top_mover = {
                "ticker": ticker,
                "name": name,
                "change_value": change_value,
                "change_pct": change_pct,
                "metric": mover_metric,
            }

    allocation_denominator = total_current_value if total_current_value else 1
    for holding in computed_holdings:
        holding["allocation_pct"] = (
            holding["current_value"] / allocation_denominator * 100 if allocation_denominator else 0.0
        )

    if portfolio_history is not None and not portfolio_history.empty:
        portfolio_history = portfolio_history.sort_index()

    normalized_targets = normalize_target_allocations(computed_holdings, target_allocations)
    for holding in computed_holdings:
        holding["target_pct"] = normalized_targets.get(holding["ticker"], 0.0)

    dod_value = total_current_value - total_prev_value
    dod_pct = (dod_value / total_prev_value * 100) if total_prev_value else 0.0
    weekly_change_value = (
        total_current_value - total_week_reference_value if total_week_reference_value else 0.0
    )
    weekly_change_pct = (
        (weekly_change_value / total_week_reference_value) * 100
        if total_week_reference_value
        else 0.0
    )
    monthly_change_value = (
        total_current_value - total_month_reference_value if total_month_reference_value else 0.0
    )
    monthly_change_pct = (
        (monthly_change_value / total_month_reference_value) * 100
        if total_month_reference_value
        else 0.0
    )

    total_pl_value = total_current_value - total_cost
    total_pl_pct = (total_pl_value / total_cost * 100) if total_cost else 0.0

    summary = {
        "total_cost": total_cost,
        "current_value": total_current_value,
        "dod_value": dod_value,
        "dod_pct": dod_pct,
        "weekly_change_value": weekly_change_value,
        "weekly_change_pct": weekly_change_pct,
        "monthly_change_value": monthly_change_value,
        "monthly_change_pct": monthly_change_pct,
        "total_pl_value": total_pl_value,
        "total_pl_pct": total_pl_pct,
        "top_mover": None,
    }

    if top_mover:
        summary["top_mover"] = {
            "ticker": top_mover.get("ticker"),
            "name": top_mover.get("name"),
            "change_value": top_mover.get("change_value"),
            "change_pct": top_mover.get("change_pct"),
        }

    performance_vs_benchmark: List[Dict[str, Any]] = []
    if portfolio_history is not None and not portfolio_history.empty:
        portfolio_returns = portfolio_history.pct_change().fillna(0.0)
        if not portfolio_returns.empty:
            portfolio_curve = (1 + portfolio_returns).cumprod() * 100

            benchmark_curve = None
            if benchmark_history is not None and not benchmark_history.empty:
                benchmark_history = benchmark_history.sort_index()
                benchmark_history = benchmark_history.reindex(portfolio_curve.index, method="ffill")
                benchmark_returns_curve = benchmark_history.pct_change().fillna(0.0)
                benchmark_curve = (1 + benchmark_returns_curve).cumprod() * 100
            elif not benchmark_returns.empty:
                aligned_returns = benchmark_returns.reindex(portfolio_curve.index).fillna(0.0)
                benchmark_curve = (1 + aligned_returns).cumprod() * 100

            if benchmark_curve is not None and not benchmark_curve.empty:
                benchmark_curve = benchmark_curve.reindex(portfolio_curve.index).fillna(method="ffill").fillna(100.0)
                performance_vs_benchmark = [
                    {
                        "date": idx.strftime("%Y-%m-%d") if isinstance(idx, pd.Timestamp) else str(idx),
                        "portfolio": float(portfolio_curve.loc[idx]),
                        "benchmark": float(benchmark_curve.loc[idx]),
                    }
                    for idx in portfolio_curve.index
                ]

    return {
        "summary": summary,
        "holdings": computed_holdings,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "target_allocations": normalized_targets,
        "performance_vs_benchmark": performance_vs_benchmark,
    }
