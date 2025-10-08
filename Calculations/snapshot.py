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
from .utils import historical_close, normalize_index, safe_float


def build_portfolio_snapshot(
    holdings: List[Dict[str, Any]],
    target_allocations: Dict[str, Any] | None = None,
    benchmark_ticker: str | None = None,
    cash_balance: float = 0.0,
    transactions: List[Dict[str, Any]] | None = None,
    cash_adjustments: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    computed_holdings: List[Dict[str, Any]] = []
    total_cost = 0.0
    total_prev_value = 0.0
    total_week_reference_value = 0.0
    total_month_reference_value = 0.0
    total_current_value = 0.0
    top_mover: Dict[str, Any] | None = None

    benchmark_returns = get_benchmark_returns(benchmark=benchmark_ticker)
    benchmark_history = get_benchmark_history(benchmark=benchmark_ticker)
    portfolio_history: pd.Series | None = None
    cash_balance = max(safe_float(cash_balance), 0.0)
    transactions = transactions or []
    cash_adjustments = cash_adjustments or []

    def build_quantity_curves() -> Dict[str, pd.Series]:
        if not transactions:
            return {}
        try:
            df = pd.DataFrame(transactions)
        except Exception:
            return {}
        if df.empty or "timestamp" not in df.columns:
            return {}
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False, errors="coerce")
        df.dropna(subset=["timestamp"], inplace=True)
        if df.empty:
            return {}
        df.sort_values("timestamp", inplace=True)
        if "ticker" not in df.columns or "quantity" not in df.columns:
            return {}
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        df["quantity"] = df["quantity"].apply(safe_float)
        curves: Dict[str, pd.Series] = {}
        for ticker, group in df.groupby("ticker"):
            if not ticker or group.empty:
                continue
            daily = (
                group.set_index("timestamp")["quantity"].resample("D").sum().cumsum()
            )
            if daily.empty:
                continue
            series = normalize_index(daily.astype(float))
            curves[ticker] = series
        return curves

    def build_cash_value_series(index: pd.DatetimeIndex | None) -> pd.Series | None:
        events: List[tuple[pd.Timestamp, float]] = []
        for adjustment in cash_adjustments:
            timestamp = pd.to_datetime(
                adjustment.get("timestamp"), utc=False, errors="coerce"
            )
            if pd.isna(timestamp):
                continue
            delta = safe_float(adjustment.get("signed_amount"))
            if delta == 0:
                continue
            events.append((timestamp, delta))

        for transaction in transactions:
            timestamp = pd.to_datetime(
                transaction.get("timestamp"), utc=False, errors="coerce"
            )
            if pd.isna(timestamp):
                continue
            quantity = safe_float(transaction.get("quantity"))
            price = safe_float(transaction.get("price"))
            commission = abs(safe_float(transaction.get("commission")))
            if quantity > 0:
                delta = -(quantity * price + commission)
            else:
                delta = abs(quantity) * price - commission
            if delta == 0:
                continue
            events.append((timestamp, delta))

        if not events:
            if index is None or len(index) == 0:
                return None
            return pd.Series(float(cash_balance), index=index, dtype=float)

        df_events = pd.DataFrame(events, columns=["timestamp", "amount"])
        df_events.dropna(subset=["timestamp"], inplace=True)
        if df_events.empty:
            if index is None or len(index) == 0:
                return None
            return pd.Series(float(cash_balance), index=index, dtype=float)

        df_events["timestamp"] = pd.to_datetime(
            df_events["timestamp"], utc=False, errors="coerce"
        )
        df_events.dropna(subset=["timestamp"], inplace=True)
        if df_events.empty:
            if index is None or len(index) == 0:
                return None
            return pd.Series(float(cash_balance), index=index, dtype=float)

        df_events.sort_values("timestamp", inplace=True)
        df_events["amount"] = df_events["amount"].apply(safe_float)
        daily = (
            df_events.set_index("timestamp")["amount"].resample("D").sum().cumsum()
        )
        daily = normalize_index(daily.astype(float))

        if index is None or len(index) == 0:
            return daily

        series = daily.reindex(index, method="ffill")
        if series is None:
            series = pd.Series(dtype=float, index=index)
        series = series.ffill().fillna(0.0)
        if not series.empty:
            final_difference = cash_balance - float(series.iloc[-1])
            if abs(final_difference) > 1e-6:
                series = series + final_difference
        else:
            series = pd.Series(float(cash_balance), index=index, dtype=float)

        return series

    quantity_curves = build_quantity_curves()

    def resolve_reference_price(raw_value, history: pd.Series | None, days_back: int) -> float | None:
        if raw_value is not None:
            candidate = safe_float(raw_value)
            if candidate > 0:
                return candidate
        if history is not None and not history.empty:
            fallback = historical_close(history, days_back)
            if fallback is not None:
                candidate = safe_float(fallback)
                if candidate > 0:
                    return candidate
        return None

    for holding in holdings:
        ticker = str(holding.get("ticker", "")).upper().strip()
        quantity = safe_float(holding.get("quantity"))
        avg_cost = safe_float(holding.get("average_cost"))

        if not ticker or quantity <= 0:
            continue

        market = get_market_snapshot(ticker)
        current_price = safe_float(market.get("current_price"), default=0.0)
        previous_close = safe_float(market.get("previous_close"), default=current_price)
        price_history = market.get("price_history")
        week_close = resolve_reference_price(market.get("week_close"), price_history, 7)
        month_close = resolve_reference_price(market.get("month_close"), price_history, 30)
        open_price_raw = market.get("open_price")
        close_price_raw = market.get("close_price")
        adj_close_price_raw = market.get("adj_close_price")
        day_high_raw = market.get("day_high")
        day_low_raw = market.get("day_low")
        market_cap_raw = market.get("market_cap")
        ema_50_raw = market.get("ema_50")
        ema_200_raw = market.get("ema_200")
        rolling_high_raw = market.get("rolling_high_250")
        rolling_low_raw = market.get("rolling_low_250")

        open_price = safe_float(open_price_raw) if open_price_raw is not None else None
        close_price = safe_float(close_price_raw) if close_price_raw is not None else None
        adj_close_price = safe_float(adj_close_price_raw) if adj_close_price_raw is not None else None
        day_high = safe_float(day_high_raw) if day_high_raw is not None else None
        day_low = safe_float(day_low_raw) if day_low_raw is not None else None
        market_cap = safe_float(market_cap_raw) if market_cap_raw is not None else None
        ema_50 = safe_float(ema_50_raw) if ema_50_raw is not None else None
        ema_200 = safe_float(ema_200_raw) if ema_200_raw is not None else None
        rolling_high_250 = safe_float(rolling_high_raw) if rolling_high_raw is not None else None
        rolling_low_250 = safe_float(rolling_low_raw) if rolling_low_raw is not None else None

        logo_url = holding.get("logo_url") or None
        name = holding.get("name") or ticker

        total_cost_value = quantity * avg_cost
        current_value = quantity * current_price
        prev_value = quantity * previous_close if previous_close else 0.0
        todays_gain = current_value - prev_value
        todays_gain_pct = (todays_gain / prev_value * 100) if prev_value else 0.0

        weekly_reference_value = 0.0
        weekly_gain = 0.0
        weekly_gain_pct = 0.0
        if week_close is not None and week_close > 0:
            weekly_reference_value = quantity * week_close
            weekly_gain = (current_price - week_close) * quantity
            weekly_gain_pct = ((current_price - week_close) / week_close * 100) if week_close else 0.0

        monthly_reference_value = 0.0
        monthly_gain = 0.0
        monthly_gain_pct = 0.0
        if month_close is not None and month_close > 0:
            monthly_reference_value = quantity * month_close
            monthly_gain = (current_price - month_close) * quantity
            monthly_gain_pct = ((current_price - month_close) / month_close * 100) if month_close else 0.0

        pl_value = current_value - total_cost_value
        pl_pct = (pl_value / total_cost_value * 100) if total_cost_value else 0.0
        yield_on_cost_pct = (pl_value / total_cost_value * 100) if total_cost_value else 0.0

        annualized_vol, sharpe, max_drawdown, beta, ewma_var_pct = compute_risk_metrics(
            price_history,
            benchmark_returns,
        )
        price_history_points: List[Dict[str, Any]] = []
        if price_history is not None and not price_history.empty:
            normalized_history = normalize_index(price_history.astype(float))
            qty_series = quantity_curves.get(ticker)
            if qty_series is not None and not qty_series.empty:
                qty_series = qty_series.reindex(normalized_history.index, method="ffill")
                qty_series = qty_series.ffill().fillna(0.0)
            else:
                qty_series = pd.Series(
                    quantity, index=normalized_history.index, dtype=float
                )
            value_series = normalized_history.astype(float) * qty_series
            portfolio_history = (
                value_series
                if portfolio_history is None
                else portfolio_history.add(value_series, fill_value=0)
            )
            trimmed_history = normalized_history.tail(260)
            price_history_points = [
                {
                    "date": idx.strftime("%Y-%m-%d") if isinstance(idx, pd.Timestamp) else str(idx),
                    "close": float(value),
                }
                for idx, value in trimmed_history.dropna().items()
            ]

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
                "previous_close": previous_close,
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
                "ewma_var_pct": ewma_var_pct,
                "ewma_var_value": current_value * (ewma_var_pct / 100.0),
                "yield_on_cost_pct": yield_on_cost_pct,
                "open_price": open_price,
                "close_price": close_price,
                "adj_close_price": adj_close_price,
                "day_high_price": day_high,
                "day_low_price": day_low,
                "market_cap": market_cap,
                "ema_50": ema_50,
                "ema_200": ema_200,
                "rolling_high_250": rolling_high_250,
                "rolling_low_250": rolling_low_250,
                "price_history": price_history_points,
            }
        )

        total_cost += total_cost_value
        total_prev_value += prev_value
        total_current_value += current_value
        total_week_reference_value += weekly_reference_value
        total_month_reference_value += monthly_reference_value

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

    computed_holdings.sort(key=lambda record: record.get("current_value", 0.0), reverse=True)

    allocation_denominator = total_current_value if total_current_value else 1
    for holding in computed_holdings:
        holding["allocation_pct"] = (
            holding["current_value"] / allocation_denominator * 100 if allocation_denominator else 0.0
        )

    cash_series: pd.Series | None = None
    if portfolio_history is not None and not portfolio_history.empty:
        portfolio_history = portfolio_history.sort_index()
        cash_series = build_cash_value_series(portfolio_history.index)
        if cash_series is not None and not cash_series.empty:
            portfolio_history = portfolio_history.add(cash_series, fill_value=0.0)
        elif cash_balance:
            portfolio_history = portfolio_history + cash_balance

    normalized_targets = normalize_target_allocations(computed_holdings, target_allocations)
    for holding in computed_holdings:
        holding["target_pct"] = normalized_targets.get(holding["ticker"], 0.0)

    invested_value = total_current_value
    total_prev_value += cash_balance
    if total_week_reference_value:
        total_week_reference_value += cash_balance
    else:
        total_week_reference_value = cash_balance
    if total_month_reference_value:
        total_month_reference_value += cash_balance
    else:
        total_month_reference_value = cash_balance
    total_current_value += cash_balance

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
        "cash_balance": cash_balance,
        "invested_value": invested_value,
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
        portfolio_series = portfolio_history.ffill().dropna()
        if not portfolio_series.empty:
            benchmark_curve = None
            initial_value = float(portfolio_series.iloc[0]) if len(portfolio_series) else 0.0

            if benchmark_history is not None and not benchmark_history.empty:
                benchmark_history = benchmark_history.sort_index()
                benchmark_aligned = benchmark_history.reindex(
                    portfolio_series.index, method="ffill"
                ).dropna()
                if not benchmark_aligned.empty:
                    base_price = float(benchmark_aligned.iloc[0])
                    if base_price != 0:
                        benchmark_curve = (
                            benchmark_aligned / base_price
                        ) * initial_value
                        benchmark_curve = benchmark_curve.reindex(
                            portfolio_series.index
                        ).ffill()

            if (benchmark_curve is None or benchmark_curve.empty) and not benchmark_returns.empty:
                aligned_returns = benchmark_returns.reindex(
                    portfolio_series.index
                ).fillna(0.0)
                benchmark_curve = (1 + aligned_returns).cumprod() * initial_value
                benchmark_curve = benchmark_curve.reindex(portfolio_series.index).ffill()

            combined = pd.DataFrame(index=portfolio_series.index)
            combined["portfolio"] = portfolio_series.astype(float)
            if benchmark_curve is not None and not benchmark_curve.empty:
                combined["benchmark"] = benchmark_curve.astype(float)
            else:
                combined["benchmark"] = pd.NA

            performance_vs_benchmark = [
                {
                    "date": idx.strftime("%Y-%m-%d") if isinstance(idx, pd.Timestamp) else str(idx),
                    "portfolio": float(row["portfolio"]),
                    "benchmark": float(row["benchmark"])
                    if pd.notna(row["benchmark"])
                    else None,
                }
                for idx, row in combined.iterrows()
            ]

    return {
        "summary": summary,
        "holdings": computed_holdings,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "target_allocations": normalized_targets,
        "performance_vs_benchmark": performance_vs_benchmark,
        "cash_balance": cash_balance,
    }
