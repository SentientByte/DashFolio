"""Portfolio snapshot construction."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional, Sequence, Set

import pandas as pd

from .allocations import normalize_target_allocations
from .market_data import (
    get_benchmark_returns,
    get_market_snapshot,
)
from .price_data import load_price_data
from .storage import (
    connect,
    ensure_performance_history_table,
    replace_performance_history,
)
from .risk_metrics import compute_risk_metrics
from .utils import historical_close, normalize_index, safe_float


FlowKind = Literal["deposit", "withdrawal", "dividend", "interest"]


def _canonical_flow_type(raw: Any) -> FlowKind:
    """Return a normalised flow type."""

    value = str(raw or "deposit").strip().lower()
    if value == "withdraw":
        value = "withdrawal"
    if value not in {"deposit", "withdrawal", "dividend", "interest"}:
        value = "deposit"
    return value  # type: ignore[return-value]


def _extract_flow_days(adjustments: Sequence[Dict[str, Any]] | None) -> Set[str]:
    """Return ISO dates where deposits or withdrawals occurred."""

    days: Set[str] = set()
    if not adjustments:
        return days
    for entry in adjustments:
        flow_type = _canonical_flow_type(entry.get("type"))
        if flow_type not in {"deposit", "withdrawal"}:
            continue
        timestamp = pd.to_datetime(entry.get("timestamp"), utc=False, errors="coerce")
        if pd.isna(timestamp):
            continue
        days.add(timestamp.normalize().strftime("%Y-%m-%d"))
    return days


def _series_with_flow_days(series: pd.Series, flow_days: Set[str]) -> pd.Series:
    """Ensure ``series`` contains entries for each flow day."""

    if series is None or series.empty or not flow_days:
        return series

    flow_index = [
        pd.to_datetime(day, utc=False, errors="coerce") for day in sorted(flow_days)
    ]
    flow_index = [ts.normalize() for ts in flow_index if ts is not None and not pd.isna(ts)]
    if not flow_index:
        return series

    combined_index = series.index.union(pd.DatetimeIndex(flow_index))
    # Fill missing values with the last known invested value; when the
    # investment history has not yet started, default to zero so the index
    # remains neutral until capital is deployed.
    reindexed = series.reindex(combined_index).sort_index()
    reindexed = reindexed.ffill().fillna(0.0)
    return reindexed


def _build_twr_index(
    days: Sequence[str],
    invested: Dict[str, float],
    flow_days: Set[str],
    start: float = 100.0,
) -> Dict[str, float]:
    """Return a time-weighted return index for the invested series."""

    out: Dict[str, float] = {}
    if not days:
        return out

    base_day = days[0]
    out[base_day] = start
    index_value = start
    for i in range(1, len(days)):
        day = days[i]
        v0 = invested.get(base_day, 0.0)
        v1 = invested.get(day, 0.0)
        change = 0.0 if v0 == 0 else (v1 - v0) / v0
        index_value = index_value * (1 + change)
        out[day] = index_value
        if day in flow_days:
            base_day = day
    return out


def _series_to_iso_map(series: pd.Series) -> Dict[str, float]:
    """Convert a dated series into an ISO date -> float mapping."""

    mapping: Dict[str, float] = {}
    if series is None or series.empty:
        return mapping
    for idx, value in series.sort_index().items():
        if isinstance(idx, pd.Timestamp):
            key = idx.strftime("%Y-%m-%d")
        else:
            key = str(idx)
        mapping[key] = float(value)
    return mapping


def _build_daily_performance_history(
    transactions: Sequence[Dict[str, Any]] | None,
    cash_adjustments: Sequence[Dict[str, Any]] | None,
    database_path: str | None,
) -> List[Dict[str, float]]:
    """Construct daily performance metrics for the portfolio."""

    transactions = transactions or []
    cash_adjustments = cash_adjustments or []

    try:
        tx_df = pd.DataFrame(transactions)
    except Exception:
        tx_df = pd.DataFrame()

    try:
        adj_df = pd.DataFrame(cash_adjustments)
    except Exception:
        adj_df = pd.DataFrame()

    if tx_df.empty and adj_df.empty:
        return []

    tx_df = tx_df.copy()
    if "timestamp" in tx_df.columns:
        tx_df["timestamp"] = pd.to_datetime(tx_df["timestamp"], utc=False, errors="coerce")
        tx_df.dropna(subset=["timestamp"], inplace=True)
        tx_df.sort_values("timestamp", inplace=True)
        tx_df["date"] = tx_df["timestamp"].dt.normalize()
    else:
        tx_df["date"] = pd.NaT

    tx_df["ticker"] = tx_df.get("ticker", "").astype(str).str.upper().str.strip()
    tx_df["quantity"] = tx_df.get("quantity", 0).apply(safe_float)
    tx_df["price"] = tx_df.get("price", 0).apply(safe_float)
    tx_df["commission"] = tx_df.get("commission", 0).apply(safe_float).abs()

    adj_df = adj_df.copy()
    if "timestamp" in adj_df.columns:
        adj_df["timestamp"] = pd.to_datetime(adj_df["timestamp"], utc=False, errors="coerce")
        adj_df.dropna(subset=["timestamp"], inplace=True)
        adj_df.sort_values("timestamp", inplace=True)
        adj_df["date"] = adj_df["timestamp"].dt.normalize()
    else:
        adj_df["date"] = pd.NaT

    def _signed_adjustment(row: pd.Series) -> float:
        if "signed_amount" in row and pd.notna(row["signed_amount"]):
            return safe_float(row["signed_amount"])
        amount = safe_float(row.get("amount"))
        adj_type = _canonical_flow_type(row.get("type"))
        if adj_type in {"deposit", "dividend", "interest"}:
            return amount
        return -amount

    if not adj_df.empty:
        adj_df["signed_amount"] = adj_df.apply(_signed_adjustment, axis=1)

    candidate_dates: List[pd.Timestamp] = []
    if not tx_df.empty and tx_df["date"].notna().any():
        candidate_dates.append(tx_df.loc[tx_df["date"].notna(), "date"].iloc[0])
    if not adj_df.empty and adj_df["date"].notna().any():
        candidate_dates.append(adj_df.loc[adj_df["date"].notna(), "date"].iloc[0])

    if not candidate_dates:
        return []

    start_date = min(candidate_dates)
    today = pd.Timestamp.utcnow().normalize()
    last_tx_date = (
        tx_df.loc[tx_df["date"].notna(), "date"].iloc[-1]
        if not tx_df.empty and tx_df["date"].notna().any()
        else start_date
    )
    last_adj_date = (
        adj_df.loc[adj_df["date"].notna(), "date"].iloc[-1]
        if not adj_df.empty and adj_df["date"].notna().any()
        else start_date
    )
    end_date = max(today, last_tx_date, last_adj_date)

    date_index = pd.date_range(start=start_date, end=end_date, freq="D")

    tickers = [
        ticker
        for ticker in sorted(tx_df["ticker"].dropna().unique())
        if ticker and ticker != "nan"
    ]

    price_history: Dict[str, pd.Series] = {}
    if tickers and database_path:
        try:
            price_frames = load_price_data(
                tickers,
                start_date.to_pydatetime(),
                (end_date + timedelta(days=1)).to_pydatetime(),
                database_path,
            )
        except Exception as exc:
            print(f"Warning: failed to load price history for performance chart: {exc}")
            price_frames = {}
        for ticker in tickers:
            df = price_frames.get(ticker)
            if df is None or df.empty:
                continue
            if "Adj Close" in df.columns:
                closes = df["Adj Close"].copy()
            elif "Close" in df.columns:
                closes = df["Close"].copy()
            else:
                continue
            closes = closes.reindex(date_index).ffill().bfill()
            price_history[ticker] = closes

    tx_by_day: Dict[pd.Timestamp, List[pd.Series]] = {}
    if not tx_df.empty:
        for _, row in tx_df.iterrows():
            date = row.get("date")
            if pd.isna(date):
                continue
            tx_by_day.setdefault(date, []).append(row)

    adj_by_day: Dict[pd.Timestamp, float] = {}
    if not adj_df.empty:
        for _, row in adj_df.iterrows():
            date = row.get("date")
            if pd.isna(date):
                continue
            adj_by_day[date] = adj_by_day.get(date, 0.0) + safe_float(row.get("signed_amount"))

    holdings: Dict[str, float] = {ticker: 0.0 for ticker in tickers}
    last_trade_price: Dict[str, float] = {ticker: 0.0 for ticker in tickers}
    cash_balance = 0.0
    previous_value: Optional[float] = None
    cumulative_factor = 1.0
    history: List[Dict[str, float]] = []

    for day in date_index:
        cash_balance += adj_by_day.get(day, 0.0)

        for row in tx_by_day.get(day, []):
            ticker = row.get("ticker")
            if not ticker:
                continue
            quantity = safe_float(row.get("quantity"))
            price = safe_float(row.get("price"))
            commission = safe_float(row.get("commission"))
            current_qty = holdings.get(ticker, 0.0)
            updated_qty = current_qty + quantity
            if abs(updated_qty) < 1e-9:
                updated_qty = 0.0
            holdings[ticker] = updated_qty
            if price > 0:
                last_trade_price[ticker] = price
            if quantity > 0:
                cash_balance -= quantity * price + abs(commission)
            else:
                sale_value = abs(quantity) * price
                cash_balance += sale_value - abs(commission)

        if abs(cash_balance) < 1e-9:
            cash_balance = 0.0

        equity_value = 0.0
        for ticker, quantity in holdings.items():
            if abs(quantity) < 1e-9:
                continue
            price_series = price_history.get(ticker)
            price_value = None
            if price_series is not None:
                try:
                    candidate = price_series.loc[day]
                except KeyError:
                    candidate = None
                if candidate is not None and not pd.isna(candidate):
                    price_value = float(candidate)
            if price_value is None:
                price_value = last_trade_price.get(ticker, 0.0)
            equity_value += quantity * price_value

        portfolio_value = equity_value + cash_balance
        if abs(portfolio_value) < 1e-9:
            portfolio_value = 0.0

        if previous_value is not None and abs(previous_value) > 1e-9:
            daily_return = (portfolio_value - previous_value) / previous_value
        else:
            daily_return = 0.0

        cumulative_factor *= 1.0 + daily_return
        cumulative_return = cumulative_factor - 1.0

        history.append(
            {
                "date": day.strftime("%Y-%m-%d"),
                "equity": float(equity_value),
                "cash": float(cash_balance),
                "portfolio_value": float(portfolio_value),
                "daily_return": float(daily_return),
                "cumulative_return": float(cumulative_return),
            }
        )

        previous_value = portfolio_value

    return history


def build_portfolio_snapshot(
    holdings: List[Dict[str, Any]],
    target_allocations: Dict[str, Any] | None = None,
    benchmark_ticker: str | None = None,
    cash_balance: float = 0.0,
    transactions: List[Dict[str, Any]] | None = None,
    cash_adjustments: List[Dict[str, Any]] | None = None,
    database_path: str | None = None,
) -> Dict[str, Any]:
    computed_holdings: List[Dict[str, Any]] = []
    total_cost = 0.0
    total_current_value = 0.0
    top_mover: Dict[str, Any] | None = None

    benchmark_returns = get_benchmark_returns(benchmark=benchmark_ticker)
    invested_history: pd.Series | None = None
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
        previous_close_raw = market.get("previous_close")
        previous_close: Optional[float]
        if previous_close_raw is None:
            previous_close = None
        else:
            candidate = safe_float(previous_close_raw)
            previous_close = candidate if candidate > 0 else None
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
        prev_value = quantity * previous_close if previous_close is not None else None
        if previous_close is not None and previous_close > 0:
            todays_gain = current_value - prev_value
            todays_gain_pct = (
                (current_price - previous_close) / previous_close * 100
                if previous_close
                else 0.0
            )
        else:
            todays_gain = None
            todays_gain_pct = None

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
            invested_history = (
                value_series
                if invested_history is None
                else invested_history.add(value_series, fill_value=0)
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
        total_current_value += current_value

        if todays_gain is not None and todays_gain_pct is not None:
            change_value = todays_gain
            change_pct = todays_gain_pct
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

    normalized_targets = normalize_target_allocations(computed_holdings, target_allocations)
    for holding in computed_holdings:
        holding["target_pct"] = normalized_targets.get(holding["ticker"], 0.0)
    flow_days = _extract_flow_days(cash_adjustments)
    invested_series: Optional[pd.Series] = None
    if invested_history is not None and not invested_history.empty:
        invested_series = invested_history.sort_index().astype(float)
        invested_series = invested_series.ffill()
        invested_series = _series_with_flow_days(invested_series, flow_days)

    if invested_series is not None and not invested_series.empty:
        invested_current = float(invested_series.iloc[-1])
        previous_invested = float(invested_series.iloc[-2]) if len(invested_series) > 1 else None
        weekly_reference_value = historical_close(invested_series, 7)
        monthly_reference_value = historical_close(invested_series, 30)
    else:
        invested_current = total_current_value
        previous_invested = None
        weekly_reference_value = None
        monthly_reference_value = None

    dod_value = invested_current - previous_invested if previous_invested is not None else 0.0
    dod_pct = (
        (dod_value / previous_invested) * 100
        if previous_invested is not None and previous_invested != 0
        else 0.0
    )

    weekly_change_value = (
        invested_current - weekly_reference_value if weekly_reference_value is not None else 0.0
    )
    weekly_change_pct = (
        (weekly_change_value / weekly_reference_value) * 100
        if weekly_reference_value not in (None, 0)
        else 0.0
    )

    monthly_change_value = (
        invested_current - monthly_reference_value if monthly_reference_value is not None else 0.0
    )
    monthly_change_pct = (
        (monthly_change_value / monthly_reference_value) * 100
        if monthly_reference_value not in (None, 0)
        else 0.0
    )

    total_pl_value = invested_current - total_cost
    total_pl_pct = (total_pl_value / total_cost * 100) if total_cost else 0.0

    summary = {
        "total_cost": total_cost,
        "current_value": invested_current + cash_balance,
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
        "invested_value": invested_current,
    }

    if top_mover:
        summary["top_mover"] = {
            "ticker": top_mover.get("ticker"),
            "name": top_mover.get("name"),
            "change_value": top_mover.get("change_value"),
            "change_pct": top_mover.get("change_pct"),
        }

    performance_history = _build_daily_performance_history(
        transactions,
        cash_adjustments,
        database_path,
    )

    if database_path:
        try:
            with connect(database_path) as conn:
                ensure_performance_history_table(conn)
                replace_performance_history(
                    conn,
                    [
                        (
                            entry["date"],
                            entry["equity"],
                            entry["cash"],
                            entry["daily_return"],
                        )
                        for entry in performance_history
                    ],
                )
        except Exception as exc:
            print(f"Warning: failed to persist performance history: {exc}")

    return {
        "summary": summary,
        "holdings": computed_holdings,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "target_allocations": normalized_targets,
        "performance_history": performance_history,
        "performance_index": performance_history,
        "cash_balance": cash_balance,
    }
