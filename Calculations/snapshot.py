"""Portfolio snapshot construction."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Literal, Optional, Sequence, Set

import pandas as pd
from pandas.api.types import is_datetime64tz_dtype

from services.activity_log import append_log

from .allocations import normalize_target_allocations
from .market_data import (
    get_benchmark_history,
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


def _to_naive_datetimes(values: Any) -> pd.Series:
    """Parse ``values`` into timezone-naive pandas datetimes."""

    parsed = pd.to_datetime(values, errors="coerce", utc=False)
    if isinstance(parsed, pd.Series):
        series = parsed
    else:
        series = pd.Series(parsed, index=getattr(values, "index", None))
    if is_datetime64tz_dtype(series.dtype):
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series


def _extract_flow_days(adjustments: Sequence[Dict[str, Any]] | None) -> Set[str]:
    """Return ISO dates where deposits or withdrawals occurred."""

    days: Set[str] = set()
    if not adjustments:
        return days
    for entry in adjustments:
        flow_type = _canonical_flow_type(entry.get("type"))
        if flow_type not in {"deposit", "withdrawal"}:
            continue
        timestamp = entry.get("timestamp")
        if not timestamp:
            timestamp = entry.get("date")
        parsed = pd.to_datetime(timestamp, utc=False, errors="coerce")
        if isinstance(parsed, pd.Timestamp) and parsed.tz is not None:
            parsed = parsed.tz_convert("UTC").tz_localize(None)
        if pd.isna(parsed):
            continue
        days.add(parsed.normalize().strftime("%Y-%m-%d"))
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


def _compute_realized_pl(transactions: Sequence[Dict[str, Any]] | None) -> float:
    """Estimate realised profit or loss from executed transactions."""

    if not transactions:
        return 0.0

    ledger: Dict[str, Dict[str, float]] = {}
    realised_total = 0.0

    try:
        ordered = sorted(transactions, key=lambda item: item.get("timestamp") or "")
    except Exception:
        ordered = transactions

    for record in ordered:
        ticker = str(record.get("ticker", "")).upper().strip()
        if not ticker:
            continue
        qty = safe_float(record.get("quantity"))
        price = safe_float(record.get("price"))
        commission = abs(safe_float(record.get("commission")))

        entry = ledger.setdefault(ticker, {"quantity": 0.0, "total_cost": 0.0})
        quantity = entry["quantity"]
        total_cost = entry["total_cost"]

        if qty > 0:
            # Purchase – increase position cost basis.
            entry["quantity"] = quantity + qty
            entry["total_cost"] = total_cost + qty * price + commission
            continue

        sell_amount = abs(qty)
        if quantity <= 0:
            # No existing long position to close; treat as new short exposure.
            entry["quantity"] = quantity - sell_amount
            entry["total_cost"] = total_cost - sell_amount * price
            realised_total -= commission
            continue

        realised_qty = min(sell_amount, quantity)
        avg_cost = total_cost / quantity if quantity else 0.0
        realised_total += (price - avg_cost) * realised_qty - commission

        entry["quantity"] = quantity - realised_qty
        entry["total_cost"] = max(entry["quantity"], 0.0) * avg_cost

        remaining = sell_amount - realised_qty
        if remaining > 0:
            # Excess sale opens a short position at the execution price.
            entry["quantity"] -= remaining
            entry["total_cost"] -= remaining * price

    return realised_total


def _build_metadata_lookup(
    holdings: Sequence[Dict[str, Any]] | None,
    metadata: Sequence[Dict[str, Any]] | None,
) -> Dict[str, Dict[str, Any]]:
    """Return a lookup table of optional ticker metadata."""

    lookup: Dict[str, Dict[str, Any]] = {}

    if metadata:
        for entry in metadata:
            ticker = str(entry.get("ticker", "")).upper().strip()
            if not ticker:
                continue
            record: Dict[str, Any] = {}
            logo = entry.get("logo_url")
            name = entry.get("name")
            if logo:
                record["logo_url"] = str(logo).strip()
            if name:
                record["name"] = str(name).strip()
            if record:
                lookup[ticker] = record

    if holdings:
        for entry in holdings:
            ticker = str(entry.get("ticker", "")).upper().strip()
            if not ticker:
                continue
            record = lookup.setdefault(ticker, {})
            logo = entry.get("logo_url")
            name = entry.get("name")
            if logo and not record.get("logo_url"):
                record["logo_url"] = str(logo).strip()
            if name and not record.get("name"):
                record["name"] = str(name).strip()

    return lookup


def _build_closed_positions(
    transactions: Sequence[Dict[str, Any]] | None,
    metadata_lookup: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    """Aggregate fully realised positions from the transaction ledger."""

    if not transactions:
        return []

    ledger: Dict[str, Dict[str, float]] = {}

    try:
        ordered = sorted(transactions, key=lambda item: item.get("timestamp") or "")
    except Exception:
        ordered = list(transactions)

    for record in ordered:
        ticker = str(record.get("ticker", "")).upper().strip()
        if not ticker:
            continue
        quantity = safe_float(record.get("quantity"))
        price = safe_float(record.get("price"))
        commission = abs(safe_float(record.get("commission")))

        entry = ledger.setdefault(
            ticker,
            {
                "net_quantity": 0.0,
                "buy_quantity": 0.0,
                "sell_quantity": 0.0,
                "buy_cost": 0.0,
                "sell_value": 0.0,
            },
        )

        if quantity > 0:
            entry["net_quantity"] += quantity
            entry["buy_quantity"] += quantity
            entry["buy_cost"] += quantity * price + commission
        elif quantity < 0:
            sell_amount = abs(quantity)
            entry["net_quantity"] -= sell_amount
            entry["sell_quantity"] += sell_amount
            entry["sell_value"] += sell_amount * price - commission

    closed: List[Dict[str, Any]] = []
    for ticker, entry in ledger.items():
        buy_qty = entry.get("buy_quantity", 0.0)
        sell_qty = entry.get("sell_quantity", 0.0)
        net_qty = entry.get("net_quantity", 0.0)
        if buy_qty <= 0 or sell_qty <= 0:
            continue
        if abs(net_qty) > 1e-6:
            continue

        buy_cost = entry.get("buy_cost", 0.0)
        sell_value = entry.get("sell_value", 0.0)
        avg_cost = buy_cost / buy_qty if buy_qty else 0.0
        avg_sell = sell_value / sell_qty if sell_qty else 0.0
        meta = (metadata_lookup or {}).get(ticker, {})

        closed.append(
            {
                "ticker": ticker,
                "logo_url": meta.get("logo_url"),
                "name": meta.get("name") or ticker,
                "quantity": sell_qty,
                "average_cost": avg_cost,
                "average_sell_price": avg_sell,
                "total_cost": buy_cost,
                "total_sell_value": sell_value,
                "profit_loss": sell_value - buy_cost,
            }
        )

    closed.sort(key=lambda item: item.get("total_sell_value", 0.0), reverse=True)
    return closed


def _optional_float(value: Any) -> Optional[float]:
    """Attempt to coerce ``value`` to ``float`` while preserving ``None``."""

    if value is None or pd.isna(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


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
    benchmark_history: pd.Series | None,
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
        tx_df["timestamp"] = _to_naive_datetimes(tx_df["timestamp"])
        tx_df.dropna(subset=["timestamp"], inplace=True)
        tx_df.sort_values("timestamp", inplace=True)
        tx_df["date"] = tx_df["timestamp"].dt.normalize()
    else:
        tx_df["date"] = pd.NaT

    if "ticker" in tx_df.columns:
        tx_df["ticker"] = tx_df["ticker"].astype(str).str.upper().str.strip()
    else:
        tx_df["ticker"] = ""

    if "quantity" in tx_df.columns:
        tx_df["quantity"] = tx_df["quantity"].apply(safe_float)
    else:
        tx_df["quantity"] = 0.0

    if "price" in tx_df.columns:
        tx_df["price"] = tx_df["price"].apply(safe_float)
    else:
        tx_df["price"] = 0.0

    if "commission" in tx_df.columns:
        tx_df["commission"] = tx_df["commission"].apply(safe_float).abs()
    else:
        tx_df["commission"] = 0.0

    adj_df = adj_df.copy()
    if "timestamp" in adj_df.columns:
        parsed_ts = _to_naive_datetimes(adj_df["timestamp"])
    else:
        parsed_ts = pd.Series(pd.NaT, index=adj_df.index, dtype="datetime64[ns]")

    if "date" in adj_df.columns:
        parsed_dates = pd.to_datetime(adj_df["date"], utc=False, errors="coerce")
        if isinstance(parsed_dates, pd.Series) and is_datetime64tz_dtype(parsed_dates.dtype):
            parsed_dates = parsed_dates.dt.tz_convert("UTC").dt.tz_localize(None)
        parsed_dates = parsed_dates.dt.normalize()
    else:
        parsed_dates = pd.Series(pd.NaT, index=adj_df.index, dtype="datetime64[ns]")

    adj_df["timestamp"] = parsed_ts
    adj_df["date"] = parsed_ts.dt.normalize().combine_first(parsed_dates)
    adj_df.dropna(subset=["date"], inplace=True)
    adj_df.sort_values(["date", "timestamp"], inplace=True)

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
    ny_zone = ZoneInfo("America/New_York")
    ny_now = datetime.now(ny_zone)
    today = pd.Timestamp(ny_now.date())
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

    base_index = pd.bdate_range(start=start_date, end=end_date)

    extra_days: Set[pd.Timestamp] = set()
    if "date" in tx_df.columns and tx_df["date"].notna().any():
        extra_days.update(tx_df.loc[tx_df["date"].notna(), "date"].tolist())
    if "date" in adj_df.columns and adj_df["date"].notna().any():
        extra_days.update(adj_df.loc[adj_df["date"].notna(), "date"].tolist())

    if extra_days:
        extra_index = pd.DatetimeIndex(sorted(extra_days))
        date_index = base_index.union(extra_index)
    else:
        date_index = base_index

    date_index = date_index.sort_values()

    benchmark_prices: Optional[pd.Series]
    benchmark_cumulative: Optional[pd.Series]
    benchmark_daily: Optional[pd.Series]
    benchmark_valid_series: Optional[pd.Series]
    if benchmark_history is not None and not benchmark_history.empty:
        series = normalize_index(benchmark_history.astype(float)).sort_index()
        if isinstance(series.index, pd.DatetimeIndex):
            series.index = series.index.normalize()
        series = series.reindex(date_index)
        benchmark_valid_series = series.notna()
        if benchmark_valid_series.any():
            first_valid = series[benchmark_valid_series].iloc[0]
            filled = series.fillna(first_valid).ffill()
            benchmark_prices = filled
            benchmark_daily = benchmark_prices.pct_change().fillna(0.0)
            benchmark_cumulative = (1.0 + benchmark_daily).cumprod() - 1.0
        else:
            benchmark_prices = pd.Series(0.0, index=date_index)
            benchmark_daily = pd.Series(0.0, index=date_index)
            benchmark_cumulative = pd.Series(0.0, index=date_index)
            benchmark_valid_series = pd.Series(False, index=date_index)
    else:
        benchmark_prices = None
        benchmark_cumulative = None
        benchmark_daily = None
        benchmark_valid_series = None

    tickers = [
        ticker
        for ticker in sorted(tx_df["ticker"].dropna().unique())
        if ticker and ticker != "nan"
    ]

    price_history: Dict[str, pd.Series] = {}
    price_history_valid: Dict[str, pd.Series] = {}
    if tickers and database_path:
        try:
            price_frames = load_price_data(
                tickers,
                datetime.combine(start_date.date(), datetime.min.time(), tzinfo=ny_zone),
                datetime.combine(end_date.date(), datetime.min.time(), tzinfo=ny_zone),
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
            aligned = closes.reindex(date_index)
            valid_mask = aligned.notna()
            if valid_mask.any():
                first_valid_value = aligned[valid_mask].iloc[0]
                filled = aligned.fillna(first_valid_value).ffill()
            else:
                filled = pd.Series(0.0, index=date_index)
                valid_mask = pd.Series(False, index=date_index)
            price_history[ticker] = filled
            price_history_valid[ticker] = valid_mask

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
        had_market_price = not tickers
        benchmark_has_price = False
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
            valid_series = price_history_valid.get(ticker)
            if price_series is not None:
                try:
                    candidate = price_series.loc[day]
                except KeyError:
                    candidate = None
                if candidate is not None and not pd.isna(candidate):
                    price_value = float(candidate)
                    if valid_series is not None:
                        try:
                            if bool(valid_series.loc[day]):
                                had_market_price = True
                        except KeyError:
                            pass
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

        if benchmark_cumulative is not None and benchmark_daily is not None:
            try:
                benchmark_has_price = bool(benchmark_valid_series is not None and bool(benchmark_valid_series.loc[day]))
            except KeyError:
                benchmark_has_price = False

        entry: Dict[str, float] = {
            "date": day.strftime("%Y-%m-%d"),
            "equity": float(equity_value),
            "cash": float(cash_balance),
            "portfolio_value": float(portfolio_value),
            "daily_return": float(daily_return),
            "cumulative_return": float(cumulative_return),
        }

        if benchmark_cumulative is not None and benchmark_daily is not None:
            entry["benchmark_daily_return"] = float(benchmark_daily.loc[day])
            entry["benchmark_cumulative_return"] = float(benchmark_cumulative.loc[day])

        entry["_had_market_price"] = had_market_price
        entry["_had_benchmark_price"] = benchmark_has_price
        entry["_had_activity"] = bool(tx_by_day.get(day) or adj_by_day.get(day))

        history.append(entry)

        previous_value = portfolio_value

    filtered_history: List[Dict[str, float]] = []
    series_started = False
    for entry in history:
        had_market_price = bool(entry.pop("_had_market_price", False))
        had_benchmark_price = bool(entry.pop("_had_benchmark_price", False))
        had_activity = bool(entry.pop("_had_activity", False))
        portfolio_value = safe_float(entry.get("portfolio_value"))

        meaningful = (
            had_market_price
            or had_benchmark_price
            or had_activity
            or abs(portfolio_value) > 1e-9
        )

        if not series_started:
            if meaningful:
                series_started = True
                filtered_history.append(entry)
            continue

        if had_market_price or had_benchmark_price or had_activity:
            filtered_history.append(entry)

    return filtered_history


def build_portfolio_snapshot(
    holdings: List[Dict[str, Any]],
    target_allocations: Dict[str, Any] | None = None,
    benchmark_ticker: str | None = None,
    cash_balance: float = 0.0,
    transactions: List[Dict[str, Any]] | None = None,
    cash_adjustments: List[Dict[str, Any]] | None = None,
    database_path: str | None = None,
    holdings_metadata: Sequence[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    append_log(
        f"Recalculating portfolio snapshot for {len(holdings)} holdings"
    )

    computed_holdings: List[Dict[str, Any]] = []
    total_cost = 0.0
    total_current_value = 0.0
    top_mover: Dict[str, Any] | None = None

    metadata_lookup = _build_metadata_lookup(holdings, holdings_metadata)
    benchmark_returns = get_benchmark_returns(benchmark=benchmark_ticker)
    invested_history: pd.Series | None = None
    cash_balance = max(safe_float(cash_balance), 0.0)
    transactions = transactions or []
    cash_adjustments = cash_adjustments or []
    realized_pl_value = _compute_realized_pl(transactions)

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
                "name": name or metadata_lookup.get(ticker, {}).get("name", ticker),
                "logo_url": logo_url or metadata_lookup.get(ticker, {}).get("logo_url"),
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
        invested_reference_current = float(invested_series.iloc[-1])
        previous_invested = (
            float(invested_series.iloc[-2]) if len(invested_series) > 1 else None
        )
        weekly_reference_value = historical_close(invested_series, 7)
        monthly_reference_value = historical_close(invested_series, 30)
    else:
        invested_reference_current = None
        previous_invested = None
        weekly_reference_value = None
        monthly_reference_value = None

    invested_current = total_current_value
    reference_current = (
        invested_reference_current if invested_reference_current is not None else invested_current
    )

    dod_value = reference_current - previous_invested if previous_invested is not None else 0.0
    dod_pct = (
        (dod_value / previous_invested) * 100
        if previous_invested is not None and previous_invested != 0
        else 0.0
    )

    weekly_change_value = (
        reference_current - weekly_reference_value if weekly_reference_value is not None else 0.0
    )
    weekly_change_pct = (
        (weekly_change_value / weekly_reference_value) * 100
        if weekly_reference_value not in (None, 0)
        else 0.0
    )

    monthly_change_value = (
        reference_current - monthly_reference_value if monthly_reference_value is not None else 0.0
    )
    monthly_change_pct = (
        (monthly_change_value / monthly_reference_value) * 100
        if monthly_reference_value not in (None, 0)
        else 0.0
    )

    total_portfolio_value = invested_current + cash_balance
    total_pl_value = total_portfolio_value - total_cost
    total_pl_pct = (total_pl_value / total_cost * 100) if total_cost else 0.0
    unrealized_pl_value = sum(
        safe_float(record.get("pl_value")) for record in computed_holdings
    )
    portfolio_beta = 0.0
    if total_current_value > 0:
        beta_numerator = 0.0
        for holding in computed_holdings:
            weight = safe_float(holding.get("current_value"))
            beta_component = safe_float(holding.get("beta_vs_benchmark"))
            beta_numerator += weight * beta_component
        portfolio_beta = beta_numerator / total_current_value if total_current_value else 0.0
    portfolio_var_value = sum(
        safe_float(record.get("ewma_var_value")) for record in computed_holdings
    )
    portfolio_var_pct = (
        (portfolio_var_value / total_current_value) * 100 if total_current_value else 0.0
    )

    closed_positions = _build_closed_positions(transactions, metadata_lookup)

    summary = {
        "total_cost": total_cost,
        "current_value": total_portfolio_value,
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
        "realized_pl_value": realized_pl_value,
        "unrealized_pl_value": unrealized_pl_value,
        "portfolio_beta": portfolio_beta,
        "portfolio_var_value": portfolio_var_value,
        "portfolio_var_pct": portfolio_var_pct,
    }

    if top_mover:
        summary["top_mover"] = {
            "ticker": top_mover.get("ticker"),
            "name": top_mover.get("name"),
            "change_value": top_mover.get("change_value"),
            "change_pct": top_mover.get("change_pct"),
        }

    benchmark_history = get_benchmark_history(benchmark=benchmark_ticker)

    performance_history = _build_daily_performance_history(
        transactions,
        cash_adjustments,
        database_path,
        benchmark_history,
    )

    if performance_history:
        latest_entry = performance_history[-1]
        latest_value = _optional_float(latest_entry.get("portfolio_value"))
        previous_value: Optional[float] = None
        for candidate in reversed(performance_history[:-1]):
            previous_value = _optional_float(candidate.get("portfolio_value"))
            if previous_value is not None:
                break
        if latest_value is not None:
            if previous_value is not None:
                dod_value = latest_value - previous_value
                dod_pct = (
                    (dod_value / previous_value) * 100
                    if abs(previous_value) > 1e-9
                    else 0.0
                )
            else:
                dod_value = 0.0
                dod_pct = 0.0
            summary["dod_value"] = dod_value
            summary["dod_pct"] = dod_pct

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

    append_log(
        "Portfolio snapshot updated · "
        f"value {total_portfolio_value:.2f} · realised P/L {realized_pl_value:.2f} · "
        f"unrealised P/L {unrealized_pl_value:.2f}"
    )

    return {
        "summary": summary,
        "holdings": computed_holdings,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "target_allocations": normalized_targets,
        "performance_history": performance_history,
        "performance_index": performance_history,
        "cash_balance": cash_balance,
        "benchmark_ticker": (benchmark_ticker or "").upper().strip() or None,
        "historical_positions": closed_positions,
    }
