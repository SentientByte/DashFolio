"""Performance table construction and index calculations.

The helpers in this module work with plain Python data structures while
optionally delegating to :mod:`pandas` when the dependency is available.
This keeps the calculations usable in constrained environments (e.g. the
unit-test environment used in this kata) without sacrificing ergonomics
for downstream callers that prefer working with dataframes.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, Iterator, List, MutableMapping, Optional, Sequence


try:  # pragma: no cover - optional dependency
    import pandas as _pd  # type: ignore
except Exception:  # pragma: no cover - fallback
    _pd = None  # type: ignore


# ---------------------------------------------------------------------------
# Lightweight dataframe implementation used when pandas is unavailable


@dataclass
class PerformanceFrame:
    """A minimal dataframe-like container."""

    _rows: List[MutableMapping[str, float]]

    def __post_init__(self) -> None:
        self._rows = [dict(row) for row in self._rows]

    @property
    def columns(self) -> List[str]:
        if not self._rows:
            return []
        return list(self._rows[0].keys())

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._rows)

    def __iter__(self) -> Iterator[MutableMapping[str, float]]:
        return iter(self._rows)

    def copy(self) -> "PerformanceFrame":
        return PerformanceFrame(self._rows)

    def to_dicts(self) -> List[MutableMapping[str, float]]:
        return [dict(row) for row in self._rows]

    def column(self, name: str) -> List[float]:
        return [row.get(name) for row in self._rows]

    def set_column(self, name: str, values: Sequence[float]) -> None:
        for row, value in zip(self._rows, values):
            row[name] = value
        if self._rows and name not in self._rows[0]:
            for row in self._rows:
                if name not in row:
                    row[name] = None

    def __getitem__(self, column: str) -> List[float]:  # pragma: no cover - sugar
        return self.column(column)


def _is_pandas_df(obj) -> bool:
    return _pd is not None and isinstance(obj, _pd.DataFrame)


def _to_records(data) -> List[MutableMapping]:
    if data is None:
        return []
    if _is_pandas_df(data):
        return list(data.to_dict(orient="records"))  # type: ignore[attr-defined]
    if isinstance(data, PerformanceFrame):
        return data.to_dicts()
    if isinstance(data, dict):
        # dict of lists
        keys = list(data.keys())
        length = len(data[keys[0]]) if keys else 0
        return [
            {key: data[key][idx] for key in keys}
            for idx in range(length)
        ]
    if isinstance(data, Iterable):
        return [dict(item) for item in data]
    raise TypeError("Unsupported input type for dataframe conversion")


def _make_dataframe(records: List[MutableMapping[str, float]]):
    if _pd is not None:  # pragma: no cover - depends on runtime
        return _pd.DataFrame(records)
    return PerformanceFrame(records)


def _clone_dataframe(df):
    if _is_pandas_df(df):  # pragma: no cover - depends on runtime
        return df.copy()
    if isinstance(df, PerformanceFrame):
        return df.copy()
    raise TypeError("Unsupported dataframe type")


def _get_column(df, name: str) -> List[float]:
    if _is_pandas_df(df):  # pragma: no cover - depends on runtime
        return list(df[name])
    if isinstance(df, PerformanceFrame):
        return df.column(name)
    raise TypeError("Unsupported dataframe type")


def _set_column(df, name: str, values: Sequence[float]) -> None:
    if _is_pandas_df(df):  # pragma: no cover - depends on runtime
        df[name] = list(values)
        return
    if isinstance(df, PerformanceFrame):
        df.set_column(name, values)
        return
    raise TypeError("Unsupported dataframe type")


# ---------------------------------------------------------------------------
# Utility functions


def _parse_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
    raise ValueError(f"Cannot parse datetime from {value!r}")


def _parse_float(value) -> float:
    if value is None:
        return 0.0
    return float(value)


def _daterange(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Public API


def build_daily_table(trades=None, prices=None, cash_events=None):
    """Construct the performance_daily table."""

    trade_records = _to_records(trades)
    price_records = _to_records(prices)
    cash_records = _to_records(cash_events)

    start_candidates: List[date] = []
    end_candidates: List[date] = []

    trade_map: Dict[date, List[Dict]] = defaultdict(list)
    if trade_records:
        trade_records.sort(key=lambda x: _parse_datetime(x["timestamp"]))
        trade_dates: List[date] = []
        for trade in trade_records:
            ts = _parse_datetime(trade["timestamp"])
            trade_map[ts.date()].append(trade)
            trade_dates.append(ts.date())
        if trade_dates:
            start_candidates.append(min(trade_dates))
            end_candidates.append(max(trade_dates))

    event_map: Dict[date, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if cash_records:
        event_dates: List[date] = []
        for event in cash_records:
            event_date = _parse_datetime(event["date"]).date()
            event_type = str(event["type"]).lower()
            event_map[event_date][event_type] += _parse_float(event["amount"])
            event_dates.append(event_date)
        if event_dates:
            start_candidates.append(min(event_dates))
            end_candidates.append(max(event_dates))

    price_map: Dict[str, List[tuple[date, float]]] = defaultdict(list)
    if price_records:
        for price in price_records:
            symbol = str(price["symbol"])
            price_date = _parse_datetime(price["date"]).date()
            price_map[symbol].append((price_date, _parse_float(price["close"])))
        for symbol in price_map:
            price_map[symbol].sort(key=lambda item: item[0])
        all_dates = [item[0] for prices_list in price_map.values() for item in prices_list]
        if all_dates:
            if not start_candidates:
                start_candidates.append(min(all_dates))
            end_candidates.append(max(all_dates))

    if not start_candidates:
        raise ValueError("No input data provided to build the performance table.")

    start_date = min(start_candidates)
    end_date = max(end_candidates or start_candidates)

    holdings: Dict[str, float] = defaultdict(float)
    cash_balance = 0.0

    price_indices = {symbol: 0 for symbol in price_map}
    latest_price: Dict[str, float] = {}
    latest_price_date: Dict[str, date] = {}

    records: List[Dict[str, float]] = []

    for current_date in _daterange(start_date, end_date):
        # Update prices
        for symbol, entries in price_map.items():
            idx = price_indices[symbol]
            while idx < len(entries) and entries[idx][0] <= current_date:
                latest_price[symbol] = entries[idx][1]
                latest_price_date[symbol] = entries[idx][0]
                idx += 1
            price_indices[symbol] = idx

        # Apply trades
        day_trades = trade_map.get(current_date, [])
        buys_cost = 0.0
        sells_proceeds = 0.0
        commissions = 0.0
        fees = 0.0

        for trade in day_trades:
            side = str(trade["side"]).lower()
            quantity = _parse_float(trade["qty"])
            price = _parse_float(trade["price"])
            symbol = str(trade["symbol"])
            commission = _parse_float(trade.get("commission"))
            fee = _parse_float(trade.get("fees"))

            if side in {"buy", "b", "long"}:
                holdings[symbol] += quantity
                buys_cost += quantity * price
            elif side in {"sell", "s", "short"}:
                holdings[symbol] -= quantity
                sells_proceeds += quantity * price
            else:
                raise ValueError(f"Unsupported trade side: {trade['side']!r}")

            commissions += commission
            fees += fee

        # Cash events
        events = event_map.get(current_date, {})
        deposits = events.get("deposit", 0.0)
        withdrawals = events.get("withdrawal", 0.0)
        external_fees = events.get("external_fee", 0.0)
        dividends = events.get("dividend", 0.0)
        interest = events.get("interest", 0.0)

        cash_balance = (
            cash_balance
            + deposits
            - withdrawals
            - external_fees
            - buys_cost
            + sells_proceeds
            + dividends
            + interest
            - commissions
            - fees
        )

        # Positions value
        positions_value = 0.0
        for symbol, qty in list(holdings.items()):
            if abs(qty) < 1e-9:
                holdings[symbol] = 0.0
                continue
            price = latest_price.get(symbol)
            if price is None:
                continue
            positions_value += qty * price

        equity = positions_value + cash_balance

        records.append(
            {
                "date": current_date,
                "positions_value": positions_value,
                "cash": cash_balance,
                "equity": equity,
                "external_cf": deposits - withdrawals - external_fees,
            }
        )

    return _make_dataframe(records)


def compute_twr(df):
    """Add time-weighted return columns to *df*."""

    if "equity" not in getattr(df, "columns", []) and not _is_pandas_df(df):
        raise ValueError("Dataframe must contain an 'equity' column.")

    result = _clone_dataframe(df)
    equities = _get_column(result, "equity")
    external_flows = _get_column(result, "external_cf")

    returns: List[float] = []
    index_values: List[float] = []
    flags: List[bool] = []

    prev_equity: Optional[float] = None
    index_value = 100.0

    for equity, external in zip(equities, external_flows):
        if prev_equity is None:
            daily_return = 0.0
            flag = equity is not None and equity <= 0
        else:
            if prev_equity is None or prev_equity <= 0:
                daily_return = 0.0
                flag = True
            else:
                daily_return = ((equity or 0.0) - (external or 0.0)) / prev_equity - 1.0
                flag = False

        index_value *= 1.0 + daily_return
        returns.append(daily_return)
        index_values.append(index_value)
        flags.append(flag)
        prev_equity = equity if equity is not None else 0.0

    _set_column(result, "daily_return_twr", returns)
    _set_column(result, "index_twr", index_values)
    _set_column(result, "twr_flag", flags)
    return result


def compute_equity_index(df):
    """Add the equity index to *df*."""

    result = _clone_dataframe(df)
    equities = _get_column(result, "equity")
    if not equities:
        _set_column(result, "index_equity", [])
        return result

    base_equity = equities[0] or 0.0
    if base_equity == 0:
        index_values = [100.0 for _ in equities]
    else:
        index_values = [100.0 * (equity or 0.0) / base_equity for equity in equities]

    _set_column(result, "index_equity", index_values)
    return result


__all__ = ["build_daily_table", "compute_twr", "compute_equity_index", "PerformanceFrame"]

