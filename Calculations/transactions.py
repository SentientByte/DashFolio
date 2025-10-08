"""Helpers for managing transaction history and derived holdings."""

from __future__ import annotations

import io
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pandas as pd

from .market_data import get_market_snapshot
from .storage import (
    connect,
    ensure_cash_balance_table,
    ensure_derived_holdings_table,
    ensure_transactions_table,
    read_cash_balance,
    write_cash_balance,
)
from .utils import safe_float


TransactionRecord = Dict[str, Any]
HoldingRecord = Dict[str, Any]


def _normalize_timestamp(value: Any) -> str:
    """Return an ISO8601 timestamp string for ``value``."""

    if value is None:
        raise ValueError("Transaction timestamp cannot be null")

    if isinstance(value, datetime):
        return value.astimezone().isoformat()

    parsed = pd.to_datetime(value, utc=False, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid transaction timestamp: {value}")
    return parsed.isoformat()


def _normalise_ticker(raw: Any) -> str:
    ticker = str(raw or "").strip().upper()
    if not ticker:
        raise ValueError("Transaction ticker must not be empty")
    return ticker


def _normalise_transaction(record: Dict[str, Any]) -> TransactionRecord:
    timestamp = _normalize_timestamp(record.get("timestamp"))
    ticker = _normalise_ticker(record.get("ticker"))
    quantity = safe_float(record.get("quantity"))
    price = safe_float(record.get("price"))
    commission = safe_float(record.get("commission"))

    if quantity == 0:
        raise ValueError("Transaction quantity must be non-zero")

    return {
        "timestamp": timestamp,
        "ticker": ticker,
        "quantity": quantity,
        "price": price,
        "commission": commission,
    }


def parse_transactions_csv(file_bytes: bytes) -> List[TransactionRecord]:
    """Parse uploaded CSV bytes into normalised transaction records."""

    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    if df.empty:
        return []

    column_map = {
        "symbol": "ticker",
        "ticker": "ticker",
        "date/time": "timestamp",
        "datetime": "timestamp",
        "date": "timestamp",
        "quantity": "quantity",
        "shares": "quantity",
        "price": "price",
        "trade price": "price",
        "commission": "commission",
        "fees": "commission",
    }

    normalised_columns: Dict[str, str] = {}
    for column in df.columns:
        key = str(column).strip().lower()
        mapped = column_map.get(key)
        if mapped:
            normalised_columns[column] = mapped

    df = df.rename(columns=normalised_columns)

    required = {"ticker", "timestamp", "quantity", "price"}
    missing = required - set(df.columns.str.lower())
    if missing:
        raise ValueError(
            "CSV is missing required columns: " + ", ".join(sorted(missing))
        )

    records: List[TransactionRecord] = []
    for _, row in df.iterrows():
        payload = {
            "timestamp": row.get("timestamp"),
            "ticker": row.get("ticker"),
            "quantity": row.get("quantity"),
            "price": row.get("price"),
            "commission": row.get("commission", 0.0),
        }
        try:
            normalised = _normalise_transaction(payload)
        except ValueError:
            continue
        records.append(normalised)

    records.sort(key=lambda rec: (rec["timestamp"], rec["ticker"]))
    return records


def _sort_transactions(transactions: Sequence[TransactionRecord]) -> List[TransactionRecord]:
    return sorted(transactions, key=lambda rec: rec["timestamp"])


def compute_holdings_from_transactions(
    transactions: Sequence[TransactionRecord],
) -> Tuple[List[HoldingRecord], float]:
    """Derive holdings and cash balance from a sequence of transaction records."""

    ledger: Dict[str, Dict[str, Any]] = {}
    cash_balance = 0.0

    for record in _sort_transactions(transactions):
        ticker = record["ticker"]
        quantity = safe_float(record.get("quantity"))
        price = safe_float(record.get("price"))
        commission_raw = safe_float(record.get("commission"))
        commission_cost = abs(commission_raw)
        timestamp = record.get("timestamp")

        entry = ledger.setdefault(
            ticker,
            {"quantity": 0.0, "total_cost": 0.0, "last_transaction_at": None},
        )

        if entry["last_transaction_at"] is None or timestamp > entry["last_transaction_at"]:
            entry["last_transaction_at"] = timestamp

        if quantity > 0:
            entry["total_cost"] += quantity * price + commission_cost
            entry["quantity"] += quantity
            cash_balance -= quantity * price + commission_cost
        else:
            sell_qty = min(entry["quantity"], abs(quantity))
            avg_cost = entry["total_cost"] / entry["quantity"] if entry["quantity"] else 0.0
            entry["total_cost"] -= avg_cost * sell_qty
            entry["total_cost"] += commission_cost
            entry["quantity"] -= sell_qty
            sale_value = abs(quantity) * price
            cash_balance += sale_value - commission_cost

            remaining = abs(quantity) - sell_qty
            if remaining > 0:
                # Treat excess as short exposure – track negative quantity with sale price cost basis.
                entry["quantity"] -= remaining
                entry["total_cost"] -= remaining * price

        if abs(entry["quantity"]) < 1e-9:
            entry["quantity"] = 0.0
            entry["total_cost"] = 0.0

    holdings: List[HoldingRecord] = []
    for ticker, entry in ledger.items():
        quantity = entry["quantity"]
        total_cost = entry["total_cost"]
        average_cost = total_cost / quantity if quantity else 0.0
        holdings.append(
            {
                "ticker": ticker,
                "quantity": quantity,
                "average_cost": average_cost,
                "total_cost": total_cost,
                "last_transaction_at": entry.get("last_transaction_at"),
            }
        )

    holdings.sort(key=lambda rec: rec["ticker"])
    if cash_balance < 0:
        cash_balance = 0.0
    return holdings, cash_balance


def load_transactions(db_path: str) -> List[TransactionRecord]:
    with connect(db_path) as conn:
        ensure_transactions_table(conn)
        cursor = conn.execute(
            "SELECT timestamp, ticker, quantity, price, commission "
            "FROM transactions ORDER BY timestamp, id"
        )
        rows = cursor.fetchall()
    return [
        {
            "timestamp": row[0],
            "ticker": row[1],
            "quantity": float(row[2]),
            "price": float(row[3]),
            "commission": float(row[4] or 0.0),
        }
        for row in rows
    ]


def load_current_holdings(db_path: str) -> List[HoldingRecord]:
    with connect(db_path) as conn:
        ensure_derived_holdings_table(conn)
        cursor = conn.execute(
            "SELECT ticker, quantity, average_cost, total_cost, last_transaction_at "
            "FROM derived_holdings ORDER BY ticker"
        )
        rows = cursor.fetchall()
    holdings: List[HoldingRecord] = []
    for row in rows:
        quantity = float(row[1])
        if abs(quantity) < 1e-9:
            continue
        holdings.append(
            {
                "ticker": row[0],
                "quantity": quantity,
                "average_cost": float(row[2] or 0.0),
                "total_cost": float(row[3] or 0.0),
                "last_transaction_at": row[4],
            }
        )
    return holdings


def _persist_transactions(conn, transactions: Sequence[TransactionRecord]) -> None:
    conn.execute("DELETE FROM transactions")
    for record in transactions:
        conn.execute(
            "INSERT INTO transactions (timestamp, ticker, quantity, price, commission) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                record["timestamp"],
                record["ticker"],
                float(record["quantity"]),
                float(record["price"]),
                float(record.get("commission", 0.0)),
            ),
        )


def _append_transactions(conn, transactions: Sequence[TransactionRecord]) -> None:
    for record in transactions:
        conn.execute(
            "INSERT INTO transactions (timestamp, ticker, quantity, price, commission) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                record["timestamp"],
                record["ticker"],
                float(record["quantity"]),
                float(record["price"]),
                float(record.get("commission", 0.0)),
            ),
        )


def _persist_holdings(
    conn,
    holdings: Sequence[HoldingRecord],
    cash_balance: float,
) -> None:
    ensure_derived_holdings_table(conn)
    conn.execute("DELETE FROM derived_holdings")
    timestamp = datetime.utcnow().isoformat() + "Z"
    for record in holdings:
        if abs(record.get("quantity", 0.0)) < 1e-9:
            continue
        conn.execute(
            "INSERT INTO derived_holdings (ticker, quantity, average_cost, total_cost, last_transaction_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                record["ticker"],
                float(record["quantity"]),
                float(record.get("average_cost", 0.0)),
                float(record.get("total_cost", 0.0)),
                record.get("last_transaction_at"),
                timestamp,
            ),
        )
    ensure_cash_balance_table(conn)
    write_cash_balance(conn, cash_balance)


def replace_transactions(
    db_path: str, transactions: Sequence[TransactionRecord]
) -> Tuple[List[HoldingRecord], float]:
    normalised = _sort_transactions([
        _normalise_transaction(record) for record in transactions
    ])
    holdings, cash_balance = compute_holdings_from_transactions(normalised)
    with connect(db_path) as conn:
        ensure_transactions_table(conn)
        ensure_derived_holdings_table(conn)
        _persist_transactions(conn, normalised)
        _persist_holdings(conn, holdings, cash_balance)
        conn.commit()
    return holdings, cash_balance


def append_transactions(
    db_path: str, transactions: Sequence[TransactionRecord]
) -> Tuple[List[HoldingRecord], float]:
    normalised = _sort_transactions([
        _normalise_transaction(record) for record in transactions
    ])
    existing = load_transactions(db_path)
    combined = _sort_transactions(existing + normalised)
    holdings, cash_balance = compute_holdings_from_transactions(combined)
    with connect(db_path) as conn:
        ensure_transactions_table(conn)
        ensure_derived_holdings_table(conn)
        _append_transactions(conn, normalised)
        _persist_holdings(conn, holdings, cash_balance)
        conn.commit()
    return holdings, cash_balance


def preview_holdings(
    db_path: str,
    transactions: Sequence[TransactionRecord],
    mode: str,
) -> Tuple[List[TransactionRecord], List[HoldingRecord], float]:
    """Return the resulting transactions and holdings for a preview operation."""

    new_records = _sort_transactions([
        _normalise_transaction(record) for record in transactions
    ])
    mode = (mode or "append").lower()
    if mode not in {"append", "replace"}:
        raise ValueError("Mode must be either 'append' or 'replace'")

    if mode == "replace":
        combined = new_records
    else:
        combined = _sort_transactions(load_transactions(db_path) + new_records)

    holdings, cash_balance = compute_holdings_from_transactions(combined)
    return combined, holdings, cash_balance


def fetch_holdings_with_market_values(
    holdings: Iterable[HoldingRecord],
) -> List[HoldingRecord]:
    enriched: List[HoldingRecord] = []
    for record in holdings:
        ticker = record["ticker"]
        market = get_market_snapshot(ticker)
        current_price = safe_float(market.get("current_price"))
        quantity = safe_float(record.get("quantity"))
        enriched.append(
            {
                **record,
                "current_price": current_price,
                "current_value": current_price * quantity,
            }
        )
    return enriched


def load_cash_balance(db_path: str) -> float:
    with connect(db_path) as conn:
        ensure_cash_balance_table(conn)
        return read_cash_balance(conn)
