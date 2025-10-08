"""SQLite storage helpers for DashFolio data."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator, Optional, Sequence, Tuple

import json
from datetime import datetime, timezone

PRICE_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS price_data (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    "Open" REAL,
    "High" REAL,
    "Low" REAL,
    "Close" REAL,
    "Adj Close" REAL,
    "Volume" REAL,
    PRIMARY KEY (ticker, date)
)
"""

RISK_RESULTS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS risk_analysis_results (
    data_period TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    ticker TEXT NOT NULL,
    trailing_stop_pct REAL NOT NULL,
    likelihood_pct REAL,
    potential_loss REAL,
    ewma_var REAL,
    PRIMARY KEY (data_period, generated_at, ticker, trailing_stop_pct)
)
"""


SNAPSHOT_CACHE_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    cache_key TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,
    holdings_fingerprint TEXT NOT NULL,
    benchmark TEXT,
    payload TEXT NOT NULL
)
"""


TRANSACTIONS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    ticker TEXT NOT NULL,
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    commission REAL DEFAULT 0
)
"""


PERFORMANCE_HISTORY_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS performance_history (
    date TEXT PRIMARY KEY,
    equity REAL NOT NULL,
    cash REAL NOT NULL,
    daily_return REAL NOT NULL
)
"""


DERIVED_HOLDINGS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS derived_holdings (
    ticker TEXT PRIMARY KEY,
    quantity REAL NOT NULL,
    average_cost REAL,
    total_cost REAL,
    last_transaction_at TEXT,
    updated_at TEXT NOT NULL
)
"""

CASH_BALANCE_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS cash_balances (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    balance REAL NOT NULL
)
"""

CASH_ADJUSTMENTS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS cash_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    amount REAL NOT NULL CHECK (amount >= 0),
    type TEXT NOT NULL CHECK (type IN ('deposit', 'withdraw', 'withdrawal', 'dividend', 'interest'))
)
"""


def ensure_directory(db_path: str) -> None:
    """Ensure the parent directory for the database exists."""
    directory = os.path.dirname(os.path.abspath(db_path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


@contextmanager
def connect(db_path: str) -> Iterator[sqlite3.Connection]:
    """Context manager returning a configured SQLite connection."""
    ensure_directory(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        yield conn
    finally:
        conn.close()


def ensure_price_table(conn: sqlite3.Connection) -> None:
    conn.execute(PRICE_TABLE_SCHEMA)
    conn.commit()


def ensure_risk_results_table(conn: sqlite3.Connection) -> None:
    conn.execute(RISK_RESULTS_TABLE_SCHEMA)
    conn.commit()


def ensure_snapshot_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(SNAPSHOT_CACHE_TABLE_SCHEMA)
    conn.commit()


def ensure_transactions_table(conn: sqlite3.Connection) -> None:
    conn.execute(TRANSACTIONS_TABLE_SCHEMA)
    conn.commit()


def ensure_derived_holdings_table(conn: sqlite3.Connection) -> None:
    conn.execute(DERIVED_HOLDINGS_TABLE_SCHEMA)
    conn.commit()


def ensure_cash_balance_table(conn: sqlite3.Connection) -> None:
    conn.execute(CASH_BALANCE_TABLE_SCHEMA)
    conn.commit()


def ensure_cash_adjustments_table(conn: sqlite3.Connection) -> None:
    conn.execute(CASH_ADJUSTMENTS_TABLE_SCHEMA)
    conn.commit()


def ensure_performance_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(PERFORMANCE_HISTORY_TABLE_SCHEMA)
    conn.commit()


def replace_performance_history(
    conn: sqlite3.Connection, rows: Sequence[Tuple[str, float, float, float]]
) -> None:
    conn.execute("DELETE FROM performance_history")
    if rows:
        conn.executemany(
            "INSERT INTO performance_history (date, equity, cash, daily_return) VALUES (?, ?, ?, ?)",
            rows,
        )
    conn.commit()


def read_performance_history(conn: sqlite3.Connection) -> list[dict]:
    cursor = conn.execute(
        "SELECT date, equity, cash, daily_return FROM performance_history ORDER BY date"
    )
    rows = []
    for date, equity, cash, daily_return in cursor.fetchall():
        rows.append(
            {
                "date": str(date),
                "equity": float(equity or 0.0),
                "cash": float(cash or 0.0),
                "daily_return": float(daily_return or 0.0),
            }
        )
    return rows


def read_cash_balance(conn: sqlite3.Connection) -> float:
    cursor = conn.execute("SELECT balance FROM cash_balances WHERE id = 1")
    row = cursor.fetchone()
    if not row:
        return 0.0
    try:
        return float(row[0])
    except (TypeError, ValueError):
        return 0.0


def write_cash_balance(conn: sqlite3.Connection, balance: float) -> None:
    conn.execute(
        "REPLACE INTO cash_balances (id, balance) VALUES (1, ?)",
        (float(balance),),
    )
    conn.commit()


def read_cash_adjustments(conn: sqlite3.Connection) -> list[dict]:
    cursor = conn.execute(
        "SELECT id, timestamp, amount, type FROM cash_adjustments ORDER BY timestamp, id"
    )
    rows = cursor.fetchall()
    adjustments: list[dict] = []
    for row in rows:
        adjustments.append(
            {
                "id": int(row[0]),
                "timestamp": row[1],
                "amount": float(row[2] or 0.0),
                "type": str(row[3] or "deposit"),
            }
        )
    return adjustments


def insert_cash_adjustment(
    conn: sqlite3.Connection, timestamp: str, amount: float, adj_type: str
) -> int:
    cursor = conn.execute(
        "INSERT INTO cash_adjustments (timestamp, amount, type) VALUES (?, ?, ?)",
        (timestamp, float(amount), adj_type),
    )
    conn.commit()
    return int(cursor.lastrowid)


def delete_cash_adjustment_record(conn: sqlite3.Connection, adjustment_id: int) -> None:
    conn.execute("DELETE FROM cash_adjustments WHERE id = ?", (int(adjustment_id),))
    conn.commit()


def read_cached_snapshot(
    conn: sqlite3.Connection, cache_key: str
) -> Optional[dict]:
    cursor = conn.execute(
        "SELECT payload, generated_at, benchmark FROM portfolio_snapshots WHERE cache_key = ?",
        (cache_key,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    payload, generated_at, benchmark = row
    try:
        snapshot = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return {
        "snapshot": snapshot,
        "generated_at": generated_at,
        "benchmark": benchmark,
    }


def write_cached_snapshot(
    conn: sqlite3.Connection,
    cache_key: str,
    fingerprint: str,
    benchmark: str | None,
    snapshot: dict,
) -> None:
    payload = json.dumps(snapshot)
    generated_at = snapshot.get("generated_at")
    if not generated_at:
        generated_at = datetime.now(timezone.utc).isoformat()
        snapshot["generated_at"] = generated_at
    conn.execute(
        "REPLACE INTO portfolio_snapshots (cache_key, generated_at, holdings_fingerprint, benchmark, payload)"
        " VALUES (?, ?, ?, ?, ?)",
        (
            cache_key,
            generated_at,
            fingerprint,
            benchmark,
            payload,
        ),
    )
    conn.commit()
