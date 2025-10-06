"""SQLite storage helpers for DashFolio data."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

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
