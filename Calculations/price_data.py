"""Historical price data loading and downloading utilities backed by SQLite."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict, Iterable, Tuple

import pandas as pd
import yfinance as yf

from .storage import connect, ensure_price_table

PRICE_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


def _normalise_key(value: str) -> str:
    """Normalise a column label to compare against canonical names."""

    if not isinstance(value, str):
        return ""

    key = value.strip().lower()
    for char in ("_", "-", "*", ".", "/"):
        key = key.replace(char, " ")
    return " ".join(key.split())


CANONICAL_NAME_BY_KEY = {
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "adj close": "Adj Close",
    "adjclose": "Adj Close",
    "volume": "Volume",
}


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure downloaded data has a flat column index.

    When yfinance returns data for a single ticker it sometimes still includes
    a multi-index where the last element is the actual column name.  Pandas
    will then treat ``df["Adj Close"]`` as a DataFrame rather than a Series,
    which later causes "Cannot index with multidimensional key" errors when we
    attempt to filter rows.  Normalising the column index keeps the rest of the
    logic simple and resilient to either single-level or multi-level inputs.
    """

    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        flattened = []
        for col in df.columns:
            if isinstance(col, tuple):
                canonical = None
                fallback_parts = []
                for item in col:
                    if item in (None, ""):
                        continue
                    key = _normalise_key(item)
                    if not canonical and key in CANONICAL_NAME_BY_KEY:
                        canonical = CANONICAL_NAME_BY_KEY[key]
                        break
                    fallback_parts.append(str(item))
                if canonical:
                    flattened.append(canonical)
                else:
                    flattened.append("_".join(fallback_parts) if fallback_parts else str(col[0]))
            else:
                flattened.append(col)
        df.columns = flattened
    return df

INSERT_PRICE_SQL = """
INSERT OR REPLACE INTO price_data (
    ticker, date, "Open", "High", "Low", "Close", "Adj Close", "Volume"
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


def _coerce_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in PRICE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_local_data(conn, ticker: str) -> pd.DataFrame:
    query = (
        "SELECT date as Date, \"Open\", \"High\", \"Low\", \"Close\", "
        "\"Adj Close\", \"Volume\" FROM price_data WHERE ticker = ? ORDER BY date"
    )
    df = pd.read_sql_query(query, conn, params=(ticker,), parse_dates=["Date"])
    if df.empty:
        return df
    df = df.set_index("Date")
    df = _coerce_price_columns(df)
    df = df.dropna(subset=["Adj Close"]).sort_index()
    return df


def _normalise_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename yfinance columns to the canonical set expected by the app."""

    if df.empty:
        return df

    rename_map = {}
    for col in df.columns:
        if not isinstance(col, str):
            continue

        key = _normalise_key(col)

        if key in CANONICAL_NAME_BY_KEY:
            rename_map[col] = CANONICAL_NAME_BY_KEY[key]

    if rename_map:
        df = df.rename(columns=rename_map)

    # Ensure the canonical columns exist even if the download omitted one of
    # them (for example, some indices occasionally miss Volume).
    for col in ["Date", *PRICE_COLUMNS]:
        if col not in df.columns:
            df[col] = pd.NA

    return df


def _persist_price_rows(conn, ticker: str, df: pd.DataFrame) -> None:
    if df.empty:
        return

    df = _flatten_columns(df)
    df = _normalise_price_columns(df)
    df = df.loc[df["Adj Close"].notna(), ["Date", *PRICE_COLUMNS]]
    if df.empty:
        return

    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = _coerce_price_columns(df)

    rows: Iterable[Tuple] = (
        (
            ticker,
            row["Date"],
            *(None if pd.isna(row[col]) else float(row[col]) for col in PRICE_COLUMNS[:-1]),
            None if pd.isna(row["Volume"]) else float(row["Volume"]),
        )
        for _, row in df.iterrows()
    )

    conn.executemany(INSERT_PRICE_SQL, rows)


def load_price_data(
    tickers,
    start_date: datetime,
    today: datetime,
    database_path: str,
) -> Dict[str, pd.DataFrame]:
    """Load or download price data for each ticker into SQLite."""

    os.makedirs(os.path.dirname(os.path.abspath(database_path)), exist_ok=True)

    start_date_str = start_date.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")

    all_data: Dict[str, pd.DataFrame] = {}

    with connect(database_path) as conn:
        ensure_price_table(conn)

        for ticker in tickers:
            ticker_data = _load_local_data(conn, ticker)

            if not ticker_data.empty:
                print(
                    "Loaded %s data from SQLite. Range: %s -> %s"
                    % (
                        ticker,
                        ticker_data.index.min().date(),
                        ticker_data.index.max().date(),
                    )
                )

            need_download = ticker_data.empty
            request_start = start_date_str
            refresh_all = ticker_data.empty

            if not ticker_data.empty:
                earliest = ticker_data.index.min()
                latest = ticker_data.index.max()

                if earliest > pd.to_datetime(start_date_str):
                    print(
                        f"{ticker} local data starts at {earliest.date()}, "
                        f"which is after requested start {start_date_str}. Will refresh from {start_date_str}."
                    )
                    need_download = True
                    refresh_all = True
                    request_start = start_date_str
                elif latest.strftime("%Y-%m-%d") < today_str:
                    need_download = True
                    refresh_all = False
                    request_start = (latest + timedelta(days=1)).strftime("%Y-%m-%d")

            if need_download:
                print(f"Requesting {ticker} data from {request_start} to {today_str}...")
                new_data = yf.download(
                    ticker,
                    start=request_start,
                    end=today_str,
                    interval="1d",
                    auto_adjust=False,
                )
                if not new_data.empty:
                    new_data = _flatten_columns(new_data)
                    new_data.reset_index(inplace=True)
                    new_data = _flatten_columns(new_data)
                    if refresh_all:
                        conn.execute("DELETE FROM price_data WHERE ticker = ?", (ticker,))
                    _persist_price_rows(conn, ticker, new_data)
                    conn.commit()
                    ticker_data = _load_local_data(conn, ticker)
                    if not ticker_data.empty:
                        print(
                            "Stored %s data in SQLite. New range: %s -> %s"
                            % (
                                ticker,
                                ticker_data.index.min().date(),
                                ticker_data.index.max().date(),
                            )
                        )
                else:
                    range_info = (
                        ticker_data.index.min() if not ticker_data.empty else "none"
                    )
                    print(
                        f"Warning: No new data returned for {ticker}. Current local range (if any): {range_info}"
                    )

            if ticker_data.empty:
                print(f"Skipping {ticker}: no valid price data.")
                continue

            ticker_data = ticker_data.loc[
                ticker_data.index >= pd.to_datetime(start_date_str)
            ]
            if ticker_data.empty:
                print(
                    f"After filtering to requested period ({start_date_str} -> {today_str}), {ticker} has NO data. Skipping."
                )
                continue

            ticker_data["Daily Return"] = ticker_data["Adj Close"].pct_change() * 100
            all_data[ticker] = ticker_data

    return all_data
