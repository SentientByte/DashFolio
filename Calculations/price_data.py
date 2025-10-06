"""Historical price data loading and downloading utilities backed by SQLite."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict, Iterable, Tuple

import pandas as pd
import yfinance as yf

from .storage import connect, ensure_price_table

PRICE_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

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


def _persist_price_rows(conn, ticker: str, df: pd.DataFrame) -> None:
    if df.empty:
        return

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
                    new_data.reset_index(inplace=True)
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
