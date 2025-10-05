"""Historical price data loading and downloading utilities."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict

import pandas as pd
import yfinance as yf


PRICE_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


def _clean_csv(file_path: str) -> None:
    df = pd.read_csv(file_path)
    df = df.loc[df["Adj Close"].notna()]
    df = df[["Date", *PRICE_COLUMNS]]
    df.to_csv(file_path, index=False)


def _coerce_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in PRICE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_local_data(file_path: str) -> pd.DataFrame:
    ticker_data = pd.read_csv(file_path, parse_dates=["Date"]).set_index("Date")
    ticker_data = _coerce_price_columns(ticker_data)
    ticker_data = ticker_data.dropna(subset=["Adj Close"]).sort_index()
    return ticker_data


def load_price_data(
    tickers,
    start_date: datetime,
    today: datetime,
    price_folder: str,
) -> Dict[str, pd.DataFrame]:
    """Load or download price data for each ticker."""
    os.makedirs(price_folder, exist_ok=True)

    start_date_str = start_date.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")

    all_data: Dict[str, pd.DataFrame] = {}

    for ticker in tickers:
        file_path = os.path.join(price_folder, f"{ticker}.csv")
        ticker_data = pd.DataFrame()

        if os.path.exists(file_path):
            try:
                ticker_data = _load_local_data(file_path)
                print(
                    "Loaded %s data from local file. Range: %s -> %s"
                    % (
                        ticker,
                        ticker_data.index.min().date(),
                        ticker_data.index.max().date(),
                    )
                )
            except Exception as exc:
                print(
                    f"Warning: Failed to load {ticker} CSV. Will re-download. Error: {exc}"
                )
                ticker_data = pd.DataFrame()

        need_download = False
        if ticker_data.empty:
            need_download = True
        else:
            if ticker_data.index.min() > pd.to_datetime(start_date_str):
                need_download = True
                print(
                    f"{ticker} local data starts at {ticker_data.index.min().date()}, "
                    f"which is after requested start {start_date_str}. Will re-download from {start_date_str}."
                )
            elif ticker_data.index.max().strftime("%Y-%m-%d") < today_str:
                need_download = True

        if need_download:
            if ticker_data.empty or ticker_data.index.min() > pd.to_datetime(start_date_str):
                request_start = start_date_str
            else:
                request_start = (ticker_data.index.max() + timedelta(days=1)).strftime("%Y-%m-%d")

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
                new_data.to_csv(file_path, index=False)
                _clean_csv(file_path)
                ticker_data = _load_local_data(file_path)
                print(
                    "Downloaded/Updated %s data and saved locally. New range: %s -> %s"
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

        if not ticker_data.empty:
            ticker_data = ticker_data.loc[
                ticker_data.index >= pd.to_datetime(start_date_str)
            ]
            if ticker_data.empty:
                print(
                    f"After filtering to requested period ({start_date_str} -> {today_str}), {ticker} has NO data. Skipping."
                )
                continue
            ticker_data["Daily Return"] = ticker_data["Adj Close"].pct_change() * 100
        else:
            print(f"Skipping {ticker}: no valid price data.")
            continue

        all_data[ticker] = ticker_data

    return all_data
