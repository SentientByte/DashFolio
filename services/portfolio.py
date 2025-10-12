"""Portfolio persistence helpers."""
from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from Calculations.allocations import normalize_target_allocations
from Calculations.transactions import (
    load_cash_balance,
    load_cash_adjustments,
    load_current_holdings,
    load_transactions,
)

from app_paths import PORTFOLIO_FILE

_DEFAULT_PORTFOLIO: Dict[str, Any] = {
    "__comment": (
        "Placeholder portfolio generated automatically. Update holdings via the "
        "dashboard or by syncing transactions."
    ),
    "holdings": [],
    "target_allocations": {},
}


def _default_portfolio_snapshot() -> Dict[str, Any]:
    """Return a copy of the built-in portfolio defaults."""

    return deepcopy(_DEFAULT_PORTFOLIO)


def _ensure_portfolio_directory() -> None:
    """Ensure the directory that stores portfolio files exists."""

    Path(PORTFOLIO_FILE).parent.mkdir(parents=True, exist_ok=True)


def ensure_default_portfolio_file() -> None:
    """Create the default portfolio file when missing, tolerating read-only dirs."""

    _ensure_portfolio_directory()
    if os.path.exists(PORTFOLIO_FILE):
        return
    try:
        with open(PORTFOLIO_FILE, "w", encoding="utf-8") as file:
            json.dump(_DEFAULT_PORTFOLIO, file, indent=4)
    except OSError as exc:
        print(
            f"Warning: unable to write default portfolio file '{PORTFOLIO_FILE}'. {exc}"
        )
    else:
        print(f"Created default portfolio file: {PORTFOLIO_FILE}")


def load_portfolio_file() -> Dict[str, Any]:
    """Load the persisted portfolio or fall back to defaults when unavailable."""

    ensure_default_portfolio_file()
    try:
        with open(PORTFOLIO_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (FileNotFoundError, PermissionError, json.JSONDecodeError) as exc:
        print(
            "Warning: falling back to in-memory defaults because portfolio file "
            f"'{PORTFOLIO_FILE}' could not be read. {exc}"
        )
        data = _default_portfolio_snapshot()

    if not isinstance(data, dict):
        data = _default_portfolio_snapshot()

    data.setdefault("holdings", [])
    data.setdefault("target_allocations", {})
    return data


def save_portfolio_file(payload: Dict[str, Any]) -> None:
    """Persist the portfolio payload to disk if the location is writable."""

    _ensure_portfolio_directory()
    try:
        with open(PORTFOLIO_FILE, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=4)
    except OSError as exc:
        print(f"Warning: unable to persist portfolio to '{PORTFOLIO_FILE}'. {exc}")


def load_portfolio_state(data_store: str) -> Dict[str, Any]:
    raw = load_portfolio_file()
    metadata_lookup: Dict[str, Dict[str, Any]] = {}
    for entry in raw.get("holdings", []):
        ticker = str(entry.get("ticker", "")).upper().strip()
        if not ticker:
            continue
        metadata_lookup[ticker] = {
            "ticker": ticker,
            "logo_url": entry.get("logo_url"),
            "name": entry.get("name"),
        }

    holdings = load_current_holdings(data_store)
    transactions = load_transactions(data_store)
    cash_adjustments = load_cash_adjustments(data_store)
    for holding in holdings:
        ticker = str(holding.get("ticker", "")).upper()
        if not ticker:
            continue
        meta = metadata_lookup.get(ticker)
        if not meta:
            continue
        if meta.get("logo_url"):
            holding["logo_url"] = meta["logo_url"]
        if meta.get("name"):
            holding["name"] = meta["name"]

    normalized_targets = normalize_target_allocations(
        holdings,
        raw.get("target_allocations"),
    )
    cash_balance = load_cash_balance(data_store)
    metadata_list = sorted(metadata_lookup.values(), key=lambda item: item["ticker"])
    return {
        "holdings": holdings,
        "target_allocations": normalized_targets,
        "cash_balance": cash_balance,
        "metadata": metadata_list,
        "transactions": transactions,
        "cash_adjustments": cash_adjustments,
    }


def save_portfolio_state(data_store: str, data: Dict[str, Any]) -> None:
    payload = load_portfolio_file()
    if "target_allocations" in data:
        payload["target_allocations"] = data.get("target_allocations", {})
    if "holdings" in data:
        payload["holdings"] = data.get("holdings", [])
    save_portfolio_file(payload)
