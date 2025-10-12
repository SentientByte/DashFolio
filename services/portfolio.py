"""Portfolio persistence helpers."""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from Calculations.allocations import normalize_target_allocations
from Calculations.transactions import (
    load_cash_balance,
    load_cash_adjustments,
    load_current_holdings,
    load_transactions,
)

from app_paths import PORTFOLIO_FILE


def ensure_default_portfolio_file() -> None:
    if os.path.exists(PORTFOLIO_FILE):
        return
    default_portfolio = {"holdings": [], "target_allocations": {}}
    with open(PORTFOLIO_FILE, "w", encoding="utf-8") as file:
        json.dump(default_portfolio, file, indent=4)
    print(f"Created default portfolio file: {PORTFOLIO_FILE}")


def load_portfolio_file() -> Dict[str, Any]:
    ensure_default_portfolio_file()
    with open(PORTFOLIO_FILE, "r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            data = {}

    if not isinstance(data, dict):
        data = {}

    data.setdefault("holdings", [])
    data.setdefault("target_allocations", {})
    return data


def save_portfolio_file(payload: Dict[str, Any]) -> None:
    with open(PORTFOLIO_FILE, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=4)


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
