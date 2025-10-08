"""Helpers for caching portfolio snapshots in SQLite."""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .snapshot import build_portfolio_snapshot
from .storage import (
    connect,
    ensure_snapshot_cache_table,
    read_cached_snapshot,
    write_cached_snapshot,
)
from .utils import safe_float

# Minimum number of seconds between background refreshes for the same cache key.
_MIN_REFRESH_INTERVAL_SECONDS = 120

# Tracks in-flight background refresh threads keyed by cache key to avoid duplicates.
_refresh_threads: Dict[str, threading.Thread] = {}
_refresh_lock = threading.Lock()


def _canonical_holdings(holdings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    canonical: List[Dict[str, Any]] = []
    for entry in holdings:
        ticker = str(entry.get("ticker", "")).upper().strip()
        if not ticker:
            continue
        logo_url = entry.get("logo_url")
        if logo_url is not None:
            logo_url = str(logo_url).strip()
        name = entry.get("name")
        if name is not None:
            name = str(name).strip()
        canonical.append(
            {
                "ticker": ticker,
                "quantity": safe_float(entry.get("quantity")),
                "average_cost": safe_float(entry.get("average_cost")),
                "logo_url": logo_url,
                "name": name,
            }
        )
    canonical.sort(key=lambda item: item["ticker"])
    return canonical


def _canonical_targets(targets: Dict[str, Any] | None) -> Dict[str, float]:
    if not targets:
        return {}
    cleaned: List[Tuple[str, float]] = []
    for ticker, value in targets.items():
        key = str(ticker).upper().strip()
        if not key:
            continue
        cleaned.append((key, safe_float(value)))
    cleaned.sort(key=lambda item: item[0])
    return {ticker: value for ticker, value in cleaned}


def _normalize_timestamp_for_cache(value: Any) -> str:
    if value is None:
        return ""
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    return parsed.isoformat()


def _canonical_transactions(transactions: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    if not transactions:
        return []
    canonical: List[Dict[str, Any]] = []
    for entry in transactions:
        canonical.append(
            {
                "timestamp": _normalize_timestamp_for_cache(entry.get("timestamp")),
                "ticker": str(entry.get("ticker", "")).upper().strip(),
                "quantity": round(safe_float(entry.get("quantity")), 6),
                "price": round(safe_float(entry.get("price")), 6),
                "commission": round(safe_float(entry.get("commission")), 6),
            }
        )
    canonical.sort(
        key=lambda item: (
            item["timestamp"],
            item["ticker"],
            item["price"],
            item["quantity"],
        )
    )
    return canonical


def _canonical_cash_adjustments(
    adjustments: List[Dict[str, Any]] | None,
) -> List[Dict[str, Any]]:
    if not adjustments:
        return []
    canonical: List[Dict[str, Any]] = []
    for entry in adjustments:
        adj_type = str(entry.get("type", "deposit")).strip().lower()
        amount = round(safe_float(entry.get("amount")), 6)
        signed = round(safe_float(entry.get("signed_amount")), 6)
        canonical.append(
            {
                "timestamp": _normalize_timestamp_for_cache(entry.get("timestamp")),
                "type": adj_type,
                "amount": amount,
                "signed": signed,
            }
        )
    canonical.sort(key=lambda item: (item["timestamp"], item["type"], item["amount"]))
    return canonical


def _generate_cache_key(
    holdings: List[Dict[str, Any]],
    targets: Dict[str, Any] | None,
    benchmark: str | None,
    cash_balance: float,
    transactions: List[Dict[str, Any]] | None,
    cash_adjustments: List[Dict[str, Any]] | None,
) -> Tuple[str, str]:
    canonical_payload = {
        "benchmark": (benchmark or "").upper().strip(),
        "holdings": _canonical_holdings(holdings),
        "targets": _canonical_targets(targets),
        "cash": round(safe_float(cash_balance), 6),
        "transactions": _canonical_transactions(transactions),
        "cash_adjustments": _canonical_cash_adjustments(cash_adjustments),
    }
    encoded = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
    cache_key = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return cache_key, encoded


def _should_refresh(existing_timestamp: Optional[str]) -> bool:
    if not existing_timestamp:
        return True
    try:
        parsed = datetime.fromisoformat(existing_timestamp.replace("Z", "+00:00"))
    except ValueError:
        return True
    age = datetime.now(timezone.utc) - parsed
    return age.total_seconds() >= _MIN_REFRESH_INTERVAL_SECONDS


def _refresh_worker(
    cache_key: str,
    fingerprint: str,
    db_path: str,
    holdings: List[Dict[str, Any]],
    targets: Dict[str, Any] | None,
    benchmark: str | None,
    cash_balance: float,
    transactions: List[Dict[str, Any]] | None,
    cash_adjustments: List[Dict[str, Any]] | None,
) -> None:
    try:
        snapshot = build_portfolio_snapshot(
            holdings,
            targets,
            benchmark,
            cash_balance,
            transactions=transactions,
            cash_adjustments=cash_adjustments,
        )
        with connect(db_path) as conn:
            ensure_snapshot_cache_table(conn)
            write_cached_snapshot(conn, cache_key, fingerprint, benchmark, snapshot)
    finally:
        with _refresh_lock:
            existing = _refresh_threads.get(cache_key)
            if existing and existing is threading.current_thread():
                _refresh_threads.pop(cache_key, None)


def _schedule_refresh_if_needed(
    cache_key: str,
    fingerprint: str,
    db_path: str,
    holdings: List[Dict[str, Any]],
    targets: Dict[str, Any] | None,
    benchmark: str | None,
    cash_balance: float,
    transactions: List[Dict[str, Any]] | None,
    cash_adjustments: List[Dict[str, Any]] | None,
    existing_timestamp: Optional[str],
) -> None:
    if not _should_refresh(existing_timestamp):
        return
    with _refresh_lock:
        existing = _refresh_threads.get(cache_key)
        if existing and existing.is_alive():
            return
        thread = threading.Thread(
            target=_refresh_worker,
            args=(
                cache_key,
                fingerprint,
                db_path,
                holdings,
                targets,
                benchmark,
                cash_balance,
                transactions,
                cash_adjustments,
            ),
            daemon=True,
        )
        _refresh_threads[cache_key] = thread
        thread.start()


def get_portfolio_snapshot(
    db_path: str,
    holdings: List[Dict[str, Any]],
    targets: Dict[str, Any] | None,
    benchmark: str | None,
    cash_balance: float = 0.0,
    transactions: List[Dict[str, Any]] | None = None,
    cash_adjustments: List[Dict[str, Any]] | None = None,
    *,
    refresh_async: bool = True,
    force_recompute: bool = False,
) -> Dict[str, Any]:
    """Return a portfolio snapshot using SQLite-backed caching.

    When ``force_recompute`` is ``True`` the snapshot is recomputed immediately
    and the cache is updated synchronously. Otherwise the cache is consulted
    first; if data exists it is returned immediately and a background refresh is
    scheduled when the data is considered stale.
    """

    cache_key, fingerprint = _generate_cache_key(
        holdings, targets, benchmark, cash_balance, transactions, cash_adjustments
    )
    cached_snapshot: Optional[Dict[str, Any]] = None
    cached_generated_at: Optional[str] = None

    if not force_recompute:
        with connect(db_path) as conn:
            ensure_snapshot_cache_table(conn)
            cached_row = read_cached_snapshot(conn, cache_key)
            if cached_row:
                cached_snapshot = cached_row["snapshot"]
                cached_generated_at = cached_row["generated_at"]

    if cached_snapshot is None:
        snapshot = build_portfolio_snapshot(
            holdings,
            targets,
            benchmark,
            cash_balance,
            transactions=transactions,
            cash_adjustments=cash_adjustments,
        )
        cached_snapshot = snapshot
        cached_generated_at = snapshot.get("generated_at")
        with connect(db_path) as conn:
            ensure_snapshot_cache_table(conn)
            write_cached_snapshot(conn, cache_key, fingerprint, benchmark, snapshot)
    elif refresh_async:
        _schedule_refresh_if_needed(
            cache_key,
            fingerprint,
            db_path,
            holdings,
            targets,
            benchmark,
            cash_balance,
            transactions,
            cash_adjustments,
            cached_generated_at,
        )

    if force_recompute and refresh_async:
        # After the synchronous recompute we may still want to queue another
        # refresh in the future if the result becomes stale.
        _schedule_refresh_if_needed(
            cache_key,
            fingerprint,
            db_path,
            holdings,
            targets,
            benchmark,
            cash_balance,
            transactions,
            cash_adjustments,
            cached_generated_at,
        )

    return cached_snapshot
