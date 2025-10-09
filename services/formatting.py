"""Formatting helpers for DashFolio templates."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict


def format_currency_value(value: Any, currency_context: Dict[str, Any]) -> str:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        numeric_value = 0.0

    converted = numeric_value * currency_context.get("rate", 1.0)
    symbol = currency_context.get("symbol", "$")
    decimals = currency_context.get("decimals", 2)
    formatted = f"{converted:,.{decimals}f}"
    return (
        f"{symbol}{formatted}"
        if currency_context.get("symbol_first", True)
        else f"{formatted}{symbol}"
    )


def format_signed_currency_value(value: Any, currency_context: Dict[str, Any]) -> str:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        numeric_value = 0.0

    prefix = "+" if numeric_value > 0 else ("-" if numeric_value < 0 else "")
    absolute = abs(numeric_value)
    formatted = format_currency_value(absolute, currency_context)
    if prefix:
        return f"{prefix}{formatted}"
    return formatted


def format_snapshot_update(timestamp: Any) -> str:
    try:
        if not timestamp:
            raise ValueError("missing timestamp")
        if isinstance(timestamp, datetime):
            parsed = timestamp.astimezone(timezone.utc)
        else:
            parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
    except Exception:
        return "Updated: Recently"

    now = datetime.now(timezone.utc)
    delta = now - parsed

    if delta.total_seconds() < 60:
        return "Updated: Recently"

    if delta.total_seconds() < 3600:
        minutes = max(int(delta.total_seconds() // 60), 1)
        return f"Updated: {minutes} min ago"

    if delta.total_seconds() < 86400:
        hours = max(int(delta.total_seconds() // 3600), 1)
        suffix = "hour" if hours == 1 else "hours"
        return f"Updated: {hours} {suffix} ago"

    utc_plus_three = timezone(timedelta(hours=3))
    localized = parsed.astimezone(utc_plus_three)
    formatted = localized.strftime("%Y-%m-%d %H:%M:%S")
    return f"Updated: {formatted} UTC+3"
