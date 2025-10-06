"""Helpers for working with target allocations."""

from __future__ import annotations

from typing import Any, Dict, List

from .utils import safe_float


def normalize_target_allocations(
    holdings: List[Dict[str, Any]],
    targets: Dict[str, Any] | None,
) -> Dict[str, float]:
    """Normalise target weights to percentages totalling 100."""

    normalized: Dict[str, float] = {}
    targets = targets or {}

    for holding in holdings:
        ticker = str(holding.get("ticker", "")).upper()
        if not ticker:
            continue
        target_value = safe_float(targets.get(ticker))
        if target_value < 0:
            target_value = 0.0
        normalized[ticker] = target_value

    if not normalized:
        return {}

    total = sum(normalized.values())
    if total <= 0:
        even_weight = 100.0 / len(normalized)
        for ticker in normalized:
            normalized[ticker] = even_weight
        return normalized

    scale = 100.0 / total
    for ticker in normalized:
        normalized[ticker] *= scale

    return normalized
