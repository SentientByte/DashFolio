"""Configuration utilities for the DashFolio calculations."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Tuple

DEFAULT_CONFIG = {
    "DATA_PERIOD": "1y",
    "CUSTOM_START_DATE": "2024-01-01",
    "STOP_LOSS_PERCENTAGE_RANGE": (1, 2),
    "STOP_LOSS_STEP": 0.2,
    "NUM_SIMULATIONS": 10000,
    "CONFIDENCE_LEVEL": 0.95,
    "SPAN_EWMA": 60,
}


def load_config(config_path: str) -> Dict:
    """Load configuration data from ``config_path``."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Missing config.json at {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_config(raw_config: Dict) -> Dict:
    """Normalize config values with defaults and expected types."""
    config = dict(DEFAULT_CONFIG)
    config.update(raw_config or {})

    normalized = {
        "DATA_PERIOD": str(config.get("DATA_PERIOD", DEFAULT_CONFIG["DATA_PERIOD"])).strip(),
        "CUSTOM_START_DATE": config.get("CUSTOM_START_DATE", DEFAULT_CONFIG["CUSTOM_START_DATE"]),
        "STOP_LOSS_STEP": float(config.get("STOP_LOSS_STEP", DEFAULT_CONFIG["STOP_LOSS_STEP"])),
        "NUM_SIMULATIONS": int(config.get("NUM_SIMULATIONS", DEFAULT_CONFIG["NUM_SIMULATIONS"])),
        "CONFIDENCE_LEVEL": float(config.get("CONFIDENCE_LEVEL", DEFAULT_CONFIG["CONFIDENCE_LEVEL"])),
        "SPAN_EWMA": int(config.get("SPAN_EWMA", DEFAULT_CONFIG["SPAN_EWMA"])),
    }

    stop_range = config.get("STOP_LOSS_PERCENTAGE_RANGE", DEFAULT_CONFIG["STOP_LOSS_PERCENTAGE_RANGE"])
    try:
        if isinstance(stop_range, (list, tuple)) and len(stop_range) >= 2:
            stop_min = float(stop_range[0])
            stop_max = float(stop_range[1])
        else:
            raise ValueError
    except Exception:
        stop_min, stop_max = DEFAULT_CONFIG["STOP_LOSS_PERCENTAGE_RANGE"]
    normalized["STOP_LOSS_PERCENTAGE_RANGE"] = (stop_min, stop_max)

    return normalized


def parse_data_period(period_str: str, today: datetime) -> Tuple[datetime, str]:
    """Parse ``period_str`` into a start date and description."""
    s = str(period_str).strip().lower()

    if s == "" or s == "1y":
        return today - timedelta(days=365), "1 year (default)"
    if s == "ytd":
        return datetime(today.year, 1, 1), "YTD"
    if s == "custom":
        return None, "custom"

    match_months = re.match(r"^(\d+)\s*(m|mo|month|months)$", s)
    if match_months:
        months = int(match_months.group(1))
        days = int(months * 30)
        return today - timedelta(days=days), f"{months} months"

    match_years = re.match(r"^(\d+(\.\d+)?)\s*(y|yr|year|years)$", s)
    if match_years:
        years = float(match_years.group(1))
        days = int(years * 365)
        return today - timedelta(days=days), f"{years} years"

    match_numeric = re.match(r"^(\d+(\.\d+)?)$", s)
    if match_numeric:
        years = float(match_numeric.group(1))
        days = int(years * 365)
        return today - timedelta(days=days), f"{years} years (numeric)"

    return today - timedelta(days=365), "fallback 1 year"


def determine_start_date(data_period: str, custom_start_date: str, today: datetime) -> Tuple[datetime, str]:
    """Determine the date range based on period and custom date."""
    parsed_start, reason = parse_data_period(data_period, today)
    if parsed_start is None and data_period.lower() == "custom":
        try:
            parsed_start = datetime.strptime(custom_start_date, "%Y-%m-%d")
            reason = f"custom {custom_start_date}"
        except Exception as exc:
            print(
                f"Invalid CUSTOM_START_DATE '{custom_start_date}', "
                f"falling back to 1y. Error: {exc}"
            )
            parsed_start = today - timedelta(days=365)
            reason = "fallback 1 year (bad custom)"

    return parsed_start, reason
