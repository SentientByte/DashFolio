"""Configuration helpers for DashFolio."""
from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any, Dict

from flask import Flask

from app_paths import CONFIG_FILE

SESSION_DURATION_CHOICES = [0, 4, 12, 24, 48]
DEFAULT_SESSION_DURATION = 12
USD_TO_BHD = 0.376081

_DEFAULT_CONFIG: Dict[str, Any] = {
    "DATA_PERIOD": "1y",
    "CUSTOM_START_DATE": "2024-01-01",
    "STOP_LOSS_PERCENTAGE_RANGE": [1, 2],
    "STOP_LOSS_STEP": 0.2,
    "NUM_SIMULATIONS": 10000,
    "CONFIDENCE_LEVEL": 0.95,
    "SPAN_EWMA": 60,
    "BENCHMARK_TICKER": "SPY",
    "CURRENCY": "USD",
    "AUTO_REFRESH_INTERVAL": 60,
    "SESSION_DURATION_HOURS": DEFAULT_SESSION_DURATION,
    "NOTIFICATIONS_ENABLED": False,
    "TELEGRAM_BOT_TOKEN": "",
    "TELEGRAM_CHAT_ID": "",
    "NOTIFY_END_OF_DAY": False,
    "NOTIFY_BEGINNING_OF_DAY": False,
}


def ensure_default_config_file() -> None:
    """Create the default configuration file if it does not exist."""
    directory = os.path.dirname(CONFIG_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    if os.path.exists(CONFIG_FILE):
        return
    with open(CONFIG_FILE, "w", encoding="utf-8") as file:
        json.dump(_DEFAULT_CONFIG, file, indent=4)
    print(f"Created default config file: {CONFIG_FILE}")


def load_config() -> Dict[str, Any]:
    """Load the configuration file with sensible fallbacks."""
    ensure_default_config_file()
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        config: Dict[str, Any] = json.load(file)

    defaults = {
        "BENCHMARK_TICKER": "SPY",
        "CURRENCY": "USD",
        "AUTO_REFRESH_INTERVAL": 60,
        "SESSION_DURATION_HOURS": DEFAULT_SESSION_DURATION,
        "NOTIFICATIONS_ENABLED": False,
        "TELEGRAM_BOT_TOKEN": "",
        "TELEGRAM_CHAT_ID": "",
        "NOTIFY_END_OF_DAY": False,
        "NOTIFY_BEGINNING_OF_DAY": False,
    }
    updated = False
    for key, value in defaults.items():
        if key not in config:
            config[key] = value
            updated = True

    try:
        interval = int(config.get("AUTO_REFRESH_INTERVAL", 60))
    except (TypeError, ValueError):
        interval = 60
    if interval < 1:
        interval = 1
        updated = True
    elif interval > 60:
        interval = 60
        updated = True
    config["AUTO_REFRESH_INTERVAL"] = interval

    try:
        session_duration = int(config.get("SESSION_DURATION_HOURS", DEFAULT_SESSION_DURATION))
    except (TypeError, ValueError):
        session_duration = DEFAULT_SESSION_DURATION
    if session_duration not in SESSION_DURATION_CHOICES:
        session_duration = DEFAULT_SESSION_DURATION
        updated = True
    config["SESSION_DURATION_HOURS"] = session_duration

    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"", "0", "false", "no", "off"}:
                return False
            if normalized in {"1", "true", "yes", "on"}:
                return True
        return bool(value)

    bool_keys = [
        "NOTIFICATIONS_ENABLED",
        "NOTIFY_END_OF_DAY",
        "NOTIFY_BEGINNING_OF_DAY",
    ]
    for key in bool_keys:
        original = config.get(key)
        coerced = _coerce_bool(original)
        if original != coerced:
            config[key] = coerced
            updated = True

    for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        value = config.get(key, "")
        trimmed = str(value or "").strip()
        if config.get(key) != trimmed:
            config[key] = trimmed
            updated = True

    if updated:
        save_config(config)

    return config


def save_config(config: Dict[str, Any]) -> None:
    directory = os.path.dirname(CONFIG_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as file:
        json.dump(config, file, indent=4)


def apply_session_duration(app: Flask, config: Dict[str, Any]) -> None:
    """Ensure the Flask session lifetime matches the stored preference."""
    try:
        duration = int(config.get("SESSION_DURATION_HOURS", DEFAULT_SESSION_DURATION))
    except (TypeError, ValueError):
        duration = DEFAULT_SESSION_DURATION

    if duration > 0:
        app.permanent_session_lifetime = timedelta(hours=duration)
    else:
        app.permanent_session_lifetime = timedelta(hours=DEFAULT_SESSION_DURATION)


def get_currency_context(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if config is None:
        config = load_config()

    currency = str(config.get("CURRENCY", "USD")).upper()
    if currency not in {"USD", "BHD"}:
        currency = "USD"

    rate = USD_TO_BHD if currency == "BHD" else 1.0
    symbol = "BD" if currency == "BHD" else "$"
    return {
        "code": currency,
        "symbol": symbol,
        "rate": rate,
        "symbol_first": True,
    }
