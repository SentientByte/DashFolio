"""Configuration helpers for DashFolio."""
from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Final, MutableMapping, Optional, TypedDict

import requests

from app_paths import CONFIG_DIR, CONFIG_FILE

SESSION_DURATION_CHOICES: Final[list[int]] = [0, 4, 12, 24, 48]
DEFAULT_SESSION_DURATION: Final[int] = 12
USD_TO_BHD: Final[float] = 0.376081

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
    "CURRENCY_RATE_OVERRIDES": {},
}


class CurrencyContext(TypedDict):
    """Structure returned to templates describing the active currency."""

    code: str
    symbol: str
    rate: float
    symbol_first: bool
    source: str


class SessionPreferences(TypedDict):
    """Resolved session duration values for the Flask layer."""

    lifetime_hours: int
    permanent: bool


_CURRENCY_FALLBACKS: Final[Dict[str, CurrencyContext]] = {
    "USD": {"code": "USD", "symbol": "$", "rate": 1.0, "symbol_first": True, "source": "default"},
    "BHD": {"code": "BHD", "symbol": "BD", "rate": USD_TO_BHD, "symbol_first": True, "source": "default"},
    "EUR": {"code": "EUR", "symbol": "€", "rate": 0.92, "symbol_first": True, "source": "fallback"},
    "GBP": {"code": "GBP", "symbol": "£", "rate": 0.79, "symbol_first": True, "source": "fallback"},
    "CAD": {"code": "CAD", "symbol": "$", "rate": 1.36, "symbol_first": True, "source": "fallback"},
}

_FX_CACHE: Dict[str, float] = {}


def _default_config_snapshot() -> Dict[str, Any]:
    """Return a copy of the built-in configuration defaults."""

    return deepcopy(_DEFAULT_CONFIG)


def _ensure_config_directory() -> None:
    """Guarantee that the configuration directory exists before writing files."""

    Path(CONFIG_DIR).mkdir(parents=True, exist_ok=True)


def ensure_default_config_file() -> None:
    """Create the default configuration file if it does not exist."""

    _ensure_config_directory()
    if os.path.exists(CONFIG_FILE):
        return
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as file:
            json.dump(_DEFAULT_CONFIG, file, indent=4)
    except OSError as exc:
        print(f"Warning: unable to write default config file '{CONFIG_FILE}'. {exc}")
    else:
        print(f"Created default config file: {CONFIG_FILE}")


def load_config() -> Dict[str, Any]:
    """Load the configuration file with sensible fallbacks."""

    ensure_default_config_file()
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            config: Dict[str, Any] = json.load(file)
    except (FileNotFoundError, PermissionError, json.JSONDecodeError) as exc:
        print(
            "Warning: falling back to in-memory defaults because config file "
            f"'{CONFIG_FILE}' could not be read. {exc}"
        )
        config = _default_config_snapshot()

    defaults = {
        "BENCHMARK_TICKER": "SPY",
        "CURRENCY": "USD",
        "AUTO_REFRESH_INTERVAL": 60,
        "SESSION_DURATION_HOURS": DEFAULT_SESSION_DURATION,
        "CURRENCY_RATE_OVERRIDES": {},
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
    elif interval > 3600:
        interval = 3600
        updated = True
    config["AUTO_REFRESH_INTERVAL"] = interval

    preferences = get_session_preferences(config)
    config["SESSION_DURATION_HOURS"] = preferences["lifetime_hours"]

    if updated:
        save_config(config)

    return config


def save_config(config: Dict[str, Any]) -> None:
    """Persist ``config`` back to disk, creating directories as needed."""

    _ensure_config_directory()
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as file:
            json.dump(config, file, indent=4)
    except OSError as exc:
        print(f"Warning: unable to persist configuration to '{CONFIG_FILE}'. {exc}")


def get_session_preferences(config: Dict[str, Any] | None = None) -> SessionPreferences:
    """Return the resolved session duration and permanence settings."""

    if config is None:
        config = load_config()

    try:
        session_duration = int(config.get("SESSION_DURATION_HOURS", DEFAULT_SESSION_DURATION))
    except (TypeError, ValueError):
        session_duration = DEFAULT_SESSION_DURATION

    if session_duration not in SESSION_DURATION_CHOICES:
        session_duration = DEFAULT_SESSION_DURATION

    return {
        "lifetime_hours": session_duration,
        "permanent": session_duration > 0,
    }


def _maybe_fetch_rate(target: str) -> Optional[float]:
    """Fetch a live USD exchange rate for ``target`` when enabled."""

    enable_flag = os.environ.get("DASHFOLIO_ENABLE_LIVE_FX", "0")
    if enable_flag.lower() not in {"1", "true", "yes"}:
        return None

    target_code = target.upper()
    if target_code in _FX_CACHE:
        return _FX_CACHE[target_code]

    template = os.environ.get("DASHFOLIO_FX_API_URL", "https://open.er-api.com/v6/latest/{base}")
    try:
        url = template.format(base="USD")
    except KeyError:
        url = "https://open.er-api.com/v6/latest/USD"

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
    except Exception:
        return None

    try:
        payload = response.json()
    except ValueError:
        return None

    rates: MutableMapping[str, Any] | None = payload.get("rates")  # type: ignore[assignment]
    if not isinstance(rates, dict):
        return None

    try:
        rate = float(rates[target_code])
    except (KeyError, TypeError, ValueError):
        return None

    _FX_CACHE[target_code] = rate
    return rate


def _resolve_currency(code: str, overrides: Dict[str, Any]) -> CurrencyContext:
    """Resolve a currency context for ``code`` honouring overrides and live FX."""

    upper_code = code.upper()
    override = overrides.get(upper_code)
    if isinstance(override, dict):
        try:
            rate_value = float(override.get("rate"))
            if rate_value <= 0:
                raise ValueError
        except (TypeError, ValueError):
            rate_value = None
        symbol = str(override.get("symbol", "")).strip() or "$"
        symbol_first = bool(override.get("symbol_first", True))
        if rate_value:
            return {
                "code": upper_code,
                "symbol": symbol,
                "rate": rate_value,
                "symbol_first": symbol_first,
                "source": "override",
            }

    fallback = _CURRENCY_FALLBACKS.get(upper_code)
    if fallback:
        return dict(fallback)

    live_rate = _maybe_fetch_rate(upper_code)
    if live_rate:
        return {
            "code": upper_code,
            "symbol": upper_code,
            "rate": live_rate,
            "symbol_first": False,
            "source": "live",
        }

    return dict(_CURRENCY_FALLBACKS["USD"])


def get_currency_context(config: Dict[str, Any] | None = None) -> CurrencyContext:
    """Resolve currency display metadata for templates and formatters."""

    if config is None:
        config = load_config()

    currency_code = str(config.get("CURRENCY", "USD") or "USD").upper()
    overrides = config.get("CURRENCY_RATE_OVERRIDES")
    if not isinstance(overrides, dict):
        overrides = {}

    context = _resolve_currency(currency_code, overrides)
    rate = context["rate"]
    if not isinstance(rate, (float, int)) or rate <= 0:
        context = dict(_CURRENCY_FALLBACKS["USD"])

    return context
