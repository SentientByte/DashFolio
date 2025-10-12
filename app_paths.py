"""Centralized filesystem paths for the DashFolio application."""
from __future__ import annotations

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_DATA_ROOT = "/mnt/config/dashfolio"


def _resolve_data_root() -> str:
    """Return the directory that should store user-modifiable data."""

    candidate = os.environ.get("DASHFOLIO_DATA_DIR", DEFAULT_DATA_ROOT)
    if not os.path.isabs(candidate):
        candidate = os.path.join(BASE_DIR, candidate)
    candidate = os.path.abspath(candidate)

    if "DASHFOLIO_DATA_DIR" in os.environ:
        os.makedirs(candidate, exist_ok=True)
        return candidate

    try:
        os.makedirs(candidate, exist_ok=True)
    except OSError:
        fallback = os.path.join(BASE_DIR, "data")
        os.makedirs(fallback, exist_ok=True)
        return fallback

    return candidate


DATA_ROOT = _resolve_data_root()


def ensure_data_root() -> str:
    """Ensure the externalised data directory exists."""

    os.makedirs(DATA_ROOT, exist_ok=True)
    return DATA_ROOT

# Prefer venv python if available, otherwise use current interpreter
VENV_PYTHON = os.path.join(BASE_DIR, "venv", "Scripts", "python.exe")
if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable

MAIN_SCRIPT = os.path.join(BASE_DIR, "main.py")
CONFIG_FILE = os.path.join(DATA_ROOT, "config.json")
PORTFOLIO_FILE = os.path.join(DATA_ROOT, "portfolio.json")
DATA_STORE = os.path.join(DATA_ROOT, "dashfolio.db")
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
