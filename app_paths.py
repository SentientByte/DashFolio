"""Centralized filesystem paths for the DashFolio application."""
from __future__ import annotations

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_DATA_ROOT = "/mnt/config/dashfolio"
DATA_ROOT = os.environ.get("DASHFOLIO_DATA_DIR", DEFAULT_DATA_ROOT)
if not os.path.isabs(DATA_ROOT):
    DATA_ROOT = os.path.join(BASE_DIR, DATA_ROOT)
DATA_ROOT = os.path.abspath(DATA_ROOT)


def ensure_data_root() -> str:
    """Ensure the externalised data directory exists."""

    os.makedirs(DATA_ROOT, exist_ok=True)
    return DATA_ROOT


ensure_data_root()

# Prefer venv python if available, otherwise use current interpreter
VENV_PYTHON = os.path.join(BASE_DIR, "venv", "Scripts", "python.exe")
if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable

MAIN_SCRIPT = os.path.join(BASE_DIR, "main.py")
CONFIG_FILE = os.path.join(DATA_ROOT, "config.json")
PORTFOLIO_FILE = os.path.join(DATA_ROOT, "portfolio.json")
DATA_STORE = os.path.join(DATA_ROOT, "dashfolio.db")
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
