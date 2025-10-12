"""Centralised filesystem paths for the DashFolio application."""
from __future__ import annotations

import os
import sys
from typing import Final

BASE_DIR: Final[str] = os.path.dirname(os.path.abspath(__file__))

_CONFIG_ROOT = os.environ.get("DASHFOLIO_CONFIG_DIR")
if _CONFIG_ROOT:
    CONFIG_DIR: Final[str] = os.path.abspath(_CONFIG_ROOT)
else:
    CONFIG_DIR = BASE_DIR

CONFIG_FILE: Final[str] = os.path.join(CONFIG_DIR, "config.json")
PORTFOLIO_FILE: Final[str] = os.path.join(CONFIG_DIR, "portfolio.json")
DATA_STORE: Final[str] = os.path.join(CONFIG_DIR, "dashfolio.db")
MAIN_SCRIPT: Final[str] = os.path.join(BASE_DIR, "main.py")
ASSETS_DIR: Final[str] = os.path.join(BASE_DIR, "assets")

_PYTHON_OVERRIDE = os.environ.get("DASHFOLIO_PYTHON_EXECUTABLE")
if _PYTHON_OVERRIDE:
    PYTHON_EXECUTABLE: Final[str] = _PYTHON_OVERRIDE
else:
    _CANDIDATES = [
        os.path.join(BASE_DIR, "venv", "Scripts", "python.exe"),
        os.path.join(BASE_DIR, "venv", "bin", "python"),
    ]
    PYTHON_EXECUTABLE = next((path for path in _CANDIDATES if os.path.exists(path)), sys.executable)

__all__ = [
    "ASSETS_DIR",
    "BASE_DIR",
    "CONFIG_DIR",
    "CONFIG_FILE",
    "DATA_STORE",
    "MAIN_SCRIPT",
    "PORTFOLIO_FILE",
    "PYTHON_EXECUTABLE",
]
