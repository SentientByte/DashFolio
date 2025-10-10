"""Centralized filesystem paths for the DashFolio application."""
from __future__ import annotations

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Prefer venv python if available, otherwise use current interpreter
VENV_PYTHON = os.path.join(BASE_DIR, "venv", "Scripts", "python.exe")
if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable

MAIN_SCRIPT = os.path.join(BASE_DIR, "main.py")
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
PORTFOLIO_FILE = os.path.join(BASE_DIR, "portfolio.json")
DATA_STORE = os.path.join(BASE_DIR, "dashfolio.db")
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
