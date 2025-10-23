"""Maintenance utilities for DashFolio administrative tasks."""

from __future__ import annotations

import os
from typing import Iterable, List

from app_paths import CONFIG_FILE, PORTFOLIO_FILE
from services.configuration import ensure_default_config_file
from services.portfolio import ensure_default_portfolio_file


def _iter_database_files(db_path: str) -> Iterable[str]:
    """Yield the SQLite database file and its ancillary sidecar files."""

    yield db_path
    for suffix in ("-wal", "-shm"):
        candidate = f"{db_path}{suffix}"
        if os.path.exists(candidate):
            yield candidate


def calculate_database_size_mb(db_path: str) -> float:
    """Return the combined size of the SQLite database files in megabytes."""

    total_bytes = 0
    for path in _iter_database_files(db_path):
        try:
            total_bytes += os.path.getsize(path)
        except OSError:
            continue
    return total_bytes / (1024 * 1024) if total_bytes else 0.0


def reset_application_state(db_path: str) -> None:
    """Remove persisted data files and recreate default configuration."""

    failures: List[str] = []
    for path in _iter_database_files(db_path):
        try:
            os.remove(path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            failures.append(f"{path}: {exc}")

    for path in (CONFIG_FILE, PORTFOLIO_FILE):
        try:
            os.remove(path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            failures.append(f"{path}: {exc}")

    ensure_default_config_file()
    ensure_default_portfolio_file()

    if failures:
        joined = "; ".join(failures)
        raise RuntimeError(f"Unable to reset application state: {joined}")
