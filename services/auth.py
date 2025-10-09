"""Authentication helpers for DashFolio."""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Optional

from flask import Flask, session

from Calculations.storage import (
    connect,
    ensure_user_table,
    read_single_user,
    update_user_last_login,
    update_user_onboarding_status,
)

from services.configuration import DEFAULT_SESSION_DURATION, load_config


def load_user_record(data_store: str) -> Optional[Dict[str, Any]]:
    with connect(data_store) as conn:
        ensure_user_table(conn)
        user = read_single_user(conn)
    return user


def login_user_session(app: Flask, data_store: str, user: Dict[str, Any]) -> None:
    session.clear()
    session["user_id"] = user.get("id")
    config = load_config()
    try:
        duration = int(config.get("SESSION_DURATION_HOURS", DEFAULT_SESSION_DURATION))
    except (TypeError, ValueError):
        duration = DEFAULT_SESSION_DURATION

    if duration > 0:
        app.permanent_session_lifetime = timedelta(hours=duration)
        session.permanent = True
    else:
        session.permanent = False

    with connect(data_store) as conn:
        ensure_user_table(conn)
        update_user_last_login(conn, int(user.get("id", 1)))


def complete_onboarding(data_store: str) -> None:
    with connect(data_store) as conn:
        ensure_user_table(conn)
        update_user_onboarding_status(conn, True)
