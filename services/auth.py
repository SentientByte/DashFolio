"""Authentication helpers for DashFolio."""
from __future__ import annotations

from typing import Any, Dict, Optional, TypedDict

from Calculations.storage import (
    connect,
    ensure_user_table,
    read_single_user,
    update_user_last_login,
    update_user_onboarding_status,
)

from services.configuration import get_session_preferences, load_config


class LoginSessionState(TypedDict):
    """Session metadata needed by the Flask layer during login."""

    user_id: int
    permanent: bool
    lifetime_hours: int


def load_user_record(data_store: str) -> Optional[Dict[str, Any]]:
    """Fetch the single-user record from SQLite, if present."""

    with connect(data_store) as conn:
        ensure_user_table(conn)
        user = read_single_user(conn)
    return user


def prepare_login_session(user: Dict[str, Any], config: Dict[str, Any] | None = None) -> LoginSessionState:
    """Build session preferences for ``user`` without touching Flask globals."""

    if config is None:
        config = load_config()

    preferences = get_session_preferences(config)
    user_id = int(user.get("id", 0))
    return {
        "user_id": user_id,
        "permanent": preferences["permanent"],
        "lifetime_hours": preferences["lifetime_hours"],
    }


def record_successful_login(data_store: str, user_id: int) -> None:
    """Persist the last-login timestamp for ``user_id``."""

    with connect(data_store) as conn:
        ensure_user_table(conn)
        update_user_last_login(conn, user_id)


def complete_onboarding(data_store: str) -> None:
    """Mark the onboarding flag as complete for the single user."""

    with connect(data_store) as conn:
        ensure_user_table(conn)
        update_user_onboarding_status(conn, True)
