"""In-memory activity log for application events."""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Deque, List

_LOG_CAPACITY = 500
_activity_log: Deque[str] = deque(maxlen=_LOG_CAPACITY)


def _timestamp() -> str:
    """Return a formatted UTC timestamp."""

    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S UTC")


def append_log(message: str) -> None:
    """Append ``message`` to the activity log with a timestamp."""

    if not message:
        return
    entry = f"{_timestamp()} · {message.strip()}"
    _activity_log.append(entry)


def get_log_entries() -> List[str]:
    """Return a snapshot list of activity log entries."""

    return list(_activity_log)


def clear_log() -> None:
    """Remove all entries from the activity log."""

    _activity_log.clear()
