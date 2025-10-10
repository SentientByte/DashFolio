"""Utilities for determining U.S. equity market trading hours."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Dict, Optional

from zoneinfo import ZoneInfo


EASTERN_TZ = ZoneInfo("America/New_York")

MARKET_OPEN_TIME = time(9, 30)
MARKET_CLOSE_TIME = time(16, 0)


def _observed_holiday(holiday: date) -> date:
    """Return the observed date for a holiday that falls on a weekend."""

    if holiday.weekday() == 5:  # Saturday -> previous Friday
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:  # Sunday -> following Monday
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    """Return the date for the n-th weekday of a month."""

    current = date(year, month, 1)
    count = 0
    while True:
        if current.weekday() == weekday:
            count += 1
            if count == occurrence:
                return current
        current += timedelta(days=1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return the date for the last given weekday within a month."""

    current = date(year, month, 1)
    next_month = month + 1
    next_year = year
    if next_month == 13:
        next_month = 1
        next_year += 1
    current = date(next_year, next_month, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _calculate_easter(year: int) -> date:
    """Return the Gregorian Easter Sunday for ``year``."""

    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def get_us_market_holidays(year: int) -> set[date]:
    """Return the set of full-day NASDAQ holiday observances for ``year``."""

    holidays: set[date] = set()

    # New Year's Day (observed)
    holidays.add(_observed_holiday(date(year, 1, 1)))

    # Martin Luther King Jr. Day (third Monday of January)
    holidays.add(_nth_weekday(year, 1, weekday=0, occurrence=3))

    # Presidents' Day / Washington's Birthday (third Monday of February)
    holidays.add(_nth_weekday(year, 2, weekday=0, occurrence=3))

    # Good Friday (two days before Easter Sunday)
    holidays.add(_calculate_easter(year) - timedelta(days=2))

    # Memorial Day (last Monday of May)
    holidays.add(_last_weekday(year, 5, weekday=0))

    # Juneteenth National Independence Day (observed)
    holidays.add(_observed_holiday(date(year, 6, 19)))

    # Independence Day (observed)
    holidays.add(_observed_holiday(date(year, 7, 4)))

    # Labor Day (first Monday of September)
    holidays.add(_nth_weekday(year, 9, weekday=0, occurrence=1))

    # Thanksgiving Day (fourth Thursday of November)
    holidays.add(_nth_weekday(year, 11, weekday=3, occurrence=4))

    # Christmas Day (observed)
    holidays.add(_observed_holiday(date(year, 12, 25)))

    return holidays


def _build_holiday_cache(year: int) -> set[date]:
    """Return holidays for ``year`` plus spillover observances nearby."""

    combined: set[date] = set()
    for target_year in (year - 1, year, year + 1):
        combined.update(get_us_market_holidays(target_year))
    return combined


def _is_trading_day(check_date: date, holidays: set[date]) -> bool:
    return check_date.weekday() < 5 and check_date not in holidays


def get_next_open_datetime(now: Optional[datetime] = None) -> datetime:
    """Return the next time the market opens at or after ``now``."""

    current = now.astimezone(EASTERN_TZ) if now else datetime.now(EASTERN_TZ)
    holidays = _build_holiday_cache(current.year)
    today = current.date()
    open_dt_today = datetime.combine(today, MARKET_OPEN_TIME, tzinfo=EASTERN_TZ)

    if _is_trading_day(today, holidays) and current < open_dt_today:
        return open_dt_today

    probe = current
    if probe.time() >= MARKET_CLOSE_TIME:
        probe = datetime.combine(today + timedelta(days=1), time(0, 0), tzinfo=EASTERN_TZ)

    while True:
        candidate_date = probe.date()
        if _is_trading_day(candidate_date, holidays):
            open_dt = datetime.combine(candidate_date, MARKET_OPEN_TIME, tzinfo=EASTERN_TZ)
            if open_dt >= current:
                return open_dt
        probe += timedelta(days=1)


def get_market_status(now: Optional[datetime] = None) -> Dict[str, Optional[str]]:
    """Return a dictionary describing the NASDAQ market status."""

    current = now.astimezone(EASTERN_TZ) if now else datetime.now(EASTERN_TZ)
    holidays = _build_holiday_cache(current.year)
    today = current.date()

    is_weekday = current.weekday() < 5
    is_holiday = today in holidays
    is_open_time = MARKET_OPEN_TIME <= current.time() < MARKET_CLOSE_TIME

    is_open = is_weekday and not is_holiday and is_open_time

    if not is_weekday:
        reason = "Weekend"
    elif is_holiday:
        reason = "Market holiday"
    elif current.time() < MARKET_OPEN_TIME:
        reason = None
    elif current.time() >= MARKET_CLOSE_TIME:
        reason = "After hours"
    else:
        reason = None

    next_open = get_next_open_datetime(current)

    status: Dict[str, Optional[str]] = {
        "is_open": is_open,
        "reason": reason,
        "as_of": current.isoformat(),
        "next_open": next_open.isoformat() if next_open else None,
    }

    status["label"] = "Market open" if is_open else "Market closed"

    return status

