from __future__ import annotations

"""Telegram notification scheduling helpers."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
import math
import threading
from typing import Any, Dict, List, Optional
from urllib import request

from Calculations.snapshot_cache import get_portfolio_snapshot
from Calculations.utils import safe_float
from services.activity_log import append_log
from services.configuration import get_currency_context
from services.formatting import (
    format_currency_value,
    format_signed_currency_value,
)
from services.market_hours import EASTERN_TZ, MARKET_CLOSE_TIME, MARKET_OPEN_TIME, is_trading_day
from services.portfolio import load_portfolio_state


@dataclass
class NotificationSettings:
    """Parsed notification preferences from ``config.json``."""

    enabled: bool
    bot_token: str
    chat_id: str
    notify_end_of_day: bool
    notify_beginning_of_day: bool

    @property
    def has_credentials(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "NotificationSettings":
        return cls(
            enabled=bool(config.get("NOTIFICATIONS_ENABLED")),
            bot_token=str(config.get("TELEGRAM_BOT_TOKEN", "") or "").strip(),
            chat_id=str(config.get("TELEGRAM_CHAT_ID", "") or "").strip(),
            notify_end_of_day=bool(config.get("NOTIFY_END_OF_DAY")),
            notify_beginning_of_day=bool(config.get("NOTIFY_BEGINNING_OF_DAY")),
        )


class NotificationError(Exception):
    """Raised when a Telegram notification cannot be delivered."""


def send_telegram_message(bot_token: str, chat_id: str, message: str) -> None:
    """Send ``message`` to ``chat_id`` using the provided ``bot_token``."""

    token = (bot_token or "").strip()
    chat = (chat_id or "").strip()
    if not token or not chat:
        raise NotificationError("Telegram bot token and chat ID are required.")

    payload = json.dumps({"chat_id": chat, "text": message}).encode("utf-8")
    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    req = request.Request(endpoint, data=payload, headers={"Content-Type": "application/json"})
    try:
        with request.urlopen(req, timeout=15) as response:
            response.read()
    except Exception as exc:  # pragma: no cover - network failures
        raise NotificationError(f"Telegram API request failed: {exc}") from exc


class NotificationScheduler:
    """Background coordinator for Telegram alerts."""

    def __init__(self, data_store: str) -> None:
        self._data_store = data_store
        self._settings = NotificationSettings(False, "", "", False, False)
        self._config: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_end_of_day: Optional[date] = None
        self._last_beginning_of_day: Optional[date] = None

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="NotificationScheduler",
                daemon=True,
            )
            self._thread.start()

    def shutdown(self) -> None:
        with self._lock:
            self._stop_event.set()
            self._wake_event.set()
            thread = self._thread
        if thread:
            thread.join(timeout=2.0)

    def update(self, config: Dict[str, Any]) -> None:
        settings = NotificationSettings.from_config(config)
        with self._lock:
            self._settings = settings
            self._config = dict(config)
            self._wake_event.set()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._wake_event.wait(timeout=60.0)
            self._wake_event.clear()
            if self._stop_event.is_set():
                break
            try:
                self._process_events()
            except Exception as exc:  # pragma: no cover - defensive logging
                append_log(f"Notification scheduler error: {exc}")

    def _process_events(self) -> None:
        with self._lock:
            settings = self._settings
            config = dict(self._config)
            last_eod = self._last_end_of_day
            last_bod = self._last_beginning_of_day

        if not settings.enabled or not settings.has_credentials:
            return
        if not (settings.notify_end_of_day or settings.notify_beginning_of_day):
            return

        now = datetime.now(EASTERN_TZ)
        trading_day = now.date()

        if settings.notify_end_of_day and is_trading_day(trading_day):
            close_time = datetime.combine(trading_day, MARKET_CLOSE_TIME, tzinfo=EASTERN_TZ)
            if now >= close_time and (last_eod is None or last_eod != trading_day):
                if self._send_notification("end_of_day", now, config, settings):
                    with self._lock:
                        self._last_end_of_day = trading_day

        if settings.notify_beginning_of_day and is_trading_day(trading_day):
            open_time = datetime.combine(trading_day, MARKET_OPEN_TIME, tzinfo=EASTERN_TZ)
            begin_window = open_time + timedelta(minutes=15)
            if now >= begin_window and (last_bod is None or last_bod != trading_day):
                if self._send_notification("beginning_of_day", now, config, settings):
                    with self._lock:
                        self._last_beginning_of_day = trading_day

    def _send_notification(
        self,
        mode: str,
        event_time: datetime,
        config: Dict[str, Any],
        settings: NotificationSettings,
    ) -> bool:
        snapshot = self._load_snapshot(config)
        if not snapshot:
            append_log("Notification skipped: no portfolio snapshot available.")
            return False

        currency_context = get_currency_context(config)
        benchmark = str(config.get("BENCHMARK_TICKER", "SPY") or "SPY").upper()

        if mode == "end_of_day":
            message = build_end_of_day_message(snapshot, currency_context, benchmark, event_time)
            label = "end-of-day"
        else:
            message = build_beginning_of_day_message(
                snapshot,
                currency_context,
                benchmark,
                event_time,
            )
            label = "beginning-of-day"

        if not message:
            append_log(f"Notification skipped: unable to build {label} payload.")
            return False

        try:
            send_telegram_message(settings.bot_token, settings.chat_id, message)
        except NotificationError as exc:
            append_log(f"Telegram {label} notification failed: {exc}")
            return False

        append_log(f"Telegram {label} notification sent.")
        return True

    def _load_snapshot(self, config: Dict[str, Any]) -> Dict[str, Any]:
        state = load_portfolio_state(self._data_store)
        holdings = state.get("holdings", []) or []
        targets = state.get("target_allocations", {}) or {}
        cash_balance = safe_float(state.get("cash_balance"))
        transactions = state.get("transactions", []) or []
        cash_adjustments = state.get("cash_adjustments", []) or []
        metadata = state.get("metadata", []) or []
        benchmark = str(config.get("BENCHMARK_TICKER", "SPY") or "SPY").upper()

        try:
            return get_portfolio_snapshot(
                self._data_store,
                holdings,
                targets,
                benchmark,
                cash_balance,
                transactions=transactions,
                cash_adjustments=cash_adjustments,
                holdings_metadata=metadata,
                refresh_async=False,
                force_recompute=True,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            append_log(f"Failed to load snapshot for notification: {exc}")
            return {}


def _safe_number(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(numeric) or math.isinf(numeric):
        return 0.0
    return numeric


def _format_percent(value: Any) -> str:
    numeric = _safe_number(value)
    return f"{numeric:+.2f}"


def _format_positive_percent(value: Any) -> str:
    numeric = _safe_number(value)
    return f"{numeric:.2f}"


def _benchmark_daily_percent(performance_history: List[Dict[str, Any]]) -> float:
    for entry in reversed(performance_history or []):
        candidate = entry.get("benchmark_daily_return")
        if candidate is None:
            continue
        numeric = _safe_number(candidate) * 100.0
        if numeric or numeric == 0.0:
            return numeric
    return 0.0


def _extract_daily_changes(holdings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []
    for holding in holdings or []:
        raw_pct = holding.get("todays_gain_pct")
        if raw_pct is None:
            continue
        pct = _safe_number(raw_pct)
        value = _safe_number(holding.get("todays_gain"))
        ticker = str(holding.get("ticker") or "—").upper()
        changes.append({"ticker": ticker, "change_pct": pct, "change_value": value})
    return changes


def _holdings_summary_rows(
    holdings: List[Dict[str, Any]],
    currency_context: Dict[str, Any],
    limit: int = 5,
) -> List[str]:
    ordered = sorted(
        holdings or [],
        key=lambda item: _safe_number(item.get("current_value")),
        reverse=True,
    )
    rows: List[str] = []
    for holding in ordered[:limit]:
        ticker = str(holding.get("ticker") or "—").upper()
        value_str = format_currency_value(holding.get("current_value"), currency_context)
        pct_str = _format_percent(holding.get("todays_gain_pct")) + "%"
        change_str = format_signed_currency_value(holding.get("todays_gain"), currency_context)
        rows.append(f"{ticker}\t{value_str}\t{pct_str}\t{change_str}")
    if len(ordered) > limit:
        rows.append("…\t…\t…\t…")
    if not rows:
        rows.append("No holdings recorded.")
    return rows


def _average_percent(changes: List[Dict[str, Any]]) -> float:
    if not changes:
        return 0.0
    total = sum(entry["change_pct"] for entry in changes)
    return total / len(changes)


def _sentiment_from_average(avg_change: float) -> str:
    if avg_change > 0.3:
        return "Bullish 🟢"
    if avg_change < -0.3:
        return "Bearish 🔴"
    return "Neutral ⚪"


def build_end_of_day_message(
    snapshot: Dict[str, Any],
    currency_context: Dict[str, Any],
    benchmark_ticker: str,
    event_time: datetime,
) -> str:
    summary = snapshot.get("summary", {}) or {}
    holdings = snapshot.get("holdings", []) or []
    performance_history = snapshot.get("performance_history", []) or []

    portfolio_value = format_currency_value(summary.get("current_value"), currency_context)
    daily_change_value = format_signed_currency_value(summary.get("dod_value"), currency_context)
    daily_change_pct = _format_percent(summary.get("dod_pct"))
    weekly_change_pct = _format_percent(summary.get("weekly_change_pct"))
    day_change_pct = _format_percent(summary.get("dod_pct"))
    benchmark_change_pct = _benchmark_daily_percent(performance_history)
    cash_balance = format_currency_value(summary.get("cash_balance"), currency_context)

    changes = _extract_daily_changes(holdings)
    gainers = sorted(changes, key=lambda item: item["change_pct"], reverse=True)
    losers = sorted(changes, key=lambda item: item["change_pct"])

    top_gain = next((entry for entry in gainers if entry["change_pct"] > 0), None)
    second_gain = next(
        (entry for entry in gainers[1:] if entry["change_pct"] > 0),
        None,
    )
    top_loss = next((entry for entry in losers if entry["change_pct"] < 0), None)

    def _format_mover(entry: Optional[Dict[str, Any]]) -> str:
        if not entry:
            return "—"
        pct_str = _format_percent(entry["change_pct"])
        return f"{entry['ticker']} {pct_str} %"

    holdings_rows = _holdings_summary_rows(holdings, currency_context)
    date_label = event_time.astimezone(EASTERN_TZ).strftime("%Y-%m-%d")

    lines = [
        "📊 End-of-Day Portfolio Report",
        "",
        f"Date: {date_label}",
        f"Portfolio Value: {portfolio_value}",
        f"Daily Change: {daily_change_pct} % ({daily_change_value})",
        "",
        "Top Movers:",
        f"🥇 {_format_mover(top_gain)}",
        f"🥈 {_format_mover(second_gain)}",
        f"💔 {_format_mover(top_loss)}",
        "",
        "Holdings Summary:",
        "",
        "Ticker\tValue\tΔ (%)\tΔ (Value)",
        *holdings_rows,
        "",
        f"Benchmark ({benchmark_ticker}): {_format_percent(benchmark_change_pct)} %",
        f"Cash Balance: {cash_balance}",
        "",
        f"📈 Your portfolio is up {weekly_change_pct} % this week and {day_change_pct} % today.",
    ]
    return "\n".join(lines)


def build_beginning_of_day_message(
    snapshot: Dict[str, Any],
    currency_context: Dict[str, Any],
    benchmark_ticker: str,
    event_time: datetime,
) -> str:
    summary = snapshot.get("summary", {}) or {}
    holdings = snapshot.get("holdings", []) or []
    performance_history = snapshot.get("performance_history", []) or []

    portfolio_value = format_currency_value(summary.get("current_value"), currency_context)
    open_change_value = format_signed_currency_value(summary.get("dod_value"), currency_context)
    open_change_pct = _format_percent(summary.get("dod_pct"))
    benchmark_change_pct = _benchmark_daily_percent(performance_history)
    cash_balance = format_currency_value(summary.get("cash_balance"), currency_context)

    changes = _extract_daily_changes(holdings)
    gainers = sorted(changes, key=lambda item: item["change_pct"], reverse=True)
    losers = sorted(changes, key=lambda item: item["change_pct"])
    top_gain = next((entry for entry in gainers if entry["change_pct"] > 0), None)
    top_loss = next((entry for entry in losers if entry["change_pct"] < 0), None)

    avg_change = _average_percent(changes)
    sentiment = _sentiment_from_average(avg_change)

    def _format_simple_mover(entry: Optional[Dict[str, Any]]) -> str:
        if not entry:
            return "—"
        pct_str = _format_percent(entry["change_pct"])
        return f"{entry['ticker']} {pct_str} %"

    date_label = event_time.astimezone(EASTERN_TZ).strftime("%Y-%m-%d")

    lines = [
        "🚀 Market Open Insight",
        "",
        f"Date: {date_label}",
        f"Portfolio Value: {portfolio_value}",
        f"Opening Change: {open_change_pct} % ({open_change_value})",
        "",
        "Top Early Movers:",
        f"📈 {_format_simple_mover(top_gain)}",
        f"📉 {_format_simple_mover(top_loss)}",
        "",
        "Highlights:",
        "",
        f"Market sentiment: {sentiment}",
        "",
        f"Benchmark ({benchmark_ticker}): {_format_percent(benchmark_change_pct)} %",
        "",
        f"Cash available: {cash_balance}",
        "",
        "🕒 15 min into trading — monitoring for early volatility & breakout opportunities.",
    ]
    return "\n".join(lines)


def send_test_notification(
    _config: Dict[str, Any],
    *,
    bot_token: str,
    chat_id: str,
    mode: str = "end_of_day",
) -> str:
    """Send a lightweight test message to confirm Telegram delivery."""

    normalized_mode = "beginning_of_day" if mode == "beginning_of_day" else "end_of_day"
    label = "beginning-of-day" if normalized_mode == "beginning_of_day" else "end-of-day"
    timestamp = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M %Z")

    lines = [
        "🔔 DashFolio notification test",
        "",
        f"Mode: {label.replace('-', ' ').title()}",
        f"Timestamp: {timestamp}",
        "",
        "Telegram delivery succeeded — your bot token and chat ID are configured correctly.",
    ]
    message = "\n".join(lines)

    send_telegram_message(bot_token, chat_id, message)
    append_log(f"Telegram test {label} notification sent.")

    return message


_scheduler: Optional[NotificationScheduler] = None


def configure_notification_scheduler(data_store: str, config: Dict[str, Any]) -> None:
    """Ensure the background notification scheduler reflects ``config``."""

    global _scheduler
    if _scheduler is None:
        _scheduler = NotificationScheduler(data_store)
        _scheduler.start()
    _scheduler.update(config)
