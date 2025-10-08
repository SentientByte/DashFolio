import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_PATH = PROJECT_ROOT / "Calculations" / "performance.py"

spec = importlib.util.spec_from_file_location("performance", PERFORMANCE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = module
spec.loader.exec_module(module)  # type: ignore[attr-defined]

PerformanceFrame = module.PerformanceFrame
build_daily_table = module.build_daily_table
compute_twr = module.compute_twr
compute_equity_index = module.compute_equity_index


def _column(df, name):
    if isinstance(df, PerformanceFrame):
        return df.column(name)
    return list(df[name])


def _rows(df):
    if isinstance(df, PerformanceFrame):
        return df.to_dicts()
    return df.to_dict(orient="records")


def _run_pipeline(trades, prices, cash_events):
    base = build_daily_table(trades, prices, cash_events)
    with_twr = compute_twr(base)
    with_equity = compute_equity_index(with_twr)
    return with_equity


def test_constant_prices_with_external_flows_only_keeps_twr_flat():
    dates = ["2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06"]
    cash_events = [
        {"date": dates[0], "type": "deposit", "amount": 1000.0},
        {"date": dates[2], "type": "withdrawal", "amount": 400.0},
    ]

    df = _run_pipeline([], [], cash_events)

    assert all(value == 100 for value in _column(df, "index_twr"))

    rows = _rows(df)
    withdraw_row = next(row for row in rows if str(row["date"]) == "2023-01-04")
    assert withdraw_row["index_equity"] == 60.0


def test_trades_reflect_in_both_indices_when_prices_change():
    trades = [
        {
            "timestamp": "2023-01-02",
            "symbol": "AAA",
            "side": "buy",
            "qty": 1,
            "price": 100.0,
            "commission": 0.0,
            "fees": 0.0,
        }
    ]
    prices = [
        {"date": "2023-01-02", "symbol": "AAA", "close": 100.0},
        {"date": "2023-01-03", "symbol": "AAA", "close": 110.0},
        {"date": "2023-01-04", "symbol": "AAA", "close": 110.0},
    ]
    cash_events = [
        {"date": "2023-01-02", "type": "deposit", "amount": 100.0},
    ]

    df = _run_pipeline(trades, prices, cash_events)

    rows = _rows(df)
    day_two = next(row for row in rows if str(row["date"]) == "2023-01-03")
    assert pytest.approx(day_two["index_twr"], rel=1e-9) == 110.0
    assert pytest.approx(day_two["index_equity"], rel=1e-9) == 110.0


def test_realised_gains_do_not_change_twr_when_no_flows():
    trades = [
        {
            "timestamp": "2023-01-02",
            "symbol": "AAA",
            "side": "buy",
            "qty": 1,
            "price": 100.0,
            "commission": 0.0,
            "fees": 0.0,
        },
        {
            "timestamp": "2023-01-04",
            "symbol": "AAA",
            "side": "sell",
            "qty": 1,
            "price": 110.0,
            "commission": 0.0,
            "fees": 0.0,
        },
    ]
    prices = [
        {"date": "2023-01-02", "symbol": "AAA", "close": 100.0},
        {"date": "2023-01-03", "symbol": "AAA", "close": 110.0},
        {"date": "2023-01-04", "symbol": "AAA", "close": 110.0},
    ]
    cash_events = [
        {"date": "2023-01-02", "type": "deposit", "amount": 100.0},
    ]

    df = _run_pipeline(trades, prices, cash_events)

    rows = _rows(df)
    sell_day = next(row for row in rows if str(row["date"]) == "2023-01-04")
    assert pytest.approx(sell_day["index_twr"], rel=1e-9) == 110.0
    assert pytest.approx(sell_day["index_equity"], rel=1e-9) == 110.0

