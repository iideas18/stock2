from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from instock.backtest.config import BacktestConfig
from instock.backtest.engine import BacktestEngine


def _stub_ohlcv(codes, start, end):
    dates = pd.bdate_range(start, end, freq="B")
    rows = []
    for c in codes:
        base = 10.0
        for i, d in enumerate(dates):
            price = base + 0.1 * i
            rows.append({
                "date": d, "code": c,
                "open": price, "high": price * 1.01,
                "low": price * 0.99, "close": price,
                "volume": 100_000, "amount": price * 100_000,
            })
    return pd.DataFrame(rows)


def _stub_holding(dates, codes):
    rows = []
    for d in dates:
        for c in codes:
            rows.append({
                "date": d, "code": c,
                "weight": 1.0 / len(codes),
                "score": 1.0,
                "strategy": "stub",
            })
    return pd.DataFrame(rows)


def test_engine_end_to_end_minimal(tmp_path):
    codes = ["600000", "600519"]
    start = date(2023, 1, 3)
    end = date(2023, 1, 20)

    ohlcv = _stub_ohlcv(codes, start, end)
    holding = _stub_holding([pd.Timestamp("2023-01-04")], codes)

    cal = [d.date() for d in pd.bdate_range(start, end)]
    source = MagicMock()
    source.get_trade_calendar.return_value = cal

    store = MagicMock()
    store.get_panel.return_value = ohlcv

    cfg = BacktestConfig(
        strategy="stub", start=start, end=end,
        initial_capital=1_000_000.0, enable_slippage=False,
    )
    engine = BacktestEngine(source=source, ohlcv_store=store)
    result = engine.run(holding_schedule=holding, config=cfg)

    assert "trades" in result and "positions" in result and "nav" in result
    nav = result["nav"]
    assert len(nav) == len(cal)
    assert nav["nav"].iloc[-1] > 0
    trades = result["trades"]
    assert (trades["side"] == "BUY").sum() == 2


def test_engine_no_rebalance_dates_returns_flat_nav():
    codes = ["600000"]
    start = date(2023, 1, 3)
    end = date(2023, 1, 10)
    source = MagicMock()
    cal = [d.date() for d in pd.bdate_range(start, end)]
    source.get_trade_calendar.return_value = cal
    store = MagicMock()
    store.get_panel.return_value = _stub_ohlcv(codes, start, end)

    cfg = BacktestConfig(strategy="s", start=start, end=end)
    engine = BacktestEngine(source=source, ohlcv_store=store)
    res = engine.run(holding_schedule=pd.DataFrame(columns=[
        "date", "code", "weight", "score", "strategy"]), config=cfg)
    assert (res["nav"]["total_value"] == cfg.initial_capital).all()
    assert len(res["trades"]) == 0


def test_engine_skips_suspended_and_defers(tmp_path):
    codes = ["600000"]
    start = date(2023, 1, 3)
    end = date(2023, 1, 13)
    ohlcv = _stub_ohlcv(codes, start, end)
    rebal = pd.Timestamp("2023-01-04")
    next_day = pd.Timestamp("2023-01-05")
    ohlcv.loc[
        (ohlcv["code"] == "600000") & (ohlcv["date"] == next_day), "volume"
    ] = 0
    holding = _stub_holding([rebal], codes)

    cal = [d.date() for d in pd.bdate_range(start, end)]
    source = MagicMock()
    source.get_trade_calendar.return_value = cal
    store = MagicMock()
    store.get_panel.return_value = ohlcv

    cfg = BacktestConfig(strategy="s", start=start, end=end,
                         enable_slippage=False)
    engine = BacktestEngine(source=source, ohlcv_store=store)
    res = engine.run(holding_schedule=holding, config=cfg)

    trades = res["trades"]
    buys = trades[trades["side"] == "BUY"]
    assert len(buys) == 1
    assert buys["date"].iloc[0] == pd.Timestamp("2023-01-06")
