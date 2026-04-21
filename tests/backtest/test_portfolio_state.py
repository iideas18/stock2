from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.portfolio_state import PortfolioState


def test_initial_state_all_cash():
    p = PortfolioState(initial_cash=1_000_000.0)
    assert p.cash == 1_000_000.0
    assert p.positions == {}
    snap = p.snapshot(pd.Timestamp("2023-01-03"), prices={})
    assert snap["total_value"] == 1_000_000.0
    assert snap["position_value"] == 0.0


def test_apply_trade_buy_updates_cash_and_position():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade(
        code="600000", side="BUY", shares=100, fill_price=10.0,
        total_fees=5.01, slippage_value=0.5,
    )
    assert p.cash == pytest.approx(1_000_000.0 - 1000.0 - 5.01 - 0.5)
    assert p.positions["600000"].shares == 100
    assert p.positions["600000"].avg_cost == pytest.approx(
        (1000.0 + 5.01 + 0.5) / 100
    )


def test_apply_trade_sell_reduces_position():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 200, 10.0, 5.0, 1.0)
    p.apply_trade("600000", "SELL", 100, 11.0, 6.05, 0.55)
    pos = p.positions["600000"]
    assert pos.shares == 100
    assert p.cash == pytest.approx(
        1_000_000.0 - 2006.0 + (1100.0 - 6.05 - 0.55)
    )


def test_sell_all_removes_position():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 100, 10.0, 5.0, 0.5)
    p.apply_trade("600000", "SELL", 100, 11.0, 5.5, 0.55)
    assert "600000" not in p.positions


def test_mark_to_market_updates_snapshot():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 100, 10.0, 5.0, 0.5)
    prices = {"600000": 12.0}
    snap = p.snapshot(pd.Timestamp("2023-01-04"), prices)
    assert snap["position_value"] == pytest.approx(100 * 12.0)
    assert snap["total_value"] == pytest.approx(p.cash + 100 * 12.0)


def test_snapshot_missing_price_uses_avg_cost():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 100, 10.0, 5.0, 0.5)
    snap = p.snapshot(pd.Timestamp("2023-01-04"), prices={})
    avg = p.positions["600000"].avg_cost
    assert snap["position_value"] == pytest.approx(100 * avg)
