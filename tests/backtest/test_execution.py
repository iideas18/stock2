from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.costs import StandardFeeModel, BpsSlippage
from instock.backtest.execution import Executor, Order
from instock.backtest.portfolio_state import PortfolioState


def test_execute_buy_lot_size_floors():
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    ex = Executor(
        fee_model=StandardFeeModel(),
        slippage_model=BpsSlippage(bps=0.0),
        lot_size=100,
    )
    order = Order(
        code="600000", side="BUY",
        target_value=999.0, target_w=0.001,
    )
    row_today = {"open": 10.0, "close": 10.0, "volume": 100}
    trade = ex.execute(
        order=order, row_today=row_today,
        portfolio=portfolio, at=pd.Timestamp("2023-01-03"),
        reason="REBALANCE",
    )
    assert trade["filled_shares"] == 0
    assert trade["reason"] == "NO_OP"


def test_execute_buy_normal_flow():
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    ex = Executor(
        fee_model=StandardFeeModel(),
        slippage_model=BpsSlippage(bps=5.0),
        lot_size=100,
    )
    order = Order(code="600000", side="BUY", target_value=10_000.0, target_w=0.01)
    row = {"open": 10.0, "close": 10.5, "volume": 100_000}
    trade = ex.execute(order, row, portfolio, pd.Timestamp("2023-01-03"), "REBALANCE")
    assert trade["fill_price"] == pytest.approx(10.005)
    assert trade["filled_shares"] == 900
    assert trade["side"] == "BUY"
    assert trade["commission"] >= 5.0
    assert portfolio.positions["600000"].shares == 900


def test_execute_sell_normal_flow():
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    portfolio.apply_trade("600000", "BUY", 500, 10.0, 5.0, 0.0)
    ex = Executor(StandardFeeModel(), BpsSlippage(5.0), lot_size=100)
    order = Order(code="600000", side="SELL", target_value=2000.0, target_w=0.0)
    row = {"open": 11.0, "close": 11.0, "volume": 100_000}
    trade = ex.execute(order, row, portfolio, pd.Timestamp("2023-01-04"), "REBALANCE")
    assert trade["side"] == "SELL"
    assert trade["filled_shares"] == 100
    assert portfolio.positions["600000"].shares == 400


def test_execute_sell_capped_by_held_shares():
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    portfolio.apply_trade("600000", "BUY", 200, 10.0, 5.0, 0.0)
    ex = Executor(StandardFeeModel(), BpsSlippage(0.0), lot_size=100)
    order = Order(code="600000", side="SELL", target_value=100_000.0, target_w=0.0)
    row = {"open": 10.0, "close": 10.0, "volume": 100_000}
    trade = ex.execute(order, row, portfolio, pd.Timestamp("2023-01-04"), "REBALANCE")
    assert trade["filled_shares"] == 200
    assert "600000" not in portfolio.positions
