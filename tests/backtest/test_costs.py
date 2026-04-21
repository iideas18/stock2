from __future__ import annotations

import pytest

from instock.backtest.costs import (
    FeeModel, StandardFeeModel, ZeroFeeModel,
)


def test_standard_fee_buy_min_commission():
    fm = StandardFeeModel()
    fees = fm.compute(value=1000.0, side="BUY")
    assert fees["commission"] == 5.0
    assert fees["stamp_tax"] == 0.0
    assert fees["transfer_fee"] == pytest.approx(1000.0 * 0.00001)


def test_standard_fee_buy_pct_dominates():
    fm = StandardFeeModel()
    fees = fm.compute(value=100_000.0, side="BUY")
    assert fees["commission"] == pytest.approx(100_000.0 * 0.00025)
    assert fees["stamp_tax"] == 0.0


def test_standard_fee_sell_stamp():
    fm = StandardFeeModel()
    fees = fm.compute(value=100_000.0, side="SELL")
    assert fees["stamp_tax"] == pytest.approx(100_000.0 * 0.0005)
    assert fees["commission"] == pytest.approx(100_000.0 * 0.00025)


def test_standard_fee_zero_value():
    fm = StandardFeeModel()
    fees = fm.compute(value=0.0, side="BUY")
    assert fees == {"commission": 5.0, "stamp_tax": 0.0, "transfer_fee": 0.0}


def test_zero_fee_returns_zero():
    fm = ZeroFeeModel()
    for side in ("BUY", "SELL"):
        fees = fm.compute(value=100_000.0, side=side)
        assert fees == {"commission": 0.0, "stamp_tax": 0.0, "transfer_fee": 0.0}


def test_fee_model_is_abc():
    assert issubclass(StandardFeeModel, FeeModel)


from instock.backtest.costs import SlippageModel, BpsSlippage, ZeroSlippage


def test_bps_slippage_buy_positive():
    s = BpsSlippage(bps=5.0)
    fill = s.fill_price(open_price=10.0, side="BUY")
    assert fill == pytest.approx(10.0 * (1 + 5 * 1e-4))


def test_bps_slippage_sell_negative():
    s = BpsSlippage(bps=5.0)
    fill = s.fill_price(open_price=10.0, side="SELL")
    assert fill == pytest.approx(10.0 * (1 - 5 * 1e-4))


def test_bps_slippage_symmetric():
    s = BpsSlippage(bps=10.0)
    buy = s.fill_price(100.0, "BUY") - 100.0
    sell = 100.0 - s.fill_price(100.0, "SELL")
    assert buy == pytest.approx(sell)


def test_zero_slippage_returns_open():
    s = ZeroSlippage()
    assert s.fill_price(10.0, "BUY") == 10.0
    assert s.fill_price(10.0, "SELL") == 10.0


def test_slippage_model_is_abc():
    assert issubclass(BpsSlippage, SlippageModel)
