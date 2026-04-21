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
