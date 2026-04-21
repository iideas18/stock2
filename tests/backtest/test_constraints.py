from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.constraints import (
    ConstraintContext,
    SuspendedConstraint,
    LimitUpConstraint,
    LimitDownConstraint,
    DelistConstraint,
    ConstraintChain,
)


def test_suspended_blocks_buy_when_volume_zero():
    today = {"open": 10.0, "close": 10.0, "volume": 0}
    ok, reason = SuspendedConstraint().check(
        side="BUY", row_today=today, row_yesterday={"close": 10.0},
    )
    assert ok is False
    assert reason == "SUSPENDED"


def test_suspended_blocks_sell_when_volume_zero():
    today = {"open": 10.0, "close": 10.0, "volume": 0}
    ok, reason = SuspendedConstraint().check(
        side="SELL", row_today=today, row_yesterday={"close": 10.0},
    )
    assert ok is False


def test_suspended_allows_when_volume_nonzero():
    today = {"open": 10.0, "close": 10.0, "volume": 100_000}
    ok, _ = SuspendedConstraint().check(
        side="BUY", row_today=today, row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_limit_up_blocks_buy_mainboard():
    ok, reason = LimitUpConstraint(tolerance=1e-3).check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 11.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is False
    assert reason == "LIMIT_UP"


def test_limit_up_allows_sell():
    ok, _ = LimitUpConstraint().check(
        code="600000",
        is_st=False,
        side="SELL",
        row_today={"open": 11.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_limit_up_chinext_20pct():
    ok, _ = LimitUpConstraint().check(
        code="300001",
        is_st=False,
        side="BUY",
        row_today={"open": 11.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_limit_down_blocks_sell():
    ok, reason = LimitDownConstraint(tolerance=1e-3).check(
        code="600000",
        is_st=False,
        side="SELL",
        row_today={"open": 9.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is False
    assert reason == "LIMIT_DOWN"


def test_limit_down_allows_buy():
    ok, _ = LimitDownConstraint().check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 9.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_delist_forces_sell_when_flag_set():
    ok, reason = DelistConstraint().check(
        side="SELL", delisted=True,
    )
    assert ok is True
    assert reason == "DELIST_FORCE_OUT"


def test_delist_blocks_buy():
    ok, reason = DelistConstraint().check(
        side="BUY", delisted=True,
    )
    assert ok is False


def test_chain_short_circuits_on_first_block():
    chain = ConstraintChain([
        SuspendedConstraint(),
        LimitUpConstraint(),
    ])
    ok, reason = chain.check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 11.0, "volume": 0},
        row_yesterday={"close": 10.0},
        delisted=False,
    )
    assert ok is False
    assert reason == "SUSPENDED"


def test_chain_allows_when_all_pass():
    chain = ConstraintChain([SuspendedConstraint(), LimitUpConstraint()])
    ok, _ = chain.check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 10.5, "volume": 100_000},
        row_yesterday={"close": 10.0},
        delisted=False,
    )
    assert ok is True
