from __future__ import annotations

import json
from datetime import date

from instock.backtest.config import BacktestConfig


def test_default_config():
    c = BacktestConfig(strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29))
    assert c.initial_capital == 1_000_000.0
    assert c.lot_size == 100
    assert c.slippage_bps == 5.0
    assert c.rng_seed == 42
    assert c.benchmarks == ("000300.SH", "000905.SH", "000852.SH")
    assert c.price_adjust == "qfq"
    assert c.enable_fees is True
    assert c.enable_slippage is True


def test_config_to_dict_deterministic():
    c = BacktestConfig(strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29))
    d = c.to_dict()
    s1 = json.dumps(d, sort_keys=True)
    s2 = json.dumps(c.to_dict(), sort_keys=True)
    assert s1 == s2
    assert d["strategy"] == "x"
    assert d["start"] == "2023-01-04"


def test_config_override_fees_off():
    c = BacktestConfig(
        strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29),
        enable_fees=False, enable_slippage=False,
    )
    assert c.enable_fees is False
    assert c.enable_slippage is False


def test_config_to_dict_cross_instance_deterministic():
    c1 = BacktestConfig(strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29))
    c2 = BacktestConfig(strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29))
    assert json.dumps(c1.to_dict(), sort_keys=True) == json.dumps(c2.to_dict(), sort_keys=True)


def test_config_is_frozen():
    import dataclasses

    import pytest

    c = BacktestConfig(strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29))
    with pytest.raises(dataclasses.FrozenInstanceError):
        c.strategy = "y"  # type: ignore[misc]
