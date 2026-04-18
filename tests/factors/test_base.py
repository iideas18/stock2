from datetime import date
import pandas as pd
import pytest

from instock.factors.base import Factor
from instock.factors.registry import register, get_all, clear_registry


class DummyFactor(Factor):
    name = "dummy"
    category = "technical"
    description = "returns 1.0 for every (date, code)"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["ohlcv"]

    def compute(self, universe, start, end):
        rows = [
            {"date": pd.Timestamp(start), "code": c, "value": 1.0}
            for c in universe
        ]
        return pd.DataFrame(rows)


def test_factor_compute_shape():
    f = DummyFactor()
    df = f.compute(["600519", "000001"], date(2024, 1, 2), date(2024, 1, 2))
    assert set(df.columns) == {"date", "code", "value"}
    assert len(df) == 2


def test_registry_register_and_get(monkeypatch):
    clear_registry()
    f = DummyFactor()
    register(f)
    assert get_all()["dummy"] is f


def test_registry_duplicate_raises():
    clear_registry()
    register(DummyFactor())
    with pytest.raises(ValueError):
        register(DummyFactor())
