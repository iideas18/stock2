from datetime import date
from unittest.mock import MagicMock
import pandas as pd

from instock.job import factor_compute_daily_job as job
from instock.factors import storage
from instock.factors.registry import clear_registry, register
from instock.factors.base import Factor


class _StubFactor(Factor):
    name = "stub"
    category = "technical"
    description = "stub"
    frequency = "daily"
    universe = "all_a"
    dependencies = []

    def compute(self, universe, start, end):
        return pd.DataFrame({
            "date": pd.date_range(start, end, freq="D"),
            "code": ["600519"] * ((end - start).days + 1),
            "value": [1.0] * ((end - start).days + 1),
        })

    def preprocess(self, raw):
        return raw


def test_job_writes_each_factor(tmp_factor_root, monkeypatch):
    clear_registry()
    register(_StubFactor())
    monkeypatch.setattr(job, "_resolve_universe", lambda: ["600519"])
    job.run(date(2024, 1, 2), date(2024, 1, 2))
    out = storage.read_factor(
        "stub",
        start=pd.Timestamp("2024-01-02"),
        end=pd.Timestamp("2024-01-02"),
    )
    assert len(out) == 1
    assert out["value"].iloc[0] == 1.0
