from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from instock.job import generate_holdings_daily_job as job
from instock.portfolio import storage
from instock.portfolio.pipeline import StrategyConfig


def test_job_writes_each_strategy(tmp_holding_root, monkeypatch):
    out_df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
        "code": ["600519", "000001"],
        "weight": [0.5, 0.5],
        "score": [0.9, 0.7],
        "strategy": ["alpha", "alpha"],
    })
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = out_df
    monkeypatch.setattr(job, "StrategyPipeline", lambda: mock_pipeline)

    cfg = StrategyConfig(name="alpha", factors=["f1"])
    errs = job.run(date(2024, 1, 2), date(2024, 1, 2), [cfg])
    assert errs == {}

    read_back = storage.read_holding(
        "alpha",
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-02"),
    )
    assert len(read_back) == 2
    assert abs(read_back["weight"].sum() - 1.0) < 1e-9


def test_job_collects_per_strategy_errors(tmp_holding_root, monkeypatch):
    mock_pipeline = MagicMock()
    mock_pipeline.run.side_effect = RuntimeError("boom")
    monkeypatch.setattr(job, "StrategyPipeline", lambda: mock_pipeline)

    cfg = StrategyConfig(name="alpha", factors=["f1"])
    errs = job.run(date(2024, 1, 2), date(2024, 1, 2), [cfg])
    assert "alpha" in errs
    assert isinstance(errs["alpha"], RuntimeError)
