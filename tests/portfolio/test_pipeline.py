from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from instock.portfolio.combiner import EqualRankCombiner
from instock.portfolio.constraints import MaxWeightConstraint
from instock.portfolio.filters import SuspendedFilter, NewListingFilter
from instock.portfolio.pipeline import StrategyConfig, StrategyPipeline
from instock.portfolio.schedule import DailyRebalance
from instock.portfolio.selector import TopNSelector
from instock.portfolio.weighter import EqualWeighter


def _factor_df(factor_name, dates, codes, values):
    rows = []
    for d in dates:
        for c, v in zip(codes, values):
            rows.append({"date": pd.Timestamp(d), "code": c, "value": v})
    return pd.DataFrame(rows)


def _ohlcv_panel(codes, dates):
    rows = []
    for c in codes:
        for d in dates:
            rows.append({
                "date": pd.Timestamp(d),
                "code": c,
                "volume": 1_000_000,
            })
    return pd.DataFrame(rows)


def test_pipeline_end_to_end_small(monkeypatch):
    dates = ["2023-10-01", "2023-10-02", "2023-12-01", "2023-12-02",
             "2024-01-02", "2024-01-03"]
    codes = ["000001", "000002", "000003", "000004", "000005"]

    def fake_read_factor(name, start, end):
        assert name == "f1"
        return _factor_df("f1", dates, codes, [0.1, 0.2, 0.3, 0.4, 0.5])

    mock_source = MagicMock()
    mock_source.get_trade_calendar.return_value = [
        date.fromisoformat(d) for d in dates
    ]
    mock_source.get_ohlcv.return_value = _ohlcv_panel(codes, dates)
    mock_source.get_index_member.return_value = codes

    monkeypatch.setattr(
        "instock.portfolio.pipeline.read_factor", fake_read_factor
    )
    monkeypatch.setattr(
        "instock.portfolio.pipeline.get_source", lambda: mock_source
    )

    cfg = StrategyConfig(
        name="test",
        factors=["f1"],
        combiner=EqualRankCombiner(),
        filters=[SuspendedFilter(), NewListingFilter(min_days=30)],
        selector=TopNSelector(n=2),
        weighter=EqualWeighter(),
        constraints=[MaxWeightConstraint(0.6)],
        schedule=DailyRebalance(),
    )
    out = StrategyPipeline().run(
        date(2024, 1, 2), date(2024, 1, 3), cfg
    )

    assert not out.empty
    assert set(out["strategy"].unique()) == {"test"}
    for d, g in out.groupby("date"):
        assert abs(g["weight"].sum() - 1.0) < 1e-9
        assert len(g) == 2
        assert (g["weight"] <= 0.6 + 1e-9).all()


def test_pipeline_empty_factor_data_returns_empty(monkeypatch):
    monkeypatch.setattr(
        "instock.portfolio.pipeline.read_factor",
        lambda name, start, end: pd.DataFrame(
            columns=["date", "code", "value"]
        ),
    )
    mock_source = MagicMock()
    mock_source.get_trade_calendar.return_value = [date(2024, 1, 2)]
    mock_source.get_ohlcv.return_value = pd.DataFrame(
        columns=["date", "code", "volume"]
    )
    mock_source.get_index_member.return_value = ["000001"]
    monkeypatch.setattr(
        "instock.portfolio.pipeline.get_source", lambda: mock_source
    )

    cfg = StrategyConfig(name="test", factors=["f1"])
    out = StrategyPipeline().run(
        date(2024, 1, 2), date(2024, 1, 2), cfg
    )
    assert out.empty
    assert list(out.columns) == [
        "date", "code", "weight", "score", "strategy"
    ]
