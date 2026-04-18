import pandas as pd
import pytest

from instock.portfolio.combiner import EqualRankCombiner


def test_equal_rank_combiner_basic():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02"] * 3),
        "code": ["600519", "000001", "000002"],
        "mom_20d": [0.1, 0.2, 0.3],
        "pe_ttm": [10.0, 20.0, 30.0],
    })
    out = EqualRankCombiner().combine(df)
    assert list(out.columns) == ["date", "code", "score"]
    assert out.sort_values("score")["code"].tolist() == [
        "600519", "000001", "000002"
    ]


def test_equal_rank_combiner_drops_row_with_any_nan():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02"] * 3),
        "code": ["600519", "000001", "000002"],
        "mom_20d": [0.1, None, 0.3],
        "pe_ttm": [10.0, 20.0, 30.0],
    })
    out = EqualRankCombiner().combine(df)
    assert "000001" not in out["code"].tolist()


def test_equal_rank_combiner_score_in_unit_interval():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02"] * 4),
        "code": ["A00001", "B00002", "C00003", "D00004"],
        "f1": [1.0, 2.0, 3.0, 4.0],
    })
    out = EqualRankCombiner().combine(df)
    assert out["score"].between(0.0, 1.0).all()
