import numpy as np
import pandas as pd
import pytest

from instock.factors import preprocess


def _long_frame(values, dates=None, codes=None):
    dates = dates or ["2024-01-02"] * len(values)
    codes = codes or [f"{i:06d}" for i in range(len(values))]
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": codes,
        "value": values,
    })


def test_winsorize_clips_tails():
    df = _long_frame([-100.0, -1, 0, 1, 100.0])
    out = preprocess.winsorize(df, lower=0.2, upper=0.8)
    assert out["value"].min() > -100.0
    assert out["value"].max() < 100.0


def test_zscore_mean_zero_std_one():
    df = _long_frame([1.0, 2, 3, 4, 5])
    out = preprocess.zscore(df)
    assert abs(out["value"].mean()) < 1e-9
    assert abs(out["value"].std(ddof=0) - 1.0) < 1e-9


def test_neutralize_removes_industry_mean():
    df = pd.DataFrame({
        "date":  pd.to_datetime(["2024-01-02"] * 4),
        "code":  ["000001", "000002", "600000", "600001"],
        "value": [1.0, 3.0, 10.0, 12.0],
    })
    industry_map = {"000001": "bank", "000002": "bank",
                    "600000": "tech", "600001": "tech"}
    out = preprocess.neutralize(df, industry_map=industry_map)
    grouped = out.groupby(out["code"].map(industry_map))["value"].mean()
    assert all(abs(v) < 1e-9 for v in grouped)


def test_default_pipeline_runs():
    df = _long_frame([1.0, 2, 3, 4, 5])
    out = preprocess.default_pipeline(df)
    assert set(out.columns) == {"date", "code", "value"}
    assert len(out) == 5
