import pandas as pd
import pytest

from instock.portfolio import storage
from instock.portfolio.schemas import HoldingScheduleValidationError


def _df(dates, codes, weight, strategy="default"):
    n = len(dates)
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": codes,
        "weight": [weight] * n,
        "score": [0.0] * n,
        "strategy": [strategy] * n,
    })


def test_write_and_read_roundtrip(tmp_holding_root):
    df = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5)
    storage.write_holding("default", df)
    out = storage.read_holding("default",
                               pd.Timestamp("2024-01-02"),
                               pd.Timestamp("2024-01-02"))
    assert len(out) == 2
    assert abs(out["weight"].sum() - 1.0) < 1e-9


def test_write_partitions_by_year(tmp_holding_root):
    df_2023 = _df(["2023-12-29", "2023-12-29"], ["600519", "000001"], 0.5)
    df_2024 = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5)
    storage.write_holding("default", df_2023)
    storage.write_holding("default", df_2024)
    files = sorted(p.name for p in (tmp_holding_root / "default").iterdir())
    assert files == ["2023.parquet", "2024.parquet"]


def test_write_dedupes_on_rewrite(tmp_holding_root):
    df_a = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5)
    df_b = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5,
               strategy="default")
    storage.write_holding("default", df_a)
    storage.write_holding("default", df_b)
    out = storage.read_holding("default",
                               pd.Timestamp("2024-01-02"),
                               pd.Timestamp("2024-01-02"))
    assert len(out) == 2


def test_write_rejects_bad_weight_sum(tmp_holding_root):
    bad = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.4)
    with pytest.raises(HoldingScheduleValidationError):
        storage.write_holding("default", bad)


def test_read_returns_empty_frame_when_no_files(tmp_holding_root):
    out = storage.read_holding("nonexistent",
                               pd.Timestamp("2024-01-02"),
                               pd.Timestamp("2024-01-02"))
    assert out.empty
    assert list(out.columns) == ["date", "code", "weight", "score", "strategy"]
