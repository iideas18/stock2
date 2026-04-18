import os
import pandas as pd
import pytest

from instock.factors import storage


def _sample(dates):
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": ["600519"] * len(dates),
        "value": [1.0 + i for i in range(len(dates))],
    })


def test_write_and_read_roundtrip(tmp_factor_root):
    df = _sample(["2024-01-02", "2024-01-03"])
    storage.write_factor("mom_5d", df)
    out = storage.read_factor("mom_5d",
                              start=pd.Timestamp("2024-01-01"),
                              end=pd.Timestamp("2024-01-10"))
    assert len(out) == 2
    assert set(out["code"]) == {"600519"}


def test_write_partitions_by_year(tmp_factor_root):
    df = _sample(["2023-12-29", "2024-01-02"])
    storage.write_factor("mom_5d", df)
    root = tmp_factor_root / "mom_5d"
    assert (root / "2023.parquet").exists()
    assert (root / "2024.parquet").exists()


def test_write_is_idempotent_for_same_dates(tmp_factor_root):
    df = _sample(["2024-01-02"])
    storage.write_factor("mom_5d", df)
    storage.write_factor("mom_5d", df)
    out = storage.read_factor("mom_5d",
                              start=pd.Timestamp("2024-01-01"),
                              end=pd.Timestamp("2024-01-10"))
    assert len(out) == 1
