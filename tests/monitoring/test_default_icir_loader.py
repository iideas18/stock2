"""Tests for _default_icir_loader (monitoring.status)."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from instock.monitoring.status import _default_icir_loader


@pytest.fixture
def seeded_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    factor_root = tmp_path / "factors"
    ohlcv_root = tmp_path / "ohlcv"
    factor_root.mkdir()
    ohlcv_root.mkdir()
    monkeypatch.setenv("INSTOCK_FACTOR_ROOT", str(factor_root))
    monkeypatch.setenv("INSTOCK_OHLCV_ROOT", str(ohlcv_root))
    return tmp_path


def _seed_factor(tmp_path: Path, name: str, rows: list[dict]) -> None:
    d = tmp_path / "factors" / name
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    for year, group in df.groupby(df["date"].dt.year):
        group.to_parquet(d / f"{year}.parquet", index=False)


def _seed_ohlcv(tmp_path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    for year, group in df.groupby(df["date"].dt.year):
        group.to_parquet(
            tmp_path / "ohlcv" / f"{year}.parquet", index=False
        )


def test_empty_factor_returns_empty(seeded_env: Path) -> None:
    df = _default_icir_loader("nonexistent")
    assert df.empty


def test_no_ohlcv_returns_empty(seeded_env: Path) -> None:
    # Factor data exists but ohlcv cache is empty → cannot compute fwd_ret.
    today = pd.Timestamp.now().normalize()
    _seed_factor(seeded_env, "f1", [
        {"date": today, "code": "000001", "value": 1.0},
    ])
    df = _default_icir_loader("f1")
    assert df.empty


def test_happy_path_joins_fwd_ret(seeded_env: Path) -> None:
    today = pd.Timestamp.now().normalize()
    d0 = today - pd.Timedelta(days=3)
    d1 = today - pd.Timedelta(days=2)
    d2 = today - pd.Timedelta(days=1)
    _seed_factor(seeded_env, "f1", [
        {"date": d0, "code": "000001", "value": 0.1},
        {"date": d0, "code": "000002", "value": 0.2},
        {"date": d1, "code": "000001", "value": 0.3},
        {"date": d1, "code": "000002", "value": 0.4},
    ])
    # Need next-day closes for each (date, code) in factor.
    _seed_ohlcv(seeded_env, [
        {"date": d0, "code": "000001", "close": 10.0},
        {"date": d0, "code": "000002", "close": 20.0},
        {"date": d1, "code": "000001", "close": 11.0},   # +10% vs d0
        {"date": d1, "code": "000002", "close": 19.0},   # -5% vs d0
        {"date": d2, "code": "000001", "close": 11.55},  # +5% vs d1
        {"date": d2, "code": "000002", "close": 19.76},  # +4% vs d1
    ])
    df = _default_icir_loader("f1")
    assert not df.empty
    assert set(df.columns) >= {"date", "code", "value", "fwd_ret"}
    # d0 rows should have fwd_ret = (close at d1 - close at d0) / close at d0
    row = df[
        (df["date"] == d0) & (df["code"] == "000001")
    ].iloc[0]
    assert abs(row["fwd_ret"] - 0.10) < 1e-6
    row = df[
        (df["date"] == d0) & (df["code"] == "000002")
    ].iloc[0]
    assert abs(row["fwd_ret"] - (-0.05)) < 1e-6
