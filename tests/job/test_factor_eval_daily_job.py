"""Tests for instock.job.factor_eval_daily_job."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from instock.job import factor_eval_daily_job as job


@pytest.fixture
def seeded_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    factor_root = tmp_path / "factors"
    ohlcv_root = tmp_path / "ohlcv"
    reports_root = tmp_path / "factor_reports"
    factor_root.mkdir()
    ohlcv_root.mkdir()
    monkeypatch.setenv("INSTOCK_FACTOR_ROOT", str(factor_root))
    monkeypatch.setenv("INSTOCK_OHLCV_ROOT", str(ohlcv_root))
    monkeypatch.setenv("INSTOCK_FACTOR_REPORTS_ROOT", str(reports_root))
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
        group.to_parquet(tmp_path / "ohlcv" / f"{year}.parquet", index=False)


def test_evaluate_and_report_writes_html(seeded_env: Path) -> None:
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
    _seed_ohlcv(seeded_env, [
        {"date": d0, "code": "000001", "close": 10.0},
        {"date": d0, "code": "000002", "close": 20.0},
        {"date": d1, "code": "000001", "close": 11.0},
        {"date": d1, "code": "000002", "close": 19.0},
        {"date": d2, "code": "000001", "close": 11.55},
        {"date": d2, "code": "000002", "close": 19.76},
    ])

    out = job.evaluate_and_report("f1", window_days=30)
    assert out is not None
    assert out.exists()
    content = out.read_text(encoding="utf-8")
    assert "f1" in content
    assert "<table" in content


def test_evaluate_and_report_no_factor(seeded_env: Path) -> None:
    assert job.evaluate_and_report("nonexistent") is None


def test_evaluate_and_report_no_ohlcv(seeded_env: Path) -> None:
    today = pd.Timestamp.now().normalize()
    _seed_factor(seeded_env, "f1", [
        {"date": today, "code": "000001", "value": 1.0},
    ])
    assert job.evaluate_and_report("f1") is None
