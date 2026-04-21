from __future__ import annotations

from pathlib import Path

import pandas as pd

from instock.monitoring.status import DataSourceRate


def _write_log(path: Path, rows):
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_no_log_yellow(tmp_path, monkeypatch):
    chk = DataSourceRate(
        source="akshare", window_hours=24, threshold=0.9,
        log_path=tmp_path / "missing.parquet",
    )
    assert chk.run().status == "YELLOW"


def test_high_rate_green(tmp_path):
    now = pd.Timestamp.now()
    rows = [
        {"ts": now - pd.Timedelta(hours=1), "source": "akshare", "ok": True},
    ] * 18 + [
        {"ts": now - pd.Timedelta(hours=1), "source": "akshare", "ok": False},
    ] * 2
    _write_log(tmp_path / "api_calls.parquet", rows)
    chk = DataSourceRate("akshare", 24, 0.9, tmp_path / "api_calls.parquet")
    assert chk.run().status == "GREEN"


def test_low_rate_red(tmp_path):
    now = pd.Timestamp.now()
    rows = [{"ts": now, "source": "akshare", "ok": False}] * 10 + \
           [{"ts": now, "source": "akshare", "ok": True}] * 1
    _write_log(tmp_path / "api_calls.parquet", rows)
    chk = DataSourceRate("akshare", 24, 0.9, tmp_path / "api_calls.parquet")
    assert chk.run().status == "RED"


def test_outside_window_ignored(tmp_path):
    now = pd.Timestamp.now()
    rows = [{"ts": now - pd.Timedelta(days=30), "source": "akshare",
             "ok": False}] * 100
    _write_log(tmp_path / "api_calls.parquet", rows)
    chk = DataSourceRate("akshare", 24, 0.9, tmp_path / "api_calls.parquet")
    assert chk.run().status == "YELLOW"  # no rows in window → degrade
