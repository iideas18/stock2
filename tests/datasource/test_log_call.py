from __future__ import annotations

from pathlib import Path

import pandas as pd

from instock.datasource.io import log_call


def test_creates_file(tmp_path):
    p = tmp_path / "api_calls.parquet"
    log_call("akshare", ok=True, path=p)
    df = pd.read_parquet(p)
    assert list(df.columns) == ["ts", "source", "ok"]
    assert df.iloc[0]["source"] == "akshare"
    assert bool(df.iloc[0]["ok"]) is True


def test_appends(tmp_path):
    p = tmp_path / "api_calls.parquet"
    log_call("akshare", ok=True, path=p)
    log_call("akshare", ok=False, path=p)
    df = pd.read_parquet(p)
    assert len(df) == 2
    assert list(df["ok"]) == [True, False]


def test_swallows_failure(tmp_path):
    d = tmp_path / "bad"
    d.mkdir()
    # path is a directory; write must fail internally but log_call swallows it
    log_call("akshare", ok=True, path=d)


def test_env_var_default(tmp_path, monkeypatch):
    target = tmp_path / "env_target.parquet"
    monkeypatch.setenv("INSTOCK_API_CALL_LOG", str(target))
    log_call("akshare", ok=True)
    assert target.exists()
