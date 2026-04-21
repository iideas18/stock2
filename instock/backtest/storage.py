"""Parquet IO for backtest artifacts.

Layout:
  <INSTOCK_BACKTEST_ROOT>/<run_id>/{trades,positions,nav}.parquet
  <INSTOCK_BACKTEST_ROOT>/_metrics.parquet   (append; dedup by run_id)
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from .schemas import (
    METRICS_SCHEMA, NAV_SCHEMA, POSITION_SCHEMA, TRADE_SCHEMA,
)


def _root() -> Path:
    r = Path(os.environ.get("INSTOCK_BACKTEST_ROOT", "data/backtest"))
    r.mkdir(parents=True, exist_ok=True)
    return r


def _run_dir(run_id: str) -> Path:
    d = _root() / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_run(
    run_id: str,
    trades: pd.DataFrame,
    positions: pd.DataFrame,
    nav: pd.DataFrame,
) -> None:
    if not trades.empty:
        TRADE_SCHEMA.validate(trades.copy())
    if not positions.empty:
        POSITION_SCHEMA.validate(positions.copy())
    if not nav.empty:
        NAV_SCHEMA.validate(nav.copy())
    d = _run_dir(run_id)
    trades.to_parquet(d / "trades.parquet", index=False)
    positions.to_parquet(d / "positions.parquet", index=False)
    nav.to_parquet(d / "nav.parquet", index=False)


def read_run(run_id: str) -> dict:
    d = _run_dir(run_id)

    def _read(name: str) -> pd.DataFrame:
        p = d / f"{name}.parquet"
        return pd.read_parquet(p) if p.exists() else pd.DataFrame()

    return {
        "trades": _read("trades"),
        "positions": _read("positions"),
        "nav": _read("nav"),
    }


def append_metrics(row: dict) -> None:
    df = pd.DataFrame([row])
    METRICS_SCHEMA.validate(df.copy())
    path = _root() / "_metrics.parquet"
    if path.exists():
        old = pd.read_parquet(path)
        merged = pd.concat([old, df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["run_id"], keep="last")
    else:
        merged = df
    merged.to_parquet(path, index=False)


def read_metrics() -> pd.DataFrame:
    path = _root() / "_metrics.parquet"
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()
