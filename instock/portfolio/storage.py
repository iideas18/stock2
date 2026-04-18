"""Parquet storage for HoldingSchedule.

Mirrors instock/factors/storage.py:
  <INSTOCK_HOLDING_ROOT>/<strategy>/<year>.parquet
  (date, code, strategy) unique per file via drop_duplicates keep="last".
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from .schemas import (
    HOLDING_SCHEDULE_SCHEMA,
    validate_holding_invariants,
)

_COLUMNS = ["date", "code", "weight", "score", "strategy"]


def _root() -> Path:
    root = os.environ.get("INSTOCK_HOLDING_ROOT", "data/holdings")
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _strategy_dir(strategy: str) -> Path:
    d = _root() / strategy
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_holding(strategy: str, df: pd.DataFrame) -> None:
    """Validate + append-and-dedup writer, partitioned by year(date)."""
    if df.empty:
        return
    df = HOLDING_SCHEDULE_SCHEMA.validate(df.copy())
    validate_holding_invariants(df)
    df["year"] = df["date"].dt.year
    for year, group in df.groupby("year"):
        target = _strategy_dir(strategy) / f"{year}.parquet"
        payload = group.drop(columns=["year"])
        if target.exists():
            old = pd.read_parquet(target)
            payload = pd.concat([old, payload], ignore_index=True)
        payload = (
            payload.drop_duplicates(
                subset=["date", "code", "strategy"], keep="last"
            )
            .sort_values(["date", "code"])
            .reset_index(drop=True)
        )
        payload.to_parquet(target, index=False)


def read_holding(
    strategy: str, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    d = _strategy_dir(strategy)
    frames = []
    for year in range(start.year, end.year + 1):
        target = d / f"{year}.parquet"
        if target.exists():
            frames.append(pd.read_parquet(target))
    if not frames:
        return pd.DataFrame(columns=_COLUMNS)
    df = pd.concat(frames, ignore_index=True)
    mask = (df["date"] >= start) & (df["date"] <= end)
    return df.loc[mask].reset_index(drop=True)
