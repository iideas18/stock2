from __future__ import annotations

import os
from pathlib import Path
import pandas as pd


def _root() -> Path:
    root = os.environ.get("INSTOCK_FACTOR_ROOT", "data/factors")
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _factor_dir(name: str) -> Path:
    d = _root() / name
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_factor(name: str, df: pd.DataFrame) -> None:
    """Append-and-dedup writer, partitioned by year of `date`."""
    if df.empty:
        return
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    for year, group in df.groupby("year"):
        target = _factor_dir(name) / f"{year}.parquet"
        payload = group.drop(columns=["year"])
        if target.exists():
            old = pd.read_parquet(target)
            payload = pd.concat([old, payload], ignore_index=True)
        payload = (
            payload.drop_duplicates(subset=["date", "code"], keep="last")
                   .sort_values(["date", "code"])
                   .reset_index(drop=True)
        )
        payload.to_parquet(target, index=False)


def read_factor(
    name: str, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    d = _factor_dir(name)
    frames = []
    for year in range(start.year, end.year + 1):
        target = d / f"{year}.parquet"
        if target.exists():
            frames.append(pd.read_parquet(target))
    if not frames:
        return pd.DataFrame(columns=["date", "code", "value"])
    df = pd.concat(frames, ignore_index=True)
    mask = (df["date"] >= start) & (df["date"] <= end)
    return df.loc[mask].reset_index(drop=True)
