"""ST-flag snapshot storage and as-of reader."""
from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .schemas import ST_SNAPSHOT_SCHEMA, RefdataNotAvailable


def _root() -> Path:
    root = Path(os.environ.get("INSTOCK_REFDATA_ROOT", "data/refdata"))
    return root / "st"


def write_st_snapshot(df: pd.DataFrame) -> Path:
    """Write one ST snapshot keyed by its snapshot_date (YYYYMMDD)."""
    df = ST_SNAPSHOT_SCHEMA.validate(df.copy())
    unique_dates = df["snapshot_date"].unique()
    if len(unique_dates) != 1:
        raise ValueError(
            f"snapshot_date must be unique per snapshot, got {len(unique_dates)}"
        )
    snap = pd.Timestamp(unique_dates[0])
    d = _root()
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{snap.strftime('%Y%m%d')}.parquet"
    df.to_parquet(path, index=False)
    return path


def read_st_flags(at: date) -> set[str]:
    """Return set of ST codes from the most recent snapshot with date <= at."""
    d = _root()
    ts_at = pd.Timestamp(at)
    candidates = []
    for p in d.glob("*.parquet"):
        try:
            snap = pd.Timestamp(datetime.strptime(p.stem, "%Y%m%d"))
        except ValueError:
            continue
        if snap <= ts_at:
            candidates.append((snap, p))
    if not candidates:
        raise RefdataNotAvailable(f"no st snapshot <= {at}")
    _, latest = max(candidates, key=lambda x: x[0])
    df = pd.read_parquet(latest)
    return set(df.loc[df["is_st"], "code"].astype(str))
