"""Industry-map snapshot storage and as-of reader."""
from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .schemas import INDUSTRY_SNAPSHOT_SCHEMA, RefdataNotAvailable


def _root() -> Path:
    root = Path(os.environ.get("INSTOCK_REFDATA_ROOT", "data/refdata"))
    d = root / "industry"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_industry_snapshot(df: pd.DataFrame) -> None:
    """Write one snapshot file keyed by its snapshot_date (YYYYMMDD).

    All rows in `df` are expected to share the same snapshot_date (one
    file per snapshot day). The first row's snapshot_date determines the
    output filename.
    """
    if df.empty:
        return
    df = INDUSTRY_SNAPSHOT_SCHEMA.validate(df.copy())
    snap = df["snapshot_date"].iloc[0]
    path = _root() / f"{snap.strftime('%Y%m%d')}.parquet"
    df.to_parquet(path, index=False)


def read_industry_map(at: date) -> dict[str, str]:
    """Load the most recent snapshot with snapshot_date <= at.

    Raises RefdataNotAvailable if no such file exists.
    """
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
        raise RefdataNotAvailable(
            f"no industry snapshot on or before {at}"
        )
    _, latest = max(candidates, key=lambda x: x[0])
    df = pd.read_parquet(latest)
    return dict(zip(df["code"].astype(str), df["industry"].astype(str)))
