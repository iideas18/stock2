"""Listing-date storage (single parquet, upsert-by-code)."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd

from .schemas import LISTING_DATES_SCHEMA, RefdataNotAvailable


def _path() -> Path:
    root = Path(os.environ.get("INSTOCK_REFDATA_ROOT", "data/refdata"))
    root.mkdir(parents=True, exist_ok=True)
    return root / "listing_dates.parquet"


def upsert_listing_dates(df: pd.DataFrame) -> None:
    """Merge incoming (code, listing_date) into the single parquet file."""
    if df.empty:
        return
    df = LISTING_DATES_SCHEMA.validate(df.copy())
    path = _path()
    if path.exists():
        old = pd.read_parquet(path)
        merged = (
            pd.concat([old, df], ignore_index=True)
            .drop_duplicates(subset=["code"], keep="last")
            .sort_values("code")
            .reset_index(drop=True)
        )
    else:
        merged = df.sort_values("code").reset_index(drop=True)
    merged = LISTING_DATES_SCHEMA.validate(merged)
    merged.to_parquet(path, index=False)


def read_listing_dates() -> dict[str, date]:
    """Return code -> listing_date dict. Raise RefdataNotAvailable if no file."""
    path = _path()
    if not path.exists():
        raise RefdataNotAvailable(f"listing_dates.parquet not found at {path}")
    df = pd.read_parquet(path)
    return {
        str(c): d.date()
        for c, d in zip(df["code"], pd.to_datetime(df["listing_date"]))
    }
