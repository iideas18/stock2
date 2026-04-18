"""OHLCV panel cache with on-demand fill from IDataSource.

Layout: <INSTOCK_OHLCV_ROOT>/<year>.parquet, row keys (date, code).
Gap-detection uses IDataSource.get_trade_calendar — NOT date.range —
so weekends/holidays never trigger spurious refetch.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from instock.datasource.base import IDataSource

log = logging.getLogger(__name__)

_COLUMNS = [
    "date", "code", "open", "high", "low", "close", "volume", "amount"
]


class OhlcvPanelStore:
    def __init__(
        self, source: IDataSource, root: Optional[Path] = None
    ) -> None:
        self.source = source
        if root is None:
            root = Path(os.environ.get("INSTOCK_OHLCV_ROOT", "data/ohlcv"))
        root.mkdir(parents=True, exist_ok=True)
        self.root = root

    def _path(self, year: int) -> Path:
        return self.root / f"{year}.parquet"

    def _load_cache(
        self, start: date, end: date
    ) -> pd.DataFrame:
        frames = []
        for y in range(start.year, end.year + 1):
            p = self._path(y)
            if p.exists():
                frames.append(pd.read_parquet(p))
        if not frames:
            return pd.DataFrame(columns=_COLUMNS)
        df = pd.concat(frames, ignore_index=True)
        ts_s, ts_e = pd.Timestamp(start), pd.Timestamp(end)
        return df.loc[
            (df["date"] >= ts_s) & (df["date"] <= ts_e)
        ].reset_index(drop=True)

    def _write_cache(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        df = df.copy()
        df["year"] = df["date"].dt.year
        for year, group in df.groupby("year"):
            path = self._path(int(year))
            payload = group.drop(columns=["year"])
            if path.exists():
                old = pd.read_parquet(path)
                payload = pd.concat([old, payload], ignore_index=True)
            payload = (
                payload.drop_duplicates(
                    subset=["date", "code"], keep="last"
                )
                .sort_values(["date", "code"])
                .reset_index(drop=True)
            )
            payload.to_parquet(path, index=False)

    def _missing_codes(
        self, cached: pd.DataFrame, codes: list[str],
        cal: list[date],
    ) -> list[str]:
        """Codes whose cached rows don't cover every trade day."""
        if not cal:
            return []
        needed = len(cal)
        if cached.empty:
            return list(codes)
        counts = cached.groupby("code")["date"].nunique()
        missing = []
        for c in codes:
            if counts.get(c, 0) < needed:
                missing.append(c)
        return missing

    def get_panel(
        self, codes: list[str], start: date, end: date,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame(columns=_COLUMNS)
        cached = self._load_cache(start, end)
        cal = self.source.get_trade_calendar(start, end)
        missing = self._missing_codes(cached, codes, cal)
        new_frames = []
        for c in missing:
            try:
                frame = self.source.get_ohlcv(c, start, end, adjust=adjust)
                if not frame.empty:
                    new_frames.append(frame)
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "OhlcvPanelStore: fetch failed for %s: %s", c, exc
                )
        if new_frames:
            fetched = pd.concat(new_frames, ignore_index=True)
            self._write_cache(fetched)
        # Reload after possibly writing new rows.
        merged = self._load_cache(start, end)
        return merged[merged["code"].isin(codes)].reset_index(drop=True)

    def warm_cache(
        self, codes: list[str], start: date, end: date,
        adjust: str = "qfq",
    ) -> None:
        self.get_panel(codes, start, end, adjust=adjust)
