"""Append-only Parquet store for monitoring rows.

Storage layout:
    <root>/_alerts/<YYYY-MM>.parquet

Each record appends to its month partition only, so total I/O per record
is O(rows in current month) rather than O(total history). Records carry
an int64 ``recorded_at`` nanosecond timestamp (via ``time.time_ns()``)
that is strictly monotonic within a process, giving deterministic
ordering across records recorded in the same millisecond.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd

from .status import StatusRow

log = logging.getLogger(__name__)

_COLUMNS = [
    "check_name", "status", "message", "metric_value",
    "as_of", "recorded_at",
]


def _partition_for(recorded_at_ns: int) -> str:
    return pd.Timestamp(recorded_at_ns, unit="ns").strftime("%Y-%m")


class AlertStateStore:
    def __init__(self, root: Path) -> None:
        self._dir = Path(root) / "_alerts"
        self._dir.mkdir(parents=True, exist_ok=True)
        # One-time migration from the legacy single-file layout.
        legacy = Path(root) / "_alerts.parquet"
        if legacy.exists() and not any(self._dir.glob("*.parquet")):
            df = pd.read_parquet(legacy)
            if not df.empty and "recorded_at" in df.columns:
                df = df.copy()
                df["recorded_at"] = df["recorded_at"].apply(
                    lambda v: (
                        int(pd.Timestamp(v).value)
                        if not isinstance(v, (int,)) else int(v)
                    )
                )
                df["_part"] = df["recorded_at"].apply(_partition_for)
                for part, g in df.groupby("_part"):
                    g.drop(columns=["_part"]).to_parquet(
                        self._dir / f"{part}.parquet", index=False,
                    )
            legacy.unlink()

    def _partition_path(self, recorded_at_ns: int) -> Path:
        return self._dir / f"{_partition_for(recorded_at_ns)}.parquet"

    def _read(self) -> pd.DataFrame:
        frames = []
        for p in sorted(self._dir.glob("*.parquet")):
            frames.append(pd.read_parquet(p))
        if not frames:
            return pd.DataFrame(columns=_COLUMNS)
        return pd.concat(frames, ignore_index=True)

    def record(self, row: StatusRow) -> None:
        # time.time_ns() is strictly monotonic within a process on Linux.
        recorded_at = time.time_ns()
        new = pd.DataFrame([{
            "check_name": row.name,
            "status": row.status,
            "message": row.message,
            "metric_value": row.metric_value,
            "as_of": row.as_of,
            "recorded_at": recorded_at,
        }], columns=_COLUMNS)
        new = new.astype({"metric_value": "float64", "recorded_at": "int64"})

        target = self._partition_path(recorded_at)
        if target.exists():
            existing = pd.read_parquet(target)
            df = pd.concat([existing, new], ignore_index=True)
        else:
            df = new
        df.to_parquet(target, index=False)

    def last_status(self, check_name: str) -> StatusRow | None:
        df = self._read()
        df = df[df["check_name"] == check_name]
        if df.empty:
            return None
        row = df.sort_values("recorded_at").iloc[-1]
        return StatusRow(
            name=row["check_name"],
            status=row["status"],
            message=row["message"] or "",
            metric_value=(
                float(row["metric_value"])
                if pd.notna(row["metric_value"]) else None
            ),
            as_of=pd.Timestamp(row["as_of"]),
        )

    def history(self, check_name: str, n: int = 30) -> pd.DataFrame:
        df = self._read()
        df = df[df["check_name"] == check_name].sort_values(
            "recorded_at", ascending=False
        ).head(n)
        return df.reset_index(drop=True)
