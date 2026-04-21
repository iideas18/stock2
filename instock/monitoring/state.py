"""Append-only Parquet store for monitoring rows."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .status import StatusRow

log = logging.getLogger(__name__)

_COLUMNS = [
    "check_name", "status", "message", "metric_value",
    "as_of", "recorded_at",
]


class AlertStateStore:
    def __init__(self, root: Path) -> None:
        self._path = Path(root) / "_alerts.parquet"
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _read(self) -> pd.DataFrame:
        if not self._path.exists():
            return pd.DataFrame(columns=_COLUMNS)
        return pd.read_parquet(self._path)

    def record(self, row: StatusRow) -> None:
        new = pd.DataFrame([{
            "check_name": row.name,
            "status": row.status,
            "message": row.message,
            "metric_value": row.metric_value,
            "as_of": row.as_of,
            "recorded_at": pd.Timestamp.now(),
        }], columns=_COLUMNS)
        existing = self._read()
        df = new if existing.empty else pd.concat(
            [existing, new], ignore_index=True
        )
        df.to_parquet(self._path, index=False)

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
