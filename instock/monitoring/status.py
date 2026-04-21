"""Status check ABC and built-in implementations."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

log = logging.getLogger(__name__)

_STATUSES = ("GREEN", "YELLOW", "RED", "ACK")


@dataclass(frozen=True)
class StatusRow:
    name: str
    status: str
    message: str
    metric_value: float | None
    as_of: pd.Timestamp

    def __post_init__(self) -> None:
        if self.status not in _STATUSES:
            raise ValueError(f"invalid status: {self.status!r}")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "metric_value": self.metric_value,
            "as_of": self.as_of.isoformat() if self.as_of is not None else None,
        }


class StatusCheck(ABC):
    name: str

    @abstractmethod
    def run(self) -> StatusRow: ...


class ArtifactFreshness(StatusCheck):
    """GREEN if latest as-of date within max_age_days_yellow,
    YELLOW if within max_age_days_red, RED otherwise. None loader = YELLOW."""

    def __init__(
        self,
        name: str,
        loader: Callable[[], pd.Timestamp | None],
        max_age_days_yellow: float,
        max_age_days_red: float,
    ) -> None:
        self.name = name
        self._loader = loader
        self._y = max_age_days_yellow
        self._r = max_age_days_red

    def run(self) -> StatusRow:
        now = pd.Timestamp.now()
        try:
            ts = self._loader()
        except Exception as exc:
            return StatusRow(
                name=self.name, status="RED",
                message=f"loader failed: {exc}",
                metric_value=None, as_of=now,
            )
        if ts is None:
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no data yet",
                metric_value=None, as_of=now,
            )
        age_days = (now - ts).total_seconds() / 86400.0
        if age_days <= self._y:
            status = "GREEN"
        elif age_days <= self._r:
            status = "YELLOW"
        else:
            status = "RED"
        return StatusRow(
            name=self.name, status=status,
            message=f"age={age_days:.2f}d (latest={ts.date()})",
            metric_value=age_days, as_of=now,
        )
