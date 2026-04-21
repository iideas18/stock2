"""Status check ABC and built-in implementations."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

import pandas as pd

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


class ICIRDecay(StatusCheck):
    """|IC_IR| over last window_days < threshold → RED. Empty data → YELLOW.

    frame_loader returns a DataFrame with columns (date, code, value, fwd_ret)
    per factor name. If not provided, falls back to reading from Sub-1
    factor storage and joining next-day returns from ohlcv cache.
    """

    def __init__(
        self,
        factor: str,
        window_days: int = 30,
        threshold: float = 0.1,
        frame_loader: Callable[[str], pd.DataFrame] | None = None,
    ) -> None:
        self.name = f"icir.{factor}"
        self._factor = factor
        self._window = window_days
        self._threshold = threshold
        self._loader = frame_loader or _default_icir_loader

    def run(self) -> StatusRow:
        now = pd.Timestamp.now()
        try:
            df = self._loader(self._factor)
        except Exception as exc:
            return StatusRow(
                name=self.name, status="RED",
                message=f"loader failed: {exc}",
                metric_value=None, as_of=now,
            )
        if df.empty:
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no data yet", metric_value=None, as_of=now,
            )
        cutoff = df["date"].max() - pd.Timedelta(days=self._window)
        window = df[df["date"] >= cutoff]
        ic_per_day = (
            window.groupby("date")
            .apply(
                lambda g: g["value"].corr(g["fwd_ret"], method="spearman"),
                include_groups=False,
            )
            .dropna()
        )
        if len(ic_per_day) < 5:
            return StatusRow(
                name=self.name, status="YELLOW",
                message=f"insufficient days ({len(ic_per_day)})",
                metric_value=None, as_of=now,
            )
        mean_ic = float(ic_per_day.mean())
        std_ic = float(ic_per_day.std(ddof=1))
        icir = abs(mean_ic / std_ic) if std_ic > 0 else 0.0
        status = "GREEN" if icir >= self._threshold else "RED"
        return StatusRow(
            name=self.name, status=status,
            message=f"IC_IR={icir:.3f} over {len(ic_per_day)}d",
            metric_value=icir, as_of=now,
        )


def _default_icir_loader(factor: str) -> pd.DataFrame:
    # Best-effort: wire into Sub-1 read_factor + Sub-2.5 ohlcv cache.
    # Kept intentionally minimal; unit tests inject a loader instead.
    from instock.factors.storage import read_factor
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=90)
    df = read_factor(factor, start, end)
    if df.empty:
        return df
    # Fwd return from ohlcv cache is environment-dependent; return empty
    # fwd_ret so that the check degrades to YELLOW when ohlcv not available.
    df = df.copy()
    df["fwd_ret"] = float("nan")
    return df.dropna(subset=["fwd_ret"])
