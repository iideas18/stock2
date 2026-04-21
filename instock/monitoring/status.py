"""Status check ABC and built-in implementations."""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
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
    """Read factor values and join next-day returns from the ohlcv cache.

    Fwd return is computed per code as the pct change from the factor's
    value date close to the next trading day's close, read from the cached
    Parquet partitions at `<INSTOCK_OHLCV_ROOT>/<year>.parquet`. Missing
    ohlcv → empty DataFrame so the check degrades to YELLOW.
    """
    from instock.factors.storage import read_factor

    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=90)
    df = read_factor(factor, start, end)
    if df.empty:
        return df

    ohlcv_root = Path(os.environ.get("INSTOCK_OHLCV_ROOT", "data/ohlcv"))
    years = range(int(df["date"].min().year), int(df["date"].max().year) + 2)
    frames = []
    for y in years:
        p = ohlcv_root / f"{y}.parquet"
        if p.exists():
            frames.append(pd.read_parquet(p, columns=["date", "code", "close"]))
    if not frames:
        return df.iloc[0:0].assign(fwd_ret=pd.Series(dtype="float64"))

    ohlcv = pd.concat(frames, ignore_index=True)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv.sort_values(["code", "date"]).reset_index(drop=True)
    ohlcv["fwd_close"] = ohlcv.groupby("code")["close"].shift(-1)
    ohlcv["fwd_ret"] = ohlcv["fwd_close"] / ohlcv["close"] - 1.0
    fwd = ohlcv[["date", "code", "fwd_ret"]].dropna(subset=["fwd_ret"])

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    merged = df.merge(fwd, on=["date", "code"], how="inner")
    return merged


class DataSourceRate(StatusCheck):
    """Success rate of recent data source calls. Missing log = YELLOW."""

    def __init__(
        self,
        source: str,
        window_hours: float = 24.0,
        threshold: float = 0.9,
        log_path: Path | None = None,
    ) -> None:
        self.name = f"datasource.{source}"
        self._source = source
        self._window_hours = window_hours
        self._threshold = threshold
        self._log_path = (
            Path(log_path) if log_path is not None
            else Path(os.environ.get("INSTOCK_API_CALL_LOG",
                                     "data/log/api_calls.parquet"))
        )

    def run(self) -> StatusRow:
        now = pd.Timestamp.now()
        if not self._log_path.exists():
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no data yet", metric_value=None, as_of=now,
            )
        try:
            df = pd.read_parquet(self._log_path)
        except Exception as exc:
            return StatusRow(
                name=self.name, status="RED",
                message=f"log load failed: {exc}",
                metric_value=None, as_of=now,
            )
        cutoff = now - pd.Timedelta(hours=self._window_hours)
        window = df[(df["source"] == self._source) & (df["ts"] >= cutoff)]
        if window.empty:
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no calls in window",
                metric_value=None, as_of=now,
            )
        rate = float(window["ok"].mean())
        status = "GREEN" if rate >= self._threshold else "RED"
        return StatusRow(
            name=self.name, status=status,
            message=f"ok_rate={rate:.3f} over {len(window)} calls",
            metric_value=rate, as_of=now,
        )
