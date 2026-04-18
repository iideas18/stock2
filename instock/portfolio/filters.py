"""Universe filters: shrink the candidate code list before selection.

MVP rules only use information already present in OHLCV.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass
class FilterContext:
    """Shared data passed to every filter. OHLCV must span back far enough
    for history-based filters (e.g. NewListingFilter.min_days).

    listing_dates and st_flags are Sub-2.5 additions; filters must tolerate
    None and fall back gracefully with a single warning.
    """
    ohlcv_panel: pd.DataFrame
    listing_dates: dict[str, date] | None = None
    st_flags: set[str] | None = None


class UniverseFilter(ABC):
    @abstractmethod
    def apply(
        self,
        codes: list[str],
        at: pd.Timestamp,
        context: FilterContext,
    ) -> list[str]:
        ...


class SuspendedFilter(UniverseFilter):
    """Drop codes with no OHLCV row on `at` or with volume == 0 on `at`."""

    def apply(self, codes, at, context):
        if not codes:
            return []
        panel = context.ohlcv_panel
        on_day = panel[panel["date"] == at]
        active = set(on_day.loc[on_day["volume"] > 0, "code"].astype(str))
        return [c for c in codes if c in active]


class NewListingFilter(UniverseFilter):
    """Drop codes whose earliest OHLCV date is < `min_days` before `at`."""

    def __init__(self, min_days: int = 60) -> None:
        self.min_days = min_days

    def apply(self, codes, at, context):
        if not codes:
            return []
        panel = context.ohlcv_panel
        if panel.empty:
            return []
        first_seen = panel.groupby("code")["date"].min()
        cutoff = at - pd.Timedelta(days=self.min_days)
        ok = set(first_seen[first_seen <= cutoff].index.astype(str))
        return [c for c in codes if c in ok]


class FilterChain:
    """Apply a list of filters in order. Short-circuits on empty intermediate."""

    def __init__(self, filters: list[UniverseFilter]) -> None:
        self.filters = filters

    def apply(self, codes, at, context):
        out = list(codes)
        for f in self.filters:
            out = f.apply(out, at, context)
            if not out:
                return []
        return out


class STFilter(UniverseFilter):
    """Drop codes flagged as ST in FilterContext.st_flags.

    If st_flags is None: warn once and no-op (keeps all codes).
    """

    def __init__(self) -> None:
        self._warned = False

    def apply(self, codes, at, context):
        if context.st_flags is None:
            if not self._warned:
                import logging
                logging.getLogger(__name__).warning(
                    "STFilter: context.st_flags is None; no-op"
                )
                self._warned = True
            return list(codes)
        flags = context.st_flags
        return [c for c in codes if c not in flags]
