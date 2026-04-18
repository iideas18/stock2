"""Universe filters: shrink the candidate code list before selection.

MVP rules only use information already present in OHLCV.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class FilterContext:
    """Shared data passed to every filter. OHLCV must span back far enough
    for history-based filters (e.g. NewListingFilter.min_days)."""
    ohlcv_panel: pd.DataFrame


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
