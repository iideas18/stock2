from __future__ import annotations
from datetime import date
import pandas as pd

from ..base import Factor


def _fetch_pit(codes: list[str], fields: list[str]) -> pd.DataFrame:
    """Pull PIT financials for the given codes. Tests patch this."""
    from instock.datasource.registry import get_source
    src = get_source()
    frames = [src.get_fundamentals_pit(c, fields) for c in codes]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


class _PITFactorBase(Factor):
    category = "fundamental"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["fundamentals_pit"]
    field: str = ""

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        raw = _fetch_pit(universe, [self.field])
        if raw.empty:
            return pd.DataFrame(columns=["date", "code", "value"])
        dates = pd.date_range(start, end, freq="D")
        out = []
        for d in dates:
            sub = raw[raw["announcement_date"] <= d]
            if sub.empty:
                continue
            latest = (
                sub.sort_values(["code", "report_period"])
                   .groupby("code")
                   .tail(1)
            )
            for _, row in latest.iterrows():
                out.append({
                    "date": d, "code": row["code"],
                    "value": row[self.field],
                })
        return pd.DataFrame(out)


class PEFactor(_PITFactorBase):
    name = "pe_ttm"
    description = "PE TTM, PIT"
    field = "pe"


class PBFactor(_PITFactorBase):
    name = "pb"
    description = "Price/Book, PIT"
    field = "pb"


class ROEFactor(_PITFactorBase):
    name = "roe_ttm"
    description = "Return on Equity TTM, PIT"
    field = "roe"
