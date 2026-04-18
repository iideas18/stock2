from __future__ import annotations
from datetime import date, timedelta
import pandas as pd

from ..base import Factor


def _fetch_lhb(start: date, end: date) -> pd.DataFrame:
    from instock.datasource.registry import get_source
    return get_source().get_lhb(start, end)


class LhbHeatFactor(Factor):
    category = "lhb"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["lhb"]

    def __init__(self, window: int = 30) -> None:
        self.window = window
        self.name = f"lhb_heat_{window}d"
        self.description = (
            f"Number of distinct LHB appearance dates in the last {window} days"
        )

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        window_start = start - timedelta(days=self.window)
        raw = _fetch_lhb(window_start, end)
        if raw.empty:
            return pd.DataFrame(columns=["date", "code", "value"])

        rows = []
        for d in pd.date_range(start, end, freq="D"):
            lo = d - pd.Timedelta(days=self.window)
            window_df = raw[(raw["trade_date"] > lo) & (raw["trade_date"] <= d)]
            counts = (
                window_df.drop_duplicates(["trade_date", "code"])
                         .groupby("code").size()
            )
            for c in universe:
                rows.append({
                    "date": d, "code": c, "value": float(counts.get(c, 0)),
                })
        return pd.DataFrame(rows)
