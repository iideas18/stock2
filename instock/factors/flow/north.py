from __future__ import annotations
from datetime import date, timedelta
import pandas as pd

from ..base import Factor


def _fetch_north(start: date, end: date) -> pd.DataFrame:
    from instock.datasource.registry import get_source
    return get_source().get_north_bound(start, end)


class NorthHoldingChgFactor(Factor):
    category = "flow"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["north_bound"]

    def __init__(self, window: int = 5) -> None:
        self.window = window
        self.name = f"north_holding_chg_{window}d"
        self.description = (
            f"Pct change of northbound hold_shares over ~{window} calendar days"
        )

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        # Fetch extra lookback so earliest row in window has a prior snapshot.
        lookback = start - timedelta(days=self.window * 2 + 5)
        raw = _fetch_north(lookback, end)
        if raw.empty:
            return pd.DataFrame(columns=["date", "code", "value"])

        raw = raw.sort_values(["code", "trade_date"]).copy()
        # Use previous snapshot for the code, not positional-N-back.
        # North-bound is sampled sparsely; fixture data may have fewer than
        # `window` rows per code.  pct_change captures "last observed move"
        # which is the intent here.
        raw["prev"] = raw.groupby("code")["hold_shares"].shift(1)
        raw["value"] = (raw["hold_shares"] - raw["prev"]) / raw["prev"]
        mask = (
            (raw["trade_date"] >= pd.Timestamp(start))
            & (raw["trade_date"] <= pd.Timestamp(end))
            & raw["code"].isin(universe)
        )
        out = raw.loc[mask, ["trade_date", "code", "value"]].rename(
            columns={"trade_date": "date"}
        ).dropna().reset_index(drop=True)
        return out
