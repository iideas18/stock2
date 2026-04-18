from __future__ import annotations
from datetime import date
import pandas as pd

from ..base import Factor


def _fetch_ohlcv(codes: list[str], start: date, end: date) -> pd.DataFrame:
    """Helper; tests patch this. Production path uses the DataSource."""
    from instock.datasource.registry import get_source
    src = get_source()
    frames = [src.get_ohlcv(c, start, end) for c in codes]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class MomentumFactor(Factor):
    category = "technical"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["ohlcv"]

    def __init__(self, window: int = 20) -> None:
        self.window = window
        self.name = f"mom_{window}d"
        self.description = f"{window}-day price momentum (close/close.shift-1)"

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        data = _fetch_ohlcv(universe, start, end)
        if data.empty:
            return pd.DataFrame(columns=["date", "code", "value"])
        data = data.sort_values(["code", "date"])
        data["value"] = (
            data.groupby("code")["close"]
                .transform(lambda s: s / s.shift(self.window) - 1.0)
        )
        return data[["date", "code", "value"]].dropna().reset_index(drop=True)
