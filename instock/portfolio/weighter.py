"""Weighter: assign portfolio weights to selected codes.

MVP: EqualWeighter. Context carries data future weighters (inverse-vol,
market-cap, score-weighted) will need; MVP ignores all of it.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class WeighterContext:
    price_panel: pd.DataFrame
    mcap_panel: pd.DataFrame | None
    scores: pd.Series


class Weighter(ABC):
    @abstractmethod
    def weigh(
        self, selected: list[str], context: WeighterContext
    ) -> pd.Series:
        """Returns Series indexed by code, weights sum to 1.0
        (empty Series when selected is empty)."""


class EqualWeighter(Weighter):
    def weigh(self, selected, context):
        n = len(selected)
        if n == 0:
            return pd.Series(dtype=float)
        return pd.Series(1.0 / n, index=selected)
