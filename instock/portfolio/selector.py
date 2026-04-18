"""Selector: pick a subset of the filtered universe based on per-code scores."""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Selector(ABC):
    @abstractmethod
    def select(self, scores: pd.Series, universe: list[str]) -> list[str]:
        """Args:
            scores: Series indexed by code, value = score on the current date.
            universe: filtered list of eligible codes.
        Returns:
            selected codes (subset of universe).
        """


def _ranked_within_universe(scores: pd.Series, universe: list[str]) -> pd.Series:
    inter = [c for c in universe if c in scores.index]
    if not inter:
        return pd.Series(dtype=float)
    return scores.loc[inter].sort_values(ascending=False)


class TopQuantileSelector(Selector):
    """Top `quantile` fraction of the (universe-intersected) scored codes.

    Always returns at least 1 code when the universe is non-empty.
    """

    def __init__(self, quantile: float) -> None:
        if not (0.0 < quantile <= 1.0):
            raise ValueError(f"quantile must be in (0, 1], got {quantile}")
        self.quantile = quantile

    def select(self, scores, universe):
        ranked = _ranked_within_universe(scores, universe)
        if ranked.empty:
            return []
        k = max(1, round(len(ranked) * self.quantile))
        return ranked.head(k).index.tolist()


class TopNSelector(Selector):
    """Top `n` of the (universe-intersected) scored codes."""

    def __init__(self, n: int) -> None:
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")
        self.n = n

    def select(self, scores, universe):
        ranked = _ranked_within_universe(scores, universe)
        if ranked.empty:
            return []
        return ranked.head(self.n).index.tolist()
