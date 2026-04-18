"""FactorCombiner: wide factor panel -> single combined score per (date, code).

MVP implementation: EqualRankCombiner — per-date cross-sectional percentile
rank of each factor, then arithmetic mean.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class FactorCombiner(ABC):
    @abstractmethod
    def combine(self, factor_panel: pd.DataFrame) -> pd.DataFrame:
        """Combine factors into one score.

        Args:
            factor_panel: long-form with columns [date, code, <factor1>,
                <factor2>, ...]. Factor columns are whatever non-key
                columns are present.

        Returns:
            DataFrame[date, code, score].
        """


class EqualRankCombiner(FactorCombiner):
    """Per-date rank(pct=True, method='average') on every factor, then mean.

    Any row with a NaN in any factor column is dropped before ranking.
    """

    def combine(self, factor_panel: pd.DataFrame) -> pd.DataFrame:
        key_cols = ["date", "code"]
        factor_cols = [c for c in factor_panel.columns if c not in key_cols]
        if not factor_cols:
            raise ValueError("factor_panel has no factor columns")

        df = factor_panel.dropna(subset=factor_cols).copy()
        if df.empty:
            return pd.DataFrame(columns=["date", "code", "score"])

        for c in factor_cols:
            df[f"_rk_{c}"] = df.groupby("date")[c].rank(
                pct=True, method="average"
            )
        rank_cols = [f"_rk_{c}" for c in factor_cols]
        df["score"] = df[rank_cols].mean(axis=1)
        return df[["date", "code", "score"]].reset_index(drop=True)
