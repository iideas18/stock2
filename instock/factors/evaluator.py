from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np
import pandas as pd


@dataclass
class FactorReport:
    ic_series: pd.Series
    rank_ic_series: pd.Series
    ic_mean: float
    rank_ic_mean: float
    ic_ir: float
    group_returns: pd.DataFrame
    decay: dict[int, float]
    turnover: float


def evaluate_from_frames(
    factor_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    n_groups: int = 10,
    decay_lags: tuple[int, ...] = (1, 5, 10, 20),
) -> FactorReport:
    """Pure-compute evaluator: takes factor values and next-period returns."""
    merged = factor_df.merge(returns_df, on=["date", "code"], how="inner")
    merged = merged.dropna(subset=["value", "ret"])

    def _ic(g):
        if len(g) < 2 or g["value"].std() == 0 or g["ret"].std() == 0:
            return np.nan
        return g["value"].corr(g["ret"])

    def _rank_ic(g):
        if len(g) < 2:
            return np.nan
        return g["value"].rank().corr(g["ret"].rank())

    ic_series = merged.groupby("date").apply(_ic)
    rank_ic_series = merged.groupby("date").apply(_rank_ic)
    ic_mean = float(ic_series.mean())
    rank_ic_mean = float(rank_ic_series.mean())
    ic_std = float(ic_series.std(ddof=0))
    ic_ir = 0.0 if ic_std == 0 else ic_mean / ic_std

    def _assign_group(g):
        g = g.copy()
        g["grp"] = pd.qcut(g["value"], q=n_groups, labels=False, duplicates="drop")
        return g
    grouped = merged.groupby("date", group_keys=False).apply(_assign_group)
    group_returns = (
        grouped.groupby(["date", "grp"])["ret"].mean().unstack().mean(axis=0)
    ).to_frame("mean_return")

    decay = {lag: float(ic_mean) if lag == 1 else float("nan") for lag in decay_lags}

    top = grouped[grouped["grp"] == grouped["grp"].max()]
    top_per_date = top.groupby("date")["code"].apply(set)
    diffs = []
    prev = None
    for s in top_per_date:
        if prev is not None and len(prev) > 0:
            diffs.append(len(s - prev) / len(prev))
        prev = s
    turnover = float(np.mean(diffs)) if diffs else 0.0

    return FactorReport(
        ic_series=ic_series,
        rank_ic_series=rank_ic_series,
        ic_mean=ic_mean,
        rank_ic_mean=rank_ic_mean,
        ic_ir=ic_ir,
        group_returns=group_returns,
        decay=decay,
        turnover=turnover,
    )
