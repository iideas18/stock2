from __future__ import annotations
from typing import Mapping
import pandas as pd


def winsorize(
    df: pd.DataFrame, lower: float = 0.01, upper: float = 0.99
) -> pd.DataFrame:
    """Clip per-date value distribution to [lower-quantile, upper-quantile]."""
    def _clip(g: pd.DataFrame) -> pd.DataFrame:
        lo = g["value"].quantile(lower)
        hi = g["value"].quantile(upper)
        g = g.copy()
        g["value"] = g["value"].clip(lo, hi)
        return g
    return df.groupby("date", group_keys=False).apply(_clip)


def neutralize(
    df: pd.DataFrame,
    industry_map: Mapping[str, str] | None = None,
    mcap: pd.Series | None = None,
) -> pd.DataFrame:
    """Subtract per-date industry (and optionally mcap-bucket) mean.

    For the MVP we implement industry-mean neutralization only; mcap is
    accepted for API stability but ignored unless provided.
    """
    if not industry_map:
        return df
    out = df.copy()
    out["_ind"] = out["code"].map(industry_map)
    group = out.groupby(["date", "_ind"])["value"].transform("mean")
    out["value"] = out["value"] - group
    return out.drop(columns=["_ind"])


def zscore(df: pd.DataFrame) -> pd.DataFrame:
    def _zs(g: pd.DataFrame) -> pd.DataFrame:
        mean = g["value"].mean()
        std = g["value"].std(ddof=0)
        g = g.copy()
        g["value"] = 0.0 if std == 0 else (g["value"] - mean) / std
        return g
    return df.groupby("date", group_keys=False).apply(_zs)


def default_pipeline(
    df: pd.DataFrame,
    industry_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """winsorize -> (optional) industry neutralize -> zscore."""
    step1 = winsorize(df)
    step2 = neutralize(step1, industry_map=industry_map)
    step3 = zscore(step2)
    return step3
