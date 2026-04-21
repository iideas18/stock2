from __future__ import annotations
from typing import Mapping
import pandas as pd


def winsorize(
    df: pd.DataFrame, lower: float = 0.01, upper: float = 0.99
) -> pd.DataFrame:
    """Clip per-date value distribution to [lower-quantile, upper-quantile]."""
    def _lo(s: pd.Series) -> float:
        return s.quantile(lower)

    def _hi(s: pd.Series) -> float:
        return s.quantile(upper)

    lo = df.groupby("date")["value"].transform(_lo)
    hi = df.groupby("date")["value"].transform(_hi)
    out = df.copy()
    out["value"] = out["value"].clip(lo, hi)
    return out


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
    mean = df.groupby("date")["value"].transform("mean")
    std = df.groupby("date")["value"].transform(lambda s: s.std(ddof=0))
    out = df.copy()
    out["value"] = (df["value"] - mean) / std
    out["value"] = out["value"].where(std != 0, 0.0)
    return out


def default_pipeline(
    df: pd.DataFrame,
    industry_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """winsorize -> (optional) industry neutralize -> zscore."""
    step1 = winsorize(df)
    step2 = neutralize(step1, industry_map=industry_map)
    step3 = zscore(step2)
    return step3
