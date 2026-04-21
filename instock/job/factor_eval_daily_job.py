"""Daily factor-evaluation job.

For each registered factor, read recent values, join next-day returns
from the ohlcv cache, run the evaluator, and write an HTML report that
the research portal's FactorDetailHandler iframe consumes.

Usage:
    python -m instock.job.factor_eval_daily_job [--window-days N]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

from instock.factors import storage
from instock.factors.evaluator import evaluate_from_frames
from instock.factors.registry import get_all
from instock.factors.report import write_factor_report

log = logging.getLogger(__name__)


def _load_returns(
    dates_min: pd.Timestamp,
    dates_max: pd.Timestamp,
    ohlcv_root: Path | None = None,
) -> pd.DataFrame:
    """Build (date, code, ret) where ret[T] = close(T+1)/close(T) - 1."""
    if ohlcv_root is None:
        ohlcv_root = Path(os.environ.get("INSTOCK_OHLCV_ROOT", "data/ohlcv"))
    frames = []
    for y in range(int(dates_min.year), int(dates_max.year) + 2):
        p = ohlcv_root / f"{y}.parquet"
        if p.exists():
            frames.append(pd.read_parquet(p, columns=["date", "code", "close"]))
    if not frames:
        return pd.DataFrame(columns=["date", "code", "ret"])
    ohlcv = pd.concat(frames, ignore_index=True)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv.sort_values(["code", "date"]).reset_index(drop=True)
    ohlcv["fwd_close"] = ohlcv.groupby("code")["close"].shift(-1)
    ohlcv["ret"] = ohlcv["fwd_close"] / ohlcv["close"] - 1.0
    return ohlcv[["date", "code", "ret"]].dropna(subset=["ret"]).reset_index(
        drop=True
    )


def evaluate_and_report(name: str, window_days: int = 90) -> Path | None:
    """Evaluate one factor and write its HTML report. Returns the path, or
    None if insufficient data."""
    end = pd.Timestamp.now().normalize()
    start = end - pd.Timedelta(days=window_days)
    factor_df = storage.read_factor(name, start, end)
    if factor_df.empty:
        log.warning("factor %s: no stored values in window", name)
        return None
    factor_df = factor_df.copy()
    factor_df["date"] = pd.to_datetime(factor_df["date"])
    returns_df = _load_returns(factor_df["date"].min(), factor_df["date"].max())
    if returns_df.empty:
        log.warning("factor %s: ohlcv cache empty, skipping report", name)
        return None
    report = evaluate_from_frames(factor_df, returns_df)
    out = write_factor_report(name, report)
    log.info("factor %s: report written to %s", name, out)
    return out


def run(window_days: int = 90) -> dict[str, Exception]:
    from instock.factors.bootstrap import register_default_factors
    register_default_factors()
    errors: dict[str, Exception] = {}
    for name in get_all().keys():
        try:
            evaluate_and_report(name, window_days=window_days)
        except Exception as exc:
            log.exception("factor %s evaluation failed", name)
            errors[name] = exc
    return errors


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-days", type=int, default=90)
    args = parser.parse_args()
    errs = run(window_days=args.window_days)
    sys.exit(1 if errs else 0)
