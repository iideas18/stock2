"""Refresh ST-flag snapshot.

Usage: python -m instock.job.refdata_st_snapshot_job [YYYY-MM-DD]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

import pandas as pd

from instock.datasource.registry import get_source
from instock.refdata.st import write_st_snapshot

log = logging.getLogger(__name__)


def _fetch() -> pd.DataFrame:
    src = get_source()
    raw = src._fetch_st_snapshot()  # type: ignore[attr-defined]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["code", "is_st"])
    code_col = "代码" if "代码" in raw.columns else raw.columns[0]
    codes = raw[code_col].astype(str).str.zfill(6)
    return pd.DataFrame({"code": codes.tolist(), "is_st": [True] * len(codes)})


def run(snapshot_date: date) -> None:
    df = _fetch()
    df["snapshot_date"] = pd.Timestamp(snapshot_date)
    if df.empty:
        log.warning("ST fetch produced no rows")
        return
    write_st_snapshot(df)
    log.info("ST snapshot: %s rows", len(df))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = sys.argv[1:]
    d = (
        datetime.strptime(args[0], "%Y-%m-%d").date()
        if args else date.today()
    )
    run(d)
