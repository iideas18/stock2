"""Refresh industry-map snapshot.

Usage: python -m instock.job.refdata_industry_snapshot_job [YYYY-MM-DD]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

import pandas as pd

from instock.datasource.registry import get_source
from instock.refdata.industry import write_industry_snapshot

log = logging.getLogger(__name__)


def _fetch() -> pd.DataFrame:
    """Return [code, industry] from akshare board lists."""
    src = get_source()
    names = src._fetch_board_industry_names()  # type: ignore[attr-defined]
    board_col = "板块名称" if "板块名称" in names.columns else names.columns[0]
    rows = []
    for board in names[board_col].astype(str).tolist():
        try:
            cons = src._fetch_board_industry_cons(board)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            log.warning("industry board %s fetch failed: %s", board, exc)
            continue
        code_col = "代码" if "代码" in cons.columns else cons.columns[1]
        codes = cons[code_col].astype(str).str.zfill(6)
        for c in codes:
            rows.append({"code": c, "industry": board})
    return pd.DataFrame(rows)


def run(snapshot_date: date) -> None:
    df = _fetch()
    if df.empty:
        log.warning("industry fetch produced no rows")
        return
    df = df.drop_duplicates(subset=["code"], keep="first")
    df["snapshot_date"] = pd.Timestamp(snapshot_date)
    write_industry_snapshot(df)
    log.info("industry snapshot: %s rows", len(df))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = sys.argv[1:]
    d = (
        datetime.strptime(args[0], "%Y-%m-%d").date()
        if args else date.today()
    )
    run(d)
