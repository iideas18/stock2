"""Refresh listing-dates table.

Usage: python -m instock.job.refdata_listing_dates_job [CODE [CODE ...]]
With no args: tries all codes currently in CSI-300 membership.
"""
from __future__ import annotations

import logging
import sys
from datetime import date

import pandas as pd

from instock.datasource.registry import get_source
from instock.refdata.listing import upsert_listing_dates

log = logging.getLogger(__name__)


def _fetch(codes: list[str]) -> pd.DataFrame:
    src = get_source()
    rows = []
    for c in codes:
        try:
            info = src._fetch_individual_info(c)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            log.warning("individual_info %s failed: %s", c, exc)
            continue
        if info is None or info.empty:
            continue
        m = dict(zip(info["item"].astype(str), info["value"].astype(str)))
        ld = m.get("上市时间") or m.get("上市日期")
        if not ld:
            continue
        try:
            lt = pd.to_datetime(ld)
        except Exception:  # noqa: BLE001
            continue
        rows.append({"code": c, "listing_date": lt})
    return pd.DataFrame(rows)


def run(codes: list[str] | None = None) -> None:
    if codes is None:
        src = get_source()
        codes = src.get_index_member("000300", date.today())
    df = _fetch(codes)
    if df.empty:
        log.warning("listing fetch produced no rows")
        return
    upsert_listing_dates(df)
    log.info("listing upsert: %s rows", len(df))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = sys.argv[1:]
    run(args if args else None)
