"""Real-akshare smoke for Sub-2.5 refdata pipeline.

Runs only when INSTOCK_SUB25_SMOKE=1 is set. Pulls tiny real samples to
verify akshare's columns still match our fetchers.
"""
from __future__ import annotations

import os
import sys
from datetime import date

import pandas as pd
import pytest

from instock.datasource.registry import get_source
from instock.refdata.industry import write_industry_snapshot, read_industry_map
from instock.refdata.listing import upsert_listing_dates, read_listing_dates
from instock.refdata.st import write_st_snapshot, read_st_flags
from instock.refdata.ohlcv_store import OhlcvPanelStore


pytestmark = pytest.mark.skipif(
    os.environ.get("INSTOCK_SUB25_SMOKE") != "1",
    reason="requires INSTOCK_SUB25_SMOKE=1 (live akshare)",
)


def test_industry_one_board(tmp_refdata_root):
    from instock.job.refdata_industry_snapshot_job import _fetch
    df = _fetch()
    assert not df.empty, "expected industry rows from akshare"
    df["snapshot_date"] = pd.Timestamp(date.today())
    df = df.head(50)
    write_industry_snapshot(df)
    got = read_industry_map(date.today())
    assert len(got) >= 1
    print(f"[smoke] industry rows: {len(got)}", file=sys.stderr)


def test_listing_one_code(tmp_refdata_root):
    from instock.job.refdata_listing_dates_job import _fetch
    df = _fetch(["000001"])
    assert not df.empty
    upsert_listing_dates(df)
    got = read_listing_dates()
    assert "000001" in got
    print(f"[smoke] listing 000001: {got['000001']}", file=sys.stderr)


def test_st_snapshot(tmp_refdata_root):
    from instock.job.refdata_st_snapshot_job import _fetch
    df = _fetch()
    df["snapshot_date"] = pd.Timestamp(date.today())
    if df.empty:
        pytest.skip("ST list empty; nothing to write")
    write_st_snapshot(df)
    got = read_st_flags(date.today())
    print(f"[smoke] ST codes: {len(got)}", file=sys.stderr)


def test_ohlcv_store_one_code(tmp_ohlcv_root):
    source = get_source()
    store = OhlcvPanelStore(source=source)
    df = store.get_panel(
        ["000001"], date(2026, 4, 1), date(2026, 4, 10)
    )
    assert not df.empty
    assert (df["code"] == "000001").all()
    print(f"[smoke] ohlcv rows: {len(df)}", file=sys.stderr)
