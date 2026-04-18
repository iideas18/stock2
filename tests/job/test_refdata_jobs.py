from datetime import date
from unittest.mock import patch

import pandas as pd

from instock.job import (
    refdata_industry_snapshot_job as ind_job,
    refdata_listing_dates_job as lst_job,
    refdata_st_snapshot_job as st_job,
)
from instock.refdata import industry, listing, st


def test_industry_job_writes_snapshot(tmp_refdata_root):
    fake_rows = pd.DataFrame({
        "code": ["000001", "600000"],
        "industry": ["银行", "银行"],
    })
    with patch.object(ind_job, "_fetch", return_value=fake_rows):
        ind_job.run(snapshot_date=date(2026, 4, 15))
    got = industry.read_industry_map(date(2026, 4, 16))
    assert got == {"000001": "银行", "600000": "银行"}


def test_listing_job_upserts(tmp_refdata_root):
    fake = pd.DataFrame({
        "code": ["000001"],
        "listing_date": pd.to_datetime(["1991-04-03"]),
    })
    with patch.object(lst_job, "_fetch", return_value=fake):
        lst_job.run(codes=["000001"])
    got = listing.read_listing_dates()
    assert got["000001"] == date(1991, 4, 3)


def test_st_job_writes_snapshot(tmp_refdata_root):
    fake = pd.DataFrame({
        "code": ["000001"],
        "is_st": [True],
    })
    with patch.object(st_job, "_fetch", return_value=fake):
        st_job.run(snapshot_date=date(2026, 4, 15))
    got = st.read_st_flags(date(2026, 4, 16))
    assert got == {"000001"}
