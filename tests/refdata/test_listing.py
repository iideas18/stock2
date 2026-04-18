import pandas as pd
import pytest
from datetime import date

from instock.refdata.listing import (
    upsert_listing_dates,
    read_listing_dates,
)
from instock.refdata.schemas import RefdataNotAvailable


def test_upsert_then_read(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001", "600000"],
        "listing_date": pd.to_datetime(["1991-04-03", "1999-11-10"]),
    })
    upsert_listing_dates(df)
    got = read_listing_dates()
    assert got == {
        "000001": date(1991, 4, 3),
        "600000": date(1999, 11, 10),
    }


def test_upsert_is_idempotent(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001"],
        "listing_date": pd.to_datetime(["1991-04-03"]),
    })
    upsert_listing_dates(df)
    upsert_listing_dates(df)
    got = read_listing_dates()
    assert len(got) == 1


def test_upsert_merges_new_codes(tmp_refdata_root):
    upsert_listing_dates(pd.DataFrame({
        "code": ["000001"], "listing_date": pd.to_datetime(["1991-04-03"]),
    }))
    upsert_listing_dates(pd.DataFrame({
        "code": ["600000"], "listing_date": pd.to_datetime(["1999-11-10"]),
    }))
    assert set(read_listing_dates().keys()) == {"000001", "600000"}


def test_upsert_updates_existing_code(tmp_refdata_root):
    upsert_listing_dates(pd.DataFrame({
        "code": ["000001"], "listing_date": pd.to_datetime(["1991-04-03"]),
    }))
    upsert_listing_dates(pd.DataFrame({
        "code": ["000001"], "listing_date": pd.to_datetime(["1991-04-04"]),
    }))
    assert read_listing_dates()["000001"] == date(1991, 4, 4)


def test_read_raises_when_empty(tmp_refdata_root):
    with pytest.raises(RefdataNotAvailable):
        read_listing_dates()
