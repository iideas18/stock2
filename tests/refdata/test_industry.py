import pandas as pd
import pytest
from datetime import date

from instock.refdata.industry import (
    write_industry_snapshot,
    read_industry_map,
)
from instock.refdata.schemas import RefdataNotAvailable


def test_write_then_read_latest(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001", "600000"],
        "industry": ["银行", "银行"],
        "snapshot_date": pd.to_datetime(["2026-04-01", "2026-04-01"]),
    })
    write_industry_snapshot(df)
    got = read_industry_map(date(2026, 4, 15))
    assert got == {"000001": "银行", "600000": "银行"}


def test_read_picks_latest_before_at(tmp_refdata_root):
    df1 = pd.DataFrame({
        "code": ["000001"], "industry": ["旧"],
        "snapshot_date": pd.to_datetime(["2026-01-01"]),
    })
    df2 = pd.DataFrame({
        "code": ["000001"], "industry": ["新"],
        "snapshot_date": pd.to_datetime(["2026-04-01"]),
    })
    write_industry_snapshot(df1)
    write_industry_snapshot(df2)
    assert read_industry_map(date(2026, 3, 1)) == {"000001": "旧"}
    assert read_industry_map(date(2026, 5, 1)) == {"000001": "新"}


def test_read_raises_when_no_snapshot(tmp_refdata_root):
    with pytest.raises(RefdataNotAvailable):
        read_industry_map(date(2026, 4, 15))


def test_read_raises_when_all_snapshots_in_future(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001"], "industry": ["银行"],
        "snapshot_date": pd.to_datetime(["2026-12-31"]),
    })
    write_industry_snapshot(df)
    with pytest.raises(RefdataNotAvailable):
        read_industry_map(date(2026, 1, 1))
