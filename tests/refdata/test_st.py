import pandas as pd
import pytest
from datetime import date

from instock.refdata.st import (
    write_st_snapshot,
    read_st_flags,
)
from instock.refdata.schemas import RefdataNotAvailable


def test_write_then_read(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001", "000002", "000003"],
        "is_st": [True, True, False],
        "snapshot_date": pd.to_datetime(
            ["2026-01-15", "2026-01-15", "2026-01-15"]
        ),
    })
    write_st_snapshot(df)
    got = read_st_flags(date(2026, 1, 15))
    assert got == {"000001", "000002"}


def test_picks_latest_snapshot(tmp_refdata_root):
    earlier = pd.DataFrame({
        "code": ["000001", "000002"],
        "is_st": [True, True],
        "snapshot_date": pd.to_datetime(["2026-01-15", "2026-01-15"]),
    })
    later = pd.DataFrame({
        "code": ["000001", "000002"],
        "is_st": [True, False],
        "snapshot_date": pd.to_datetime(["2026-02-15", "2026-02-15"]),
    })
    write_st_snapshot(earlier)
    write_st_snapshot(later)
    assert read_st_flags(date(2026, 2, 20)) == {"000001"}
    assert read_st_flags(date(2026, 1, 20)) == {"000001", "000002"}


def test_raises_when_no_snapshot(tmp_refdata_root):
    with pytest.raises(RefdataNotAvailable):
        read_st_flags(date(2026, 1, 15))
