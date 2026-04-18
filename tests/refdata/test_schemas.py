import pandas as pd
import pytest
import pandera.errors as pa_errors

from instock.refdata.schemas import (
    INDUSTRY_SNAPSHOT_SCHEMA,
    LISTING_DATES_SCHEMA,
    ST_SNAPSHOT_SCHEMA,
    RefdataNotAvailable,
)


def test_industry_schema_accepts_valid():
    df = pd.DataFrame({
        "code": ["000001", "600000"],
        "industry": ["银行", "银行"],
        "snapshot_date": pd.to_datetime(["2026-04-18", "2026-04-18"]),
    })
    INDUSTRY_SNAPSHOT_SCHEMA.validate(df)


def test_industry_schema_rejects_bad_code():
    df = pd.DataFrame({
        "code": ["1"],
        "industry": ["银行"],
        "snapshot_date": pd.to_datetime(["2026-04-18"]),
    })
    with pytest.raises(pa_errors.SchemaError):
        INDUSTRY_SNAPSHOT_SCHEMA.validate(df)


def test_listing_schema_accepts_valid():
    df = pd.DataFrame({
        "code": ["000001"],
        "listing_date": pd.to_datetime(["1991-04-03"]),
    })
    LISTING_DATES_SCHEMA.validate(df)


def test_listing_schema_rejects_duplicate_codes():
    df = pd.DataFrame({
        "code": ["000001", "000001"],
        "listing_date": pd.to_datetime(["1991-04-03", "1991-04-03"]),
    })
    with pytest.raises(pa_errors.SchemaError):
        LISTING_DATES_SCHEMA.validate(df)


def test_st_schema_accepts_valid():
    df = pd.DataFrame({
        "code": ["000001"],
        "is_st": [True],
        "snapshot_date": pd.to_datetime(["2026-04-18"]),
    })
    ST_SNAPSHOT_SCHEMA.validate(df)


def test_refdata_not_available_is_exception():
    assert issubclass(RefdataNotAvailable, Exception)
