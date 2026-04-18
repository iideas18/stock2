import pandas as pd
import pytest
from instock.datasource import schemas


def _ohlcv_row():
    return {
        "date": pd.Timestamp("2024-01-02"),
        "code": "600519",
        "open": 1700.0, "high": 1720.0, "low": 1695.0, "close": 1710.0,
        "volume": 1_000_000.0, "amount": 1.7e9,
    }


def test_ohlcv_schema_accepts_valid():
    df = pd.DataFrame([_ohlcv_row()])
    validated = schemas.OHLCV_SCHEMA.validate(df)
    assert len(validated) == 1


def test_ohlcv_schema_rejects_bad_code():
    bad = _ohlcv_row() | {"code": "ABC"}
    df = pd.DataFrame([bad])
    with pytest.raises(Exception):
        schemas.OHLCV_SCHEMA.validate(df)


def test_ohlcv_schema_rejects_nonpositive_price():
    bad = _ohlcv_row() | {"close": 0.0}
    df = pd.DataFrame([bad])
    with pytest.raises(Exception):
        schemas.OHLCV_SCHEMA.validate(df)


def test_ohlcv_schema_unique_key():
    row = _ohlcv_row()
    df = pd.DataFrame([row, row])
    with pytest.raises(Exception):
        schemas.OHLCV_SCHEMA.validate(df)


def test_fundamentals_pit_schema_requires_announcement_date():
    df = pd.DataFrame([{
        "code": "600519",
        "report_period": pd.Timestamp("2023-09-30"),
        "announcement_date": pd.Timestamp("2023-10-28"),
        "revenue": 1.0e10,
    }])
    validated = schemas.FUNDAMENTALS_PIT_SCHEMA.validate(df)
    assert "announcement_date" in validated.columns


def test_lhb_schema_smoke():
    df = pd.DataFrame([{
        "trade_date": pd.Timestamp("2024-01-02"),
        "code": "600519",
        "seat": "中信证券北京建国路",
        "buy_amount": 1.0e8,
        "sell_amount": 5.0e7,
        "reason": "日涨幅偏离值达7%",
    }])
    schemas.LHB_SCHEMA.validate(df)
