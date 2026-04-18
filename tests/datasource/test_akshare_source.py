from datetime import date
import pandas as pd
import pytest

from instock.datasource.akshare_source import AkShareSource
from instock.datasource.base import SchemaValidationError


def _ak_ohlcv_fixture():
    return pd.DataFrame({
        "日期":    ["2024-01-02", "2024-01-03"],
        "开盘":    [1700.0, 1710.0],
        "收盘":    [1710.0, 1705.0],
        "最高":    [1720.0, 1715.0],
        "最低":    [1695.0, 1700.0],
        "成交量":  [1_000_000, 900_000],
        "成交额":  [1.7e9, 1.5e9],
    })


def test_get_ohlcv_normalises_columns(mocker):
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_zh_a_hist",
        return_value=_ak_ohlcv_fixture(),
    )
    src = AkShareSource()
    df = src.get_ohlcv("600519", date(2024, 1, 1), date(2024, 1, 5))

    assert list(df.columns) == [
        "date", "code", "open", "high", "low", "close", "volume", "amount",
    ]
    assert df["code"].iloc[0] == "600519"
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_get_ohlcv_schema_violation_raises(mocker):
    bad = _ak_ohlcv_fixture()
    bad.loc[0, "收盘"] = 0.0
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_zh_a_hist",
        return_value=bad,
    )
    src = AkShareSource()
    with pytest.raises(SchemaValidationError):
        src.get_ohlcv("600519", date(2024, 1, 1), date(2024, 1, 5))
