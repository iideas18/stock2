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


def _ak_fin_indicator_fixture():
    return pd.DataFrame({
        "报告期":       ["20230930", "20230630"],
        "公告日期":     ["20231028", "20230829"],
        "营业收入":     [1.0e10, 9.0e9],
        "净利润":       [1.5e9, 1.2e9],
        "净资产收益率": [18.5, 15.2],
    })


def test_get_fundamentals_pit_normalises(mocker):
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_financial_abstract",
        return_value=_ak_fin_indicator_fixture(),
    )
    src = AkShareSource()
    df = src.get_fundamentals_pit("600519", ["revenue", "net_profit", "roe"])
    assert "announcement_date" in df.columns
    assert "report_period" in df.columns
    assert df["code"].iloc[0] == "600519"
    assert pd.api.types.is_datetime64_any_dtype(df["announcement_date"])
    assert pd.api.types.is_datetime64_any_dtype(df["report_period"])


def _ak_lhb_fixture():
    return pd.DataFrame({
        "交易日":      ["2024-01-02"],
        "代码":        ["600519"],
        "营业部名称":  ["中信证券北京建国路"],
        "买入金额":    [1.0e8],
        "卖出金额":    [5.0e7],
        "上榜原因":    ["日涨幅偏离值达7%"],
    })


def test_get_lhb_normalises(mocker):
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_lhb_detail_em",
        return_value=_ak_lhb_fixture(),
    )
    src = AkShareSource()
    df = src.get_lhb(date(2024, 1, 1), date(2024, 1, 5))
    assert list(df.columns) == [
        "trade_date", "code", "seat", "buy_amount", "sell_amount", "reason",
    ]


def test_get_trade_calendar_returns_list_of_dates(mocker):
    fake = pd.DataFrame({"trade_date": pd.to_datetime(
        ["2024-01-02", "2024-01-03", "2024-01-04"]
    )})
    mocker.patch(
        "instock.datasource.akshare_source.ak.tool_trade_date_hist_sina",
        return_value=fake,
    )
    src = AkShareSource()
    cal = src.get_trade_calendar(date(2024, 1, 1), date(2024, 1, 31))
    assert cal == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
