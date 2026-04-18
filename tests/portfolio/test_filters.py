import logging
import pandas as pd
import pytest

from instock.portfolio.filters import (
    FilterChain,
    FilterContext,
    NewListingFilter,
    STFilter,
    SuspendedFilter,
)


def _ohlcv(code, dates, volume=1_000_000):
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": [code] * len(dates),
        "volume": [volume] * len(dates),
    })


def test_suspended_filter_drops_zero_volume():
    panel = pd.concat([
        _ohlcv("600519", ["2024-01-02"], volume=1000),
        _ohlcv("000001", ["2024-01-02"], volume=0),
    ])
    ctx = FilterContext(ohlcv_panel=panel)
    out = SuspendedFilter().apply(
        ["600519", "000001"], pd.Timestamp("2024-01-02"), ctx
    )
    assert out == ["600519"]


def test_suspended_filter_drops_missing_rows():
    panel = _ohlcv("600519", ["2024-01-02"])
    ctx = FilterContext(ohlcv_panel=panel)
    out = SuspendedFilter().apply(
        ["600519", "000001"], pd.Timestamp("2024-01-02"), ctx
    )
    assert out == ["600519"]


def test_new_listing_filter_drops_too_young():
    panel = pd.concat([
        _ohlcv("600519", ["2023-12-01", "2024-03-01"]),
        _ohlcv("000001", ["2024-02-15", "2024-03-01"]),
    ])
    ctx = FilterContext(ohlcv_panel=panel)
    out = NewListingFilter(min_days=60).apply(
        ["600519", "000001"], pd.Timestamp("2024-03-01"), ctx
    )
    assert out == ["600519"]


def test_filter_chain_applies_in_order():
    panel = pd.concat([
        _ohlcv("600519", ["2023-12-01", "2024-03-01"]),
        _ohlcv("000001", ["2024-02-15", "2024-03-01"]),
        _ohlcv("000002", ["2023-12-01", "2024-03-01"], volume=0),
    ])
    ctx = FilterContext(ohlcv_panel=panel)
    chain = FilterChain([SuspendedFilter(), NewListingFilter(min_days=60)])
    out = chain.apply(
        ["600519", "000001", "000002"], pd.Timestamp("2024-03-01"), ctx
    )
    assert out == ["600519"]


def test_filter_chain_short_circuits_on_empty():
    panel = pd.DataFrame({"date": [], "code": [], "volume": []})
    ctx = FilterContext(ohlcv_panel=panel)
    out = FilterChain([SuspendedFilter()]).apply(
        ["600519"], pd.Timestamp("2024-01-02"), ctx
    )
    assert out == []


def _empty_ohlcv():
    return pd.DataFrame(
        columns=["date", "code", "open", "high", "low", "close", "volume"]
    )


def test_stfilter_drops_st_codes():
    ctx = FilterContext(
        ohlcv_panel=_empty_ohlcv(),
        st_flags={"000001"},
    )
    out = STFilter().apply(
        ["000001", "600000"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == ["600000"]


def test_stfilter_noop_when_flags_missing(caplog):
    ctx = FilterContext(ohlcv_panel=_empty_ohlcv(), st_flags=None)
    with caplog.at_level(logging.WARNING):
        out = STFilter().apply(
            ["000001"], pd.Timestamp("2026-04-01"), ctx
        )
    assert out == ["000001"]
    assert any("st_flags" in rec.message.lower() for rec in caplog.records)


def test_stfilter_warns_only_once(caplog):
    flt = STFilter()
    ctx = FilterContext(ohlcv_panel=_empty_ohlcv(), st_flags=None)
    with caplog.at_level(logging.WARNING):
        flt.apply(["000001"], pd.Timestamp("2026-04-01"), ctx)
        flt.apply(["000002"], pd.Timestamp("2026-04-02"), ctx)
    st_warnings = [
        r for r in caplog.records if "st_flags" in r.message.lower()
    ]
    assert len(st_warnings) == 1
