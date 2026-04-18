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


from instock.portfolio.filters import LimitFilter, default_thresholds


def test_thresholds_by_board():
    assert default_thresholds("000001", False) == 0.10
    assert default_thresholds("600000", False) == 0.10
    assert default_thresholds("300001", False) == 0.20
    assert default_thresholds("688001", False) == 0.20
    assert default_thresholds("830001", False) == 0.30
    assert default_thresholds("430001", False) == 0.30
    assert default_thresholds("000001", True) == 0.05  # ST override


def test_limitfilter_drops_limit_up_on_prior_day():
    panel = pd.DataFrame({
        "date": pd.to_datetime(
            ["2026-03-30", "2026-03-31", "2026-04-01"]
        ),
        "code": ["000001"] * 3,
        "open":   [10.0, 10.5, 11.1],
        "high":   [10.0, 11.0, 11.2],
        "low":    [9.8, 10.3, 10.9],
        "close":  [10.0, 11.0, 11.1],
        "volume": [1000.0, 1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, st_flags=set())
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == []


def test_limitfilter_keeps_non_limit():
    panel = pd.DataFrame({
        "date": pd.to_datetime(["2026-03-30", "2026-03-31"]),
        "code": ["000001"] * 2,
        "open":   [10.0, 10.2],
        "high":   [10.1, 10.5],
        "low":    [9.9, 10.1],
        "close":  [10.0, 10.3],
        "volume": [1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, st_flags=set())
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == ["000001"]


def test_limitfilter_uses_st_threshold():
    panel = pd.DataFrame({
        "date": pd.to_datetime(["2026-03-30", "2026-03-31"]),
        "code": ["000001"] * 2,
        "open":   [10.0, 10.4],
        "high":   [10.0, 10.5],
        "low":    [9.8, 10.3],
        "close":  [10.0, 10.5],
        "volume": [1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, st_flags={"000001"})
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == []


def test_limitfilter_filters_when_t_minus_1_missing():
    panel = pd.DataFrame(
        columns=["date", "code", "open", "high", "low", "close", "volume"]
    )
    ctx = FilterContext(ohlcv_panel=panel, st_flags=set())
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == []


from datetime import date as _date
from instock.portfolio.filters import NewListingFilter as _NewListingFilter


def test_newlisting_uses_listing_dates_when_provided():
    panel = pd.DataFrame(
        columns=["date", "code", "open", "high", "low", "close", "volume"]
    )
    ctx = FilterContext(
        ohlcv_panel=panel,
        listing_dates={
            "000001": _date(2020, 1, 1),
            "600000": _date(2026, 4, 1),  # too new
        },
    )
    out = _NewListingFilter(min_days=60).apply(
        ["000001", "600000"], pd.Timestamp("2026-04-10"), ctx
    )
    assert out == ["000001"]


def test_newlisting_falls_back_to_ohlcv_when_listing_dates_none(caplog):
    panel = pd.DataFrame({
        "date": pd.to_datetime(
            ["2020-01-01", "2026-03-01", "2026-04-10"]
        ),
        "code": ["000001", "600000", "600000"],
        "open":   [10.0, 10.0, 10.0],
        "high":   [10.0, 10.0, 10.0],
        "low":    [10.0, 10.0, 10.0],
        "close":  [10.0, 10.0, 10.0],
        "volume": [1000.0, 1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, listing_dates=None)
    with caplog.at_level(logging.WARNING):
        out = _NewListingFilter(min_days=60).apply(
            ["000001", "600000"], pd.Timestamp("2026-04-10"), ctx
        )
    assert out == ["000001"]
    assert any(
        "fallback" in r.message.lower() or "ohlcv" in r.message.lower()
        for r in caplog.records
    )
