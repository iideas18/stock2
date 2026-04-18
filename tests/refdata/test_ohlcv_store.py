from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from instock.refdata.ohlcv_store import OhlcvPanelStore


def _ohlcv_rows(code, dates):
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": [code] * len(dates),
        "open": [10.0] * len(dates),
        "high": [11.0] * len(dates),
        "low": [9.5] * len(dates),
        "close": [10.5] * len(dates),
        "volume": [1000.0] * len(dates),
        "amount": [10500.0] * len(dates),
    })


def test_cache_miss_fetches_and_stores(tmp_ohlcv_root):
    source = MagicMock()
    source.get_trade_calendar.return_value = [
        date(2026, 4, 1), date(2026, 4, 2)
    ]
    source.get_ohlcv.side_effect = lambda c, s, e, adjust="qfq": \
        _ohlcv_rows(c, ["2026-04-01", "2026-04-02"])

    store = OhlcvPanelStore(source=source)
    got = store.get_panel(
        ["000001"], date(2026, 4, 1), date(2026, 4, 2)
    )
    assert len(got) == 2
    source.get_ohlcv.assert_called_once()


def test_cache_hit_skips_fetch(tmp_ohlcv_root):
    source = MagicMock()
    source.get_trade_calendar.return_value = [
        date(2026, 4, 1), date(2026, 4, 2)
    ]
    source.get_ohlcv.side_effect = lambda c, s, e, adjust="qfq": \
        _ohlcv_rows(c, ["2026-04-01", "2026-04-02"])
    store = OhlcvPanelStore(source=source)

    store.get_panel(["000001"], date(2026, 4, 1), date(2026, 4, 2))
    source.get_ohlcv.reset_mock()
    got2 = store.get_panel(["000001"], date(2026, 4, 1), date(2026, 4, 2))
    assert len(got2) == 2
    source.get_ohlcv.assert_not_called()


def test_missing_by_trade_calendar_not_date_range(tmp_ohlcv_root):
    """Weekends/holidays in [start,end] should NOT trigger refetch."""
    source = MagicMock()
    # Only Monday is a trade day; Sat/Sun are gaps by calendar.
    source.get_trade_calendar.return_value = [date(2026, 4, 6)]
    source.get_ohlcv.side_effect = lambda c, s, e, adjust="qfq": \
        _ohlcv_rows(c, ["2026-04-06"])
    store = OhlcvPanelStore(source=source)

    store.get_panel(["000001"], date(2026, 4, 4), date(2026, 4, 6))
    source.get_ohlcv.reset_mock()
    store.get_panel(["000001"], date(2026, 4, 4), date(2026, 4, 6))
    source.get_ohlcv.assert_not_called()


def test_single_code_fetch_failure_logged_and_skipped(tmp_ohlcv_root, caplog):
    source = MagicMock()
    source.get_trade_calendar.return_value = [date(2026, 4, 1)]

    def _maybe_fail(c, s, e, adjust="qfq"):
        if c == "000001":
            raise RuntimeError("boom")
        return _ohlcv_rows(c, ["2026-04-01"])

    source.get_ohlcv.side_effect = _maybe_fail
    store = OhlcvPanelStore(source=source)
    got = store.get_panel(
        ["000001", "600000"], date(2026, 4, 1), date(2026, 4, 1)
    )
    assert set(got["code"]) == {"600000"}
