from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from instock.backtest.benchmarks import load_benchmarks


def _ohlcv_series(start, end, code, base=3000.0):
    dates = pd.bdate_range(start, end)
    return pd.DataFrame({
        "date": dates, "code": [code] * len(dates),
        "open": base, "high": base, "low": base,
        "close": [base + i * 0.1 for i in range(len(dates))],
        "volume": 1, "amount": 1,
    })


def test_load_benchmarks_returns_wide_frame():
    src = MagicMock()

    def _gt(code, start, end, adjust="qfq"):
        return _ohlcv_series(start, end, code)

    src.get_ohlcv.side_effect = _gt

    out = load_benchmarks(
        src, ["000300.SH", "000905.SH"],
        date(2023, 1, 4), date(2023, 1, 20),
    )
    assert "000300.SH" in out.columns
    assert "000905.SH" in out.columns
    assert isinstance(out.index, pd.DatetimeIndex)


def test_load_benchmarks_skips_missing():
    src = MagicMock()

    def _gt(code, start, end, adjust="qfq"):
        if code == "000300.SH":
            return _ohlcv_series(start, end, code)
        raise RuntimeError("not found")

    src.get_ohlcv.side_effect = _gt

    out = load_benchmarks(
        src, ["000300.SH", "000999.SH"],
        date(2023, 1, 4), date(2023, 1, 20),
    )
    assert "000300.SH" in out.columns
    assert "000999.SH" not in out.columns


def test_load_benchmarks_empty_list():
    out = load_benchmarks(
        MagicMock(), [], date(2023, 1, 4), date(2023, 1, 20),
    )
    assert out.empty
