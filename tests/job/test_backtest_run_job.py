from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from instock.job import backtest_run_job


def test_run_once_writes_artifacts(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_BACKTEST_ROOT", str(tmp_path))

    src = MagicMock()
    src.get_trade_calendar.return_value = [
        d.date() for d in pd.bdate_range("2023-01-03", "2023-01-20")
    ]
    src.get_ohlcv.return_value = pd.DataFrame()

    codes = ["600000"]
    dates = pd.bdate_range("2023-01-03", "2023-01-20")
    ohlcv = pd.DataFrame([
        {"date": d, "code": c, "open": 10.0, "high": 10.1,
         "low": 9.9, "close": 10.0, "volume": 100_000, "amount": 1e6}
        for d in dates for c in codes
    ])
    store = MagicMock()
    store.get_panel.return_value = ohlcv

    holding = pd.DataFrame([{
        "date": pd.Timestamp("2023-01-04"), "code": "600000",
        "weight": 1.0, "score": 1.0, "strategy": "stub",
    }])

    run_id = backtest_run_job.run_once(
        strategy="stub",
        start=date(2023, 1, 3),
        end=date(2023, 1, 20),
        source=src,
        store=store,
        holding=holding,
        benchmarks=(),
        enable_fees=False,
        enable_slippage=False,
        write_report=True,
    )
    assert (tmp_path / run_id).is_dir()
    assert (tmp_path / run_id / "nav.parquet").exists()
    assert (tmp_path / run_id / "report.html").exists()
    assert (tmp_path / "_metrics.parquet").exists()
