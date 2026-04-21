from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.report import render_report


def test_render_report_returns_html_with_required_sections():
    nav = pd.DataFrame({
        "date": pd.bdate_range("2023-01-04", periods=60),
        "nav": [1.0 + i * 0.001 for i in range(60)],
        "cash": [1000.0] * 60, "position_value": [0.0] * 60,
        "total_value": [1_000_000.0] * 60,
        "ret_daily": [0.001] * 60, "ret_cum": [i * 0.001 for i in range(60)],
        "turnover_daily": [0.0] * 60, "n_holdings": [0] * 60,
    })
    trades = pd.DataFrame()
    metrics = {
        "run_id": "x_abcd1234", "strategy": "x",
        "ret_annual": 0.1, "ret_total": 0.06, "vol_annual": 0.01,
        "sharpe": 8.0, "max_drawdown": 0.0, "total_cost_bps": 0.0,
        "fingerprint_sha": "a" * 64,
    }
    benchmarks = pd.DataFrame()
    html = render_report(
        run_id="x_abcd1234", nav=nav, trades=trades,
        metrics=metrics, benchmarks=benchmarks,
        refdata_as_of="2026-04-18",
    )
    assert "<html" in html.lower()
    assert "x_abcd1234" in html
    assert "refdata as of 2026-04-18" in html
    assert "a" * 16 in html
    assert "data:image/png;base64," in html


def test_render_report_handles_missing_benchmarks():
    nav = pd.DataFrame({
        "date": pd.bdate_range("2023-01-04", periods=10),
        "nav": [1.0] * 10, "cash": [0.0] * 10,
        "position_value": [0.0] * 10, "total_value": [1e6] * 10,
        "ret_daily": [0.0] * 10, "ret_cum": [0.0] * 10,
        "turnover_daily": [0.0] * 10, "n_holdings": [0] * 10,
    })
    html = render_report(
        run_id="t", nav=nav, trades=pd.DataFrame(),
        metrics={"run_id": "t", "strategy": "t",
                 "fingerprint_sha": "a" * 64, "sharpe": 0.0},
        benchmarks=pd.DataFrame(),
        refdata_as_of=None,
    )
    assert "refdata not available" in html
