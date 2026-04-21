from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from instock.backtest.metrics import (
    annualize_return, annualize_volatility,
    sharpe_ratio, max_drawdown, compute_metrics,
)


def test_annualize_constant_daily_return():
    rets = pd.Series([0.01] * 252)
    ann = annualize_return(rets)
    assert ann == pytest.approx((1.01) ** 252 - 1, rel=1e-6)


def test_annualize_vol_scales_sqrt():
    rets = pd.Series(np.random.default_rng(0).normal(0, 0.01, 252))
    vol_daily = rets.std(ddof=1)
    ann = annualize_volatility(rets)
    assert ann == pytest.approx(vol_daily * np.sqrt(252), rel=1e-6)


def test_sharpe_positive_drift():
    rets = pd.Series([0.001] * 252)
    sr = sharpe_ratio(rets)
    assert pd.isna(sr)


def test_max_drawdown_known():
    nav = pd.Series([1.0, 1.2, 1.0, 1.3])
    dd, dur = max_drawdown(nav)
    assert dd == pytest.approx(-1 / 6, rel=1e-4)
    assert dur == 1


def test_max_drawdown_no_drawdown():
    nav = pd.Series([1.0, 1.1, 1.2])
    dd, dur = max_drawdown(nav)
    assert dd == 0.0
    assert dur == 0


def test_compute_metrics_smoke():
    dates = pd.bdate_range("2023-01-04", periods=252)
    nav_values = np.cumprod(1 + 0.0005 * np.ones(252))
    nav_df = pd.DataFrame({
        "date": dates, "nav": nav_values,
        "cash": 0.0, "position_value": nav_values * 1_000_000,
        "total_value": nav_values * 1_000_000,
        "ret_daily": [0.0005] * 252,
        "ret_cum": nav_values - 1,
        "turnover_daily": [0.0] * 252, "n_holdings": [10] * 252,
    })
    trades = pd.DataFrame([])
    benchmarks = pd.DataFrame()
    m = compute_metrics(nav_df, trades, benchmarks)
    assert m["ret_annual"] > 0
    assert m["vol_annual"] == 0.0
    assert pd.isna(m["sharpe"])
    assert m["max_drawdown"] == 0.0
    assert m["total_cost_bps"] == 0.0
