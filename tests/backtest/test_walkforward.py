from __future__ import annotations

from datetime import date

from instock.backtest.walkforward import WalkForwardConfig, window_bounds


def test_window_bounds_basic():
    cfg = WalkForwardConfig(train_window_months=12, test_window_months=3, step_months=3)
    wins = window_bounds(date(2020, 1, 1), date(2022, 1, 1), cfg)
    assert len(wins) > 0
    w0 = wins[0]
    assert w0.train_start == date(2020, 1, 1)
    assert w0.train_end == date(2021, 1, 1)
    assert w0.test_start == date(2021, 1, 1)
    assert w0.test_end == date(2021, 4, 1)
    # step of 3 months
    w1 = wins[1]
    assert w1.train_start == date(2020, 4, 1)
    assert w1.test_start == date(2021, 4, 1)


def test_window_bounds_truncates_final():
    cfg = WalkForwardConfig(train_window_months=12, test_window_months=6, step_months=6)
    wins = window_bounds(date(2020, 1, 1), date(2021, 9, 1), cfg)
    assert wins[-1].test_end <= date(2021, 9, 1)


def test_window_bounds_empty_when_window_too_large():
    cfg = WalkForwardConfig(train_window_months=36, test_window_months=12, step_months=3)
    wins = window_bounds(date(2020, 1, 1), date(2021, 1, 1), cfg)
    assert wins == []
