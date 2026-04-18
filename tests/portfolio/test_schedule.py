from datetime import date

import pytest

from instock.portfolio.schedule import DailyRebalance, WeeklyRebalance


def _cal(days):
    return [date(2024, 1, d) for d in days]


def test_daily_rebalance_returns_every_trading_day():
    cal = _cal([2, 3, 4, 5])
    out = DailyRebalance().rebalance_dates(date(2024, 1, 1), date(2024, 1, 5), cal)
    assert out == cal


def test_daily_rebalance_respects_window():
    cal = _cal([2, 3, 4, 5])
    out = DailyRebalance().rebalance_dates(date(2024, 1, 3), date(2024, 1, 4), cal)
    assert out == [date(2024, 1, 3), date(2024, 1, 4)]


def test_weekly_rebalance_picks_fri():
    # Jan 2024: Fri=5, 12, 19, 26
    cal = _cal([2, 3, 4, 5, 8, 9, 10, 11, 12])
    out = WeeklyRebalance("FRI").rebalance_dates(
        date(2024, 1, 1), date(2024, 1, 14), cal
    )
    assert out == [date(2024, 1, 5), date(2024, 1, 12)]


def test_weekly_rebalance_falls_back_when_friday_missing():
    # Friday Jan 5 missing from calendar; should fall back to Thu Jan 4.
    cal = _cal([2, 3, 4, 8, 9, 10, 11, 12])
    out = WeeklyRebalance("FRI").rebalance_dates(
        date(2024, 1, 1), date(2024, 1, 14), cal
    )
    assert out == [date(2024, 1, 4), date(2024, 1, 12)]


def test_weekly_rebalance_rejects_bad_weekday():
    with pytest.raises(ValueError):
        WeeklyRebalance("XYZ")
