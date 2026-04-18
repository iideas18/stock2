"""RebalanceSchedule: decide which dates in [start, end] to rebalance on."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, timedelta


_WEEKDAYS = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4,
             "SAT": 5, "SUN": 6}


class RebalanceSchedule(ABC):
    @abstractmethod
    def rebalance_dates(
        self, start: date, end: date, trade_calendar: list[date]
    ) -> list[date]:
        ...


class DailyRebalance(RebalanceSchedule):
    def rebalance_dates(self, start, end, trade_calendar):
        return [d for d in trade_calendar if start <= d <= end]


class WeeklyRebalance(RebalanceSchedule):
    """Pick one trading day per ISO week: the latest trading day <= the
    target weekday. If no such day exists in that week, skip the week."""

    def __init__(self, weekday: str = "FRI") -> None:
        key = weekday.upper()
        if key not in _WEEKDAYS:
            raise ValueError(f"unknown weekday {weekday!r}")
        self.weekday = _WEEKDAYS[key]

    def rebalance_dates(self, start, end, trade_calendar):
        cal_set = set(trade_calendar)
        out: list[date] = []
        cur = start - timedelta(days=start.weekday())
        while cur <= end:
            target = cur + timedelta(days=self.weekday)
            picked = None
            for i in range(self.weekday + 1):
                candidate = target - timedelta(days=i)
                if candidate < cur:
                    break
                if (
                    start <= candidate <= end
                    and candidate in cal_set
                ):
                    picked = candidate
                    break
            if picked is not None:
                out.append(picked)
            cur += timedelta(days=7)
        return out
