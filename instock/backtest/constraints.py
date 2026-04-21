"""Trade constraints: gate each order before execution.

Rule: first constraint to block wins. Delist is "force sell"; treated as
allowed-for-SELL but with reason=DELIST_FORCE_OUT so downstream records it.
"""
from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional

from instock.portfolio.filters import default_thresholds


@dataclass
class ConstraintContext:
    """Per-order snapshot passed into each constraint."""
    row_today: dict
    row_yesterday: dict
    listing_dates: Optional[dict] = None
    delisted: bool = False


class TradeConstraint(ABC):
    pass


class SuspendedConstraint(TradeConstraint):
    """Block both sides if today's volume <= 0."""

    def check(self, side: str, row_today: dict, row_yesterday: dict):
        if row_today.get("volume", 0) <= 0:
            return False, "SUSPENDED"
        return True, None


class LimitUpConstraint(TradeConstraint):
    """Block BUYs if today's open >= yesterday_close * (1 + threshold - tol)."""

    def __init__(self, tolerance: float = 1e-3) -> None:
        self.tolerance = tolerance

    def check(
        self, code: str, is_st: bool, side: str,
        row_today: dict, row_yesterday: dict,
    ):
        if side != "BUY":
            return True, None
        yc = row_yesterday.get("close", 0.0)
        if yc <= 0:
            return True, None
        thr = default_thresholds(code, is_st)
        if row_today["open"] / yc - 1.0 >= thr - self.tolerance:
            return False, "LIMIT_UP"
        return True, None


class LimitDownConstraint(TradeConstraint):
    """Block SELLs if today's open <= yesterday_close * (1 - threshold + tol)."""

    def __init__(self, tolerance: float = 1e-3) -> None:
        self.tolerance = tolerance

    def check(
        self, code: str, is_st: bool, side: str,
        row_today: dict, row_yesterday: dict,
    ):
        if side != "SELL":
            return True, None
        yc = row_yesterday.get("close", 0.0)
        if yc <= 0:
            return True, None
        thr = default_thresholds(code, is_st)
        if row_today["open"] / yc - 1.0 <= -(thr - self.tolerance):
            return False, "LIMIT_DOWN"
        return True, None


class DelistConstraint(TradeConstraint):
    """If delisted: force SELL (reason=DELIST_FORCE_OUT), block BUY."""

    def check(self, side: str, delisted: bool):
        if not delisted:
            return True, None
        if side == "SELL":
            return True, "DELIST_FORCE_OUT"
        return False, "DELISTED_NO_BUY"


class ConstraintChain:
    """Apply constraints in order; first block wins."""

    def __init__(self, constraints: list) -> None:
        self.constraints = constraints

    def check(
        self, code: str, is_st: bool, side: str,
        row_today: dict, row_yesterday: dict,
        delisted: bool = False,
    ):
        for c in self.constraints:
            if isinstance(c, SuspendedConstraint):
                ok, reason = c.check(side, row_today, row_yesterday)
            elif isinstance(c, (LimitUpConstraint, LimitDownConstraint)):
                ok, reason = c.check(
                    code, is_st, side, row_today, row_yesterday
                )
            elif isinstance(c, DelistConstraint):
                ok, reason = c.check(side, delisted)
            else:
                raise TypeError(f"Unknown constraint {type(c)}")
            if not ok:
                return False, reason
            if reason == "DELIST_FORCE_OUT":
                return True, reason
        return True, None
