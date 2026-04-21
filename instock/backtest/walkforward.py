"""Walk-forward window generator.

Emits consecutive (train, test) windows stepping by `step_months`. Train
window immediately precedes test window. The last window's test_end is
truncated to `end` if it would overrun. Windows where test_start >= end
are dropped.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List

from dateutil.relativedelta import relativedelta


@dataclass(frozen=True)
class WalkForwardConfig:
    train_window_months: int
    test_window_months: int
    step_months: int
    min_train_obs: int = 200


@dataclass(frozen=True)
class Window:
    train_start: date
    train_end: date
    test_start: date
    test_end: date


def window_bounds(
    start: date,
    end: date,
    cfg: WalkForwardConfig,
) -> List[Window]:
    out: List[Window] = []
    cur_train_start = start
    while True:
        train_end = cur_train_start + relativedelta(months=cfg.train_window_months)
        test_start = train_end
        test_end = test_start + relativedelta(months=cfg.test_window_months)
        if test_start >= end:
            break
        if test_end > end:
            test_end = end
        out.append(Window(cur_train_start, train_end, test_start, test_end))
        cur_train_start = cur_train_start + relativedelta(months=cfg.step_months)
    return out
