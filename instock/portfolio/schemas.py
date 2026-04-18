"""Pandera schema and invariants for HoldingSchedule.

Semantic contract (CRITICAL, relied on by Sub-3 backtester):
    HoldingSchedule.date = T means:
        The target portfolio is generated from data known as of T's close
        and is executable at T+1 open.
    Implication: factors/prices/halt-status must all use data <= T.
"""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa


class HoldingScheduleValidationError(Exception):
    """Raised when a HoldingSchedule violates schema or invariants.

    Fail-fast: never retried. Indicates a bug in pipeline/constraints.
    """


HOLDING_SCHEDULE_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "code": pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "weight": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
        "score": pa.Column(float, nullable=True),
        "strategy": pa.Column(str, pa.Check.str_length(min_value=1)),
    },
    coerce=True,
    strict=False,
)


_WEIGHT_SUM_TOL = 1e-6


def validate_holding_invariants(df: pd.DataFrame) -> None:
    """Enforce invariants pandera cannot express.

    For each (date, strategy) group:
      - weight.sum() ~= 1.0 (within _WEIGHT_SUM_TOL)
      - code is unique
    """
    if df.empty:
        return
    for (d, s), g in df.groupby(["date", "strategy"]):
        total = g["weight"].sum()
        if abs(total - 1.0) > _WEIGHT_SUM_TOL:
            raise HoldingScheduleValidationError(
                f"weight sum {total:.6f} != 1.0 for (date={d}, strategy={s})"
            )
        if g["code"].duplicated().any():
            dup = g.loc[g["code"].duplicated(), "code"].tolist()
            raise HoldingScheduleValidationError(
                f"duplicate codes {dup} for (date={d}, strategy={s})"
            )
