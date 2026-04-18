import pandas as pd
import pytest

from instock.portfolio.schemas import (
    HOLDING_SCHEDULE_SCHEMA,
    HoldingScheduleValidationError,
    validate_holding_invariants,
)


def _good_row(date="2024-01-02", code="600519", weight=1.0,
              score=0.5, strategy="default"):
    return {
        "date": pd.Timestamp(date),
        "code": code,
        "weight": weight,
        "score": score,
        "strategy": strategy,
    }


def test_schema_accepts_valid_row():
    df = pd.DataFrame([_good_row()])
    out = HOLDING_SCHEDULE_SCHEMA.validate(df)
    assert list(out.columns) >= ["date", "code", "weight", "score", "strategy"]


def test_schema_rejects_bad_code():
    df = pd.DataFrame([_good_row(code="ABC")])
    with pytest.raises(Exception):
        HOLDING_SCHEDULE_SCHEMA.validate(df)


def test_schema_rejects_weight_out_of_range():
    df = pd.DataFrame([_good_row(weight=1.5)])
    with pytest.raises(Exception):
        HOLDING_SCHEDULE_SCHEMA.validate(df)


def test_schema_accepts_null_score():
    df = pd.DataFrame([_good_row(score=None)])
    out = HOLDING_SCHEDULE_SCHEMA.validate(df)
    assert pd.isna(out["score"].iloc[0])


def test_invariants_pass_when_weights_sum_to_one():
    df = pd.DataFrame([
        _good_row(code="600519", weight=0.5),
        _good_row(code="000001", weight=0.5),
    ])
    validate_holding_invariants(df)  # should not raise


def test_invariants_fail_when_weights_do_not_sum_to_one():
    df = pd.DataFrame([
        _good_row(code="600519", weight=0.4),
        _good_row(code="000001", weight=0.4),
    ])
    with pytest.raises(HoldingScheduleValidationError):
        validate_holding_invariants(df)


def test_invariants_fail_on_duplicate_code_in_same_group():
    df = pd.DataFrame([
        _good_row(code="600519", weight=0.5),
        _good_row(code="600519", weight=0.5),
    ])
    with pytest.raises(HoldingScheduleValidationError):
        validate_holding_invariants(df)
