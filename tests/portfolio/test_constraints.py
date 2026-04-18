import pandas as pd
import pytest

from instock.portfolio.constraints import (
    ConstraintContext,
    IndustryExposureConstraint,
    MaxWeightConstraint,
)


def _ctx(industry_map=None):
    return ConstraintContext(industry_map=industry_map)


def test_max_weight_noop_when_under_cap():
    w = pd.Series({"A00001": 0.3, "B00002": 0.3, "C00003": 0.4})
    out = MaxWeightConstraint(0.5).apply(w, _ctx())
    pd.testing.assert_series_equal(out.sort_index(), w.sort_index())


def test_max_weight_clips_and_redistributes():
    # One name at 60%, caps at 40%; the 20% overflow should redistribute
    # to the other two (originally 20% each) proportionally.
    # (Cap 0.4 chosen so 3 * 0.4 = 1.2 >= 1.0 is feasible.)
    w = pd.Series({"A00001": 0.6, "B00002": 0.2, "C00003": 0.2})
    out = MaxWeightConstraint(0.4).apply(w, _ctx())
    assert abs(out.sum() - 1.0) < 1e-9
    assert out["A00001"] <= 0.4 + 1e-9
    # B, C equally split the redistributed weight, so still equal
    assert abs(out["B00002"] - out["C00003"]) < 1e-9


def test_max_weight_iterates_until_converged():
    w = pd.Series({"A00001": 0.5, "B00002": 0.4, "C00003": 0.05,
                   "D00004": 0.05})
    out = MaxWeightConstraint(0.3).apply(w, _ctx())
    assert (out <= 0.3 + 1e-9).all()
    assert abs(out.sum() - 1.0) < 1e-9


def test_max_weight_infeasible_raises():
    w = pd.Series({"A00001": 0.5, "B00002": 0.5})
    with pytest.raises(ValueError):
        MaxWeightConstraint(0.3).apply(w, _ctx())


def test_industry_constraint_is_noop_without_map():
    w = pd.Series({"A00001": 0.5, "B00002": 0.5})
    out = IndustryExposureConstraint(0.4, industry_map=None).apply(
        w, _ctx(industry_map=None)
    )
    pd.testing.assert_series_equal(out.sort_index(), w.sort_index())


def test_industry_constraint_scales_over_cap_industry():
    w = pd.Series({
        "A00001": 0.4, "B00002": 0.3,
        "C00003": 0.2, "D00004": 0.1,
    })
    imap = {"A00001": "tech", "B00002": "tech",
            "C00003": "finance", "D00004": "finance"}
    out = IndustryExposureConstraint(0.4, industry_map=imap).apply(
        w, _ctx(industry_map=imap)
    )
    tech_total = out[["A00001", "B00002"]].sum()
    assert tech_total <= 0.4 + 1e-9
    assert abs(out.sum() - 1.0) < 1e-9
