import numpy as np
import pandas as pd

from instock.factors.evaluator import evaluate_from_frames


def _synthetic_factor_and_returns():
    """A factor whose value correlates perfectly with next-day return."""
    rng = np.random.default_rng(0)
    dates = pd.date_range("2024-01-02", periods=10, freq="B")
    codes = [f"{i:06d}" for i in range(20)]

    rows_f, rows_r = [], []
    for d in dates[:-1]:
        vals = rng.normal(size=len(codes))
        for c, v in zip(codes, vals):
            rows_f.append({"date": d, "code": c, "value": v})
            # Forward-looking return attributed to the signal date d,
            # so evaluator can inner-join on (date, code).
            rows_r.append({
                "date": d,
                "code": c,
                "ret": v + rng.normal(scale=0.01),
            })
    return pd.DataFrame(rows_f), pd.DataFrame(rows_r)


def test_ic_positive_on_synthetic():
    factor_df, returns_df = _synthetic_factor_and_returns()
    report = evaluate_from_frames(factor_df, returns_df, n_groups=5)
    assert report.ic_mean > 0.5
    assert report.rank_ic_mean > 0.5
    assert report.ic_ir > 0.0


def test_report_fields():
    factor_df, returns_df = _synthetic_factor_and_returns()
    report = evaluate_from_frames(factor_df, returns_df, n_groups=5)
    assert hasattr(report, "ic_series")
    assert hasattr(report, "group_returns")
    assert hasattr(report, "decay")
