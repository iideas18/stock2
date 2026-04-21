from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from instock.monitoring.status import ICIRDecay


def _make_factor(mean_ic: float, n_days: int = 40) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.bdate_range("2026-01-01", periods=n_days)
    rows = []
    for d in dates:
        codes = [f"{i:06d}" for i in range(50)]
        values = rng.standard_normal(50)
        fwd = values * mean_ic + rng.standard_normal(50) * 0.1
        for c, v, f in zip(codes, values, fwd):
            rows.append({"date": d, "code": c, "value": v, "fwd_ret": f})
    return pd.DataFrame(rows)


def test_healthy_factor_green(monkeypatch):
    df = _make_factor(mean_ic=0.3)
    chk = ICIRDecay(factor="mom_5d", window_days=30, threshold=0.1,
                    frame_loader=lambda name: df)
    row = chk.run()
    assert row.status == "GREEN"
    assert row.metric_value > 0.1


def test_decayed_factor_red():
    df = _make_factor(mean_ic=0.0)
    chk = ICIRDecay(factor="mom_5d", window_days=30, threshold=0.5,
                    frame_loader=lambda name: df)
    row = chk.run()
    assert row.status == "RED"


def test_empty_data_yellow():
    chk = ICIRDecay(factor="mom_5d", window_days=30, threshold=0.1,
                    frame_loader=lambda name: pd.DataFrame(
                        columns=["date", "code", "value", "fwd_ret"]
                    ))
    assert chk.run().status == "YELLOW"
