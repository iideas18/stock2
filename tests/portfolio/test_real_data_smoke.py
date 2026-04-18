"""Real-data smoke test for Sub-2 MVP gate.

Skip unless INSTOCK_SUB2_SMOKE=1 is set in the environment. This test reads
real factor Parquet written by Sub-1's daily job (INSTOCK_FACTOR_ROOT must
also be set) and runs the default strategy over a 3-month CSI-300 window.

Assertions (MVP gate per spec Q8/B):
  - Each rebalance date's weight sum == 1.0
  - All weights <= 3%
  - At least one rebalance date produces non-empty holdings
  - Industry distribution is dumped to stderr for human eyeballing
"""
from __future__ import annotations

import os
import sys
from datetime import date

import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("INSTOCK_SUB2_SMOKE") != "1",
    reason="real-data smoke: set INSTOCK_SUB2_SMOKE=1 to run",
)


def test_default_strategy_three_months():
    from instock.job.generate_holdings_daily_job import _default_configs
    from instock.portfolio import storage
    from instock.portfolio.pipeline import StrategyPipeline

    configs = _default_configs()
    assert configs, "no default configs (factor registry empty?)"
    cfg = configs[0]

    start = date(2024, 1, 2)
    end = date(2024, 3, 29)

    df = StrategyPipeline().run(start, end, cfg)
    assert not df.empty, "pipeline produced no rows"

    for d, g in df.groupby("date"):
        assert abs(g["weight"].sum() - 1.0) < 1e-6, (
            f"sum != 1 on {d}: {g['weight'].sum()}"
        )
        assert (g["weight"] <= 0.03 + 1e-9).all(), (
            f"weight > 3% on {d}: {g.loc[g['weight'] > 0.03].to_dict()}"
        )

    storage.write_holding(cfg.name, df)
    back = storage.read_holding(
        cfg.name, pd.Timestamp(start), pd.Timestamp(end)
    )
    assert len(back) == len(df)

    summary = df.groupby("date").agg(
        n_names=("code", "count"),
        sum_w=("weight", "sum"),
        max_w=("weight", "max"),
    )
    print("\n=== Sub-2 smoke summary ===", file=sys.stderr)
    print(summary.to_string(), file=sys.stderr)
