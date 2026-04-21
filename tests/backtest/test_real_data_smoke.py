from __future__ import annotations

import os
from datetime import date

import pandas as pd
import pytest

from instock.backtest.config import BacktestConfig
from instock.backtest.engine import BacktestEngine
from instock.backtest.metrics import compute_metrics

_SMOKE_ENABLED = os.environ.get("INSTOCK_SUB3_SMOKE") == "1"


@pytest.mark.skipif(
    not _SMOKE_ENABLED, reason="set INSTOCK_SUB3_SMOKE=1 to run"
)
def test_smoke_run_6m_window(tmp_path, monkeypatch):
    from instock.datasource.registry import get_source
    from instock.refdata.ohlcv_store import OhlcvPanelStore

    monkeypatch.setenv("INSTOCK_BACKTEST_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_OHLCV_ROOT", str(tmp_path / "ohlcv"))

    src = get_source()
    store = OhlcvPanelStore(source=src)
    codes = ["600000", "600519", "000001"]
    dates = [pd.Timestamp("2023-07-07"), pd.Timestamp("2023-10-13")]
    holding = pd.DataFrame([
        {"date": d, "code": c, "weight": 1.0 / len(codes),
         "score": 1.0, "strategy": "smoke"}
        for d in dates for c in codes
    ])

    cfg = BacktestConfig(
        strategy="smoke",
        start=date(2023, 7, 3), end=date(2023, 12, 29),
    )
    eng = BacktestEngine(source=src, ohlcv_store=store)
    res = eng.run(holding_schedule=holding, config=cfg)

    assert len(res["trades"]) > 0
    nav = res["nav"]
    assert len(nav) > 100
    assert nav["nav"].iloc[-1] > 0
    m = compute_metrics(nav, res["trades"], pd.DataFrame())
    for k in ("ret_annual", "vol_annual", "max_drawdown",
              "turnover_annual", "total_cost_bps"):
        assert k in m and not pd.isna(m[k])
