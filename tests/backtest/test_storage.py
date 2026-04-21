from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest import storage as bt_storage


@pytest.fixture
def tmp_root(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_BACKTEST_ROOT", str(tmp_path))
    return tmp_path


def _trade_df():
    return pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"), "code": "600000", "side": "BUY",
        "target_w": 0.01, "filled_shares": 100, "fill_price": 10.0,
        "fill_value": 1000.0, "commission": 5.0, "stamp_tax": 0.0,
        "transfer_fee": 0.01, "slippage_value": 0.5, "gross_value": 1005.51,
        "net_cash_change": -1005.51, "reason": "REBALANCE",
    }])


def _nav_df():
    return pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"), "nav": 1.0,
        "cash": 100000.0, "position_value": 0.0, "total_value": 100000.0,
        "ret_daily": 0.0, "ret_cum": 0.0, "turnover_daily": 0.0,
        "n_holdings": 0,
    }])


def _positions_df():
    return pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"), "code": "600000",
        "shares": 100, "avg_cost": 10.05, "market_value": 1000.0,
        "unrealized_pnl": -5.0, "weight": 0.01,
    }])


def test_write_read_round_trip(tmp_root):
    bt_storage.write_run(
        run_id="test_run_abcd1234",
        trades=_trade_df(), positions=_positions_df(), nav=_nav_df(),
    )
    r = bt_storage.read_run("test_run_abcd1234")
    assert len(r["trades"]) == 1
    assert len(r["nav"]) == 1
    assert len(r["positions"]) == 1


def test_append_metrics_merges_by_run_id(tmp_root):
    row1 = {
        "run_id": "x_2023_abcd1234", "strategy": "x",
        "start": pd.Timestamp("2023-01-04"), "end": pd.Timestamp("2023-12-29"),
        "ret_annual": 0.1, "ret_total": 0.12, "vol_annual": 0.15,
        "sharpe": 0.7, "sortino": 0.9, "max_drawdown": -0.1,
        "max_dd_duration_days": 30, "calmar": 1.0,
        "win_rate_daily": 0.52, "win_rate_monthly": 0.6,
        "turnover_annual": 2.0, "total_cost_bps": 30.0, "lot_drag_bps": 2.0,
        "fingerprint_sha": "a" * 64, "refdata_as_of": "2026-04-18",
    }
    bt_storage.append_metrics(row1)
    row1["sharpe"] = 0.8
    bt_storage.append_metrics(row1)
    all_m = bt_storage.read_metrics()
    assert len(all_m) == 1
    assert all_m["sharpe"].iloc[0] == 0.8


def test_read_run_missing_returns_empty(tmp_root):
    r = bt_storage.read_run("does_not_exist")
    assert r["trades"].empty
    assert r["nav"].empty
    assert r["positions"].empty
