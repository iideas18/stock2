from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
import pytest

from instock.backtest.schemas import (
    TRADE_SCHEMA,
    POSITION_SCHEMA,
    NAV_SCHEMA,
    METRICS_SCHEMA,
    BacktestValidationError,
)


def _valid_trade_row() -> dict:
    return {
        "date": pd.Timestamp("2023-01-03"),
        "code": "600000",
        "side": "BUY",
        "target_w": 0.01,
        "filled_shares": 100,
        "fill_price": 10.0,
        "fill_value": 1000.0,
        "commission": 5.0,
        "stamp_tax": 0.0,
        "transfer_fee": 0.01,
        "slippage_value": 0.5,
        "gross_value": 1000.5,
        "net_cash_change": -1005.51,
        "reason": "REBALANCE",
    }


def test_trade_schema_valid():
    df = pd.DataFrame([_valid_trade_row()])
    TRADE_SCHEMA.validate(df)


def test_trade_schema_rejects_bad_side():
    row = _valid_trade_row()
    row["side"] = "HOLD"
    df = pd.DataFrame([row])
    with pytest.raises(pa.errors.SchemaError):
        TRADE_SCHEMA.validate(df)


def test_trade_schema_rejects_bad_code():
    row = _valid_trade_row()
    row["code"] = "60000"  # 5 digits
    df = pd.DataFrame([row])
    with pytest.raises(pa.errors.SchemaError):
        TRADE_SCHEMA.validate(df)


def test_position_schema_valid():
    df = pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"),
        "code": "600000",
        "shares": 100,
        "avg_cost": 10.0,
        "market_value": 1010.0,
        "unrealized_pnl": 10.0,
        "weight": 0.01,
    }])
    POSITION_SCHEMA.validate(df)


def test_nav_schema_valid():
    df = pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"),
        "nav": 1.0,
        "cash": 100000.0,
        "position_value": 0.0,
        "total_value": 100000.0,
        "ret_daily": 0.0,
        "ret_cum": 0.0,
        "turnover_daily": 0.0,
        "n_holdings": 0,
    }])
    NAV_SCHEMA.validate(df)


def test_metrics_schema_valid():
    df = pd.DataFrame([{
        "run_id": "x_2022-01-04_2023-12-29_abcd1234",
        "strategy": "x",
        "start": pd.Timestamp("2022-01-04"),
        "end": pd.Timestamp("2023-12-29"),
        "ret_annual": 0.1,
        "ret_total": 0.2,
        "vol_annual": 0.15,
        "sharpe": 0.7,
        "sortino": 0.9,
        "max_drawdown": -0.1,
        "max_dd_duration_days": 30,
        "calmar": 1.0,
        "win_rate_daily": 0.52,
        "win_rate_monthly": 0.6,
        "turnover_annual": 2.0,
        "total_cost_bps": 30.0,
        "lot_drag_bps": 2.0,
        "fingerprint_sha": "abcd" * 16,
        "refdata_as_of": "2026-04-18",
    }])
    METRICS_SCHEMA.validate(df)
