"""Pandera schemas for backtest outputs.

All DataFrames use pandas dtypes; dates are datetime64[ns]; codes are
6-digit strings; enum columns enforced with str_matches.
"""
from __future__ import annotations

import pandera.pandas as pa


class BacktestValidationError(Exception):
    """Raised when a backtest DataFrame violates its schema contract.

    Will be raised by validate_*_invariants helpers added alongside storage (Task 9).
    """


_CODE_RE = r"^\d{6}$"
_SIDE_RE = r"^(BUY|SELL)$"
_REASON_RE = r"^(REBALANCE|ST_FORCE_OUT|DELIST_FORCE_OUT|NO_OP)$"


TRADE_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "code": pa.Column(str, pa.Check.str_matches(_CODE_RE)),
        "side": pa.Column(str, pa.Check.str_matches(_SIDE_RE)),
        "target_w": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
        "filled_shares": pa.Column(int),
        "fill_price": pa.Column(float, pa.Check.ge(0.0)),
        "fill_value": pa.Column(float, pa.Check.ge(0.0)),
        "commission": pa.Column(float, pa.Check.ge(0.0)),
        "stamp_tax": pa.Column(float, pa.Check.ge(0.0)),
        "transfer_fee": pa.Column(float, pa.Check.ge(0.0)),
        "slippage_value": pa.Column(float),
        "gross_value": pa.Column(float),
        "net_cash_change": pa.Column(float),
        "reason": pa.Column(str, pa.Check.str_matches(_REASON_RE)),
    },
    coerce=True,
    strict=False,
)


POSITION_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "code": pa.Column(str, pa.Check.str_matches(_CODE_RE)),
        "shares": pa.Column(int, pa.Check.ge(0)),
        "avg_cost": pa.Column(float, pa.Check.ge(0.0)),
        "market_value": pa.Column(float, pa.Check.ge(0.0)),
        "unrealized_pnl": pa.Column(float),
        "weight": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
    },
    coerce=True,
    strict=False,
)


NAV_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "nav": pa.Column(float, pa.Check.gt(0.0)),
        "cash": pa.Column(float),
        "position_value": pa.Column(float, pa.Check.ge(0.0)),
        "total_value": pa.Column(float, pa.Check.gt(0.0)),
        "ret_daily": pa.Column(float),
        "ret_cum": pa.Column(float),
        "turnover_daily": pa.Column(float, pa.Check.ge(0.0)),
        "n_holdings": pa.Column(int, pa.Check.ge(0)),
    },
    coerce=True,
    strict=False,
)


METRICS_SCHEMA = pa.DataFrameSchema(
    {
        "run_id": pa.Column(str, pa.Check.str_length(min_value=1)),
        "strategy": pa.Column(str, pa.Check.str_length(min_value=1)),
        "start": pa.Column("datetime64[ns]"),
        "end": pa.Column("datetime64[ns]"),
        "ret_annual": pa.Column(float),
        "ret_total": pa.Column(float),
        "vol_annual": pa.Column(float, pa.Check.ge(0.0)),
        "sharpe": pa.Column(float, nullable=True),
        "sortino": pa.Column(float, nullable=True),
        "max_drawdown": pa.Column(float, pa.Check.le(0.0)),
        "max_dd_duration_days": pa.Column(int, pa.Check.ge(0)),
        "calmar": pa.Column(float, nullable=True),
        "win_rate_daily": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
        "win_rate_monthly": pa.Column(
            float, pa.Check.in_range(0.0, 1.0), nullable=True
        ),
        "turnover_annual": pa.Column(float, pa.Check.ge(0.0)),
        "total_cost_bps": pa.Column(float, pa.Check.ge(0.0)),
        "lot_drag_bps": pa.Column(float),
        "fingerprint_sha": pa.Column(str, pa.Check.str_length(min_value=8)),
        "refdata_as_of": pa.Column(str, nullable=True),  # ISO date string; tracks refdata snapshot age for audit
    },
    coerce=True,
    strict=False,
)
