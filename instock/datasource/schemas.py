"""pandera schema contracts for IDataSource method returns."""
from __future__ import annotations

import pandera as pa


OHLCV_SCHEMA = pa.DataFrameSchema(
    {
        "date":   pa.Column("datetime64[ns]"),
        "code":   pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "open":   pa.Column(float, pa.Check.gt(0)),
        "high":   pa.Column(float, pa.Check.gt(0)),
        "low":    pa.Column(float, pa.Check.gt(0)),
        "close":  pa.Column(float, pa.Check.gt(0)),
        "volume": pa.Column(float, pa.Check.ge(0)),
        "amount": pa.Column(float, pa.Check.ge(0)),
    },
    unique=["date", "code"],
    strict=False,
    coerce=True,
)


FUNDAMENTALS_PIT_SCHEMA = pa.DataFrameSchema(
    {
        "code":              pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "report_period":     pa.Column("datetime64[ns]"),
        "announcement_date": pa.Column("datetime64[ns]"),
    },
    unique=["code", "report_period"],
    strict=False,
    coerce=True,
)


LHB_SCHEMA = pa.DataFrameSchema(
    {
        "trade_date":  pa.Column("datetime64[ns]"),
        "code":        pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "seat":        pa.Column(str),
        "buy_amount":  pa.Column(float, pa.Check.ge(0)),
        "sell_amount": pa.Column(float, pa.Check.ge(0)),
        "reason":      pa.Column(str, nullable=True),
    },
    strict=False,
    coerce=True,
)


NORTH_BOUND_SCHEMA = pa.DataFrameSchema(
    {
        "trade_date":     pa.Column("datetime64[ns]"),
        "code":           pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "hold_shares":    pa.Column(float, pa.Check.ge(0)),
        "hold_ratio":     pa.Column(float, nullable=True),
    },
    unique=["trade_date", "code"],
    strict=False,
    coerce=True,
)


MONEY_FLOW_SCHEMA = pa.DataFrameSchema(
    {
        "trade_date":       pa.Column("datetime64[ns]"),
        "code":             pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "main_net_inflow":  pa.Column(float),
        "big_order_inflow": pa.Column(float),
    },
    unique=["trade_date", "code"],
    strict=False,
    coerce=True,
)
