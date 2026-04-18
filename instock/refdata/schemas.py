"""Pandera schemas + error types for refdata (industry/listing/ST)."""
from __future__ import annotations

import pandera.pandas as pa


class RefdataNotAvailable(Exception):
    """Raised when a refdata resource has no snapshot satisfying the query."""


INDUSTRY_SNAPSHOT_SCHEMA = pa.DataFrameSchema(
    {
        "code":          pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "industry":      pa.Column(str, pa.Check.str_length(min_value=1)),
        "snapshot_date": pa.Column("datetime64[ns]"),
    },
    unique=["code"],
    strict=False,
    coerce=True,
)


LISTING_DATES_SCHEMA = pa.DataFrameSchema(
    {
        "code":         pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "listing_date": pa.Column("datetime64[ns]"),
    },
    unique=["code"],
    strict=False,
    coerce=True,
)


ST_SNAPSHOT_SCHEMA = pa.DataFrameSchema(
    {
        "code":          pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "is_st":         pa.Column(bool),
        "snapshot_date": pa.Column("datetime64[ns]"),
    },
    unique=["code"],
    strict=False,
    coerce=True,
)
