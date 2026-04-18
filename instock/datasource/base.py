from __future__ import annotations
from datetime import date
from typing import Protocol, runtime_checkable

import pandas as pd


class DataSourceError(Exception):
    """Base error for the datasource layer."""


class SchemaValidationError(DataSourceError):
    """Raised when a DataSource method's return fails its pandera contract."""


@runtime_checkable
class IDataSource(Protocol):
    def get_ohlcv(
        self, code: str, start: date, end: date, adjust: str = "qfq"
    ) -> pd.DataFrame: ...

    def get_fundamentals_pit(
        self, code: str, fields: list[str]
    ) -> pd.DataFrame: ...

    def get_lhb(self, start: date, end: date) -> pd.DataFrame: ...

    def get_north_bound(self, start: date, end: date) -> pd.DataFrame: ...

    def get_money_flow(
        self, code: str, start: date, end: date
    ) -> pd.DataFrame: ...

    def get_index_member(self, index_code: str, at: date) -> list[str]: ...

    def get_trade_calendar(self, start: date, end: date) -> list[date]: ...
