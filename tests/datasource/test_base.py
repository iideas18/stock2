import pytest
from instock.datasource.base import (
    IDataSource,
    DataSourceError,
    SchemaValidationError,
)


def test_exceptions_are_importable():
    assert issubclass(DataSourceError, Exception)
    assert issubclass(SchemaValidationError, DataSourceError)


def test_protocol_has_required_methods():
    required = {
        "get_ohlcv", "get_fundamentals_pit", "get_lhb",
        "get_north_bound", "get_money_flow",
        "get_index_member", "get_trade_calendar",
    }
    assert required.issubset(set(dir(IDataSource)))
