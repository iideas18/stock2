import pytest
from instock.datasource.registry import get_source
from instock.datasource.akshare_source import AkShareSource


def test_default_returns_akshare():
    src = get_source()
    assert isinstance(src, AkShareSource)


def test_unknown_name_raises():
    with pytest.raises(ValueError):
        get_source("nonexistent")


def test_tushare_not_enabled():
    with pytest.raises(ValueError):
        get_source("tushare")
