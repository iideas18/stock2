"""Placeholder Tushare implementation. All methods raise NotImplementedError
until a Pro subscription and mapping are added."""
from __future__ import annotations

from datetime import date
import pandas as pd

from .base import IDataSource


class TushareSource(IDataSource):
    def get_ohlcv(self, code, start, end, adjust="qfq"):
        raise NotImplementedError("TushareSource not enabled")
    def get_fundamentals_pit(self, code, fields):
        raise NotImplementedError("TushareSource not enabled")
    def get_lhb(self, start, end):
        raise NotImplementedError("TushareSource not enabled")
    def get_north_bound(self, start, end):
        raise NotImplementedError("TushareSource not enabled")
    def get_money_flow(self, code, start, end):
        raise NotImplementedError("TushareSource not enabled")
    def get_index_member(self, index_code, at):
        raise NotImplementedError("TushareSource not enabled")
    def get_trade_calendar(self, start, end):
        raise NotImplementedError("TushareSource not enabled")
