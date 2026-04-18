"""AkShare-backed IDataSource implementation."""
from __future__ import annotations

from datetime import date
from typing import Any

import akshare as ak
import pandas as pd
import pandera.errors as pa_errors

from .base import IDataSource, DataSourceError, SchemaValidationError
from .io import RateLimiter, with_retry
from . import schemas


_OHLCV_RENAME = {
    "日期": "date", "开盘": "open", "收盘": "close",
    "最高": "high", "最低": "low",
    "成交量": "volume", "成交额": "amount",
}


class AkShareSource(IDataSource):
    def __init__(self) -> None:
        self._limiter = RateLimiter(min_interval=0.2)

    def get_ohlcv(
        self, code: str, start: date, end: date, adjust: str = "qfq"
    ) -> pd.DataFrame:
        raw = self._fetch_ohlcv(code, start, end, adjust)
        if raw is None or raw.empty:
            return _empty_like(schemas.OHLCV_SCHEMA)

        df = raw.rename(columns=_OHLCV_RENAME).copy()
        df["date"] = pd.to_datetime(df["date"])
        df["code"] = code
        df = df[["date", "code", "open", "high", "low", "close", "volume", "amount"]]
        df[["open", "high", "low", "close", "volume", "amount"]] = (
            df[["open", "high", "low", "close", "volume", "amount"]].astype(float)
        )
        return _validate(schemas.OHLCV_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_ohlcv(
        self, code: str, start: date, end: date, adjust: str
    ) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust=adjust,
        )

    def get_fundamentals_pit(self, code: str, fields: list[str]) -> pd.DataFrame:
        raise NotImplementedError

    def get_lhb(self, start: date, end: date) -> pd.DataFrame:
        raise NotImplementedError

    def get_north_bound(self, start: date, end: date) -> pd.DataFrame:
        raise NotImplementedError

    def get_money_flow(self, code: str, start: date, end: date) -> pd.DataFrame:
        raise NotImplementedError

    def get_index_member(self, index_code: str, at: date) -> list[str]:
        raise NotImplementedError

    def get_trade_calendar(self, start: date, end: date) -> list[date]:
        raise NotImplementedError


def _empty_like(schema: Any) -> pd.DataFrame:
    cols = list(schema.columns.keys())
    return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})


def _validate(schema: Any, df: pd.DataFrame) -> pd.DataFrame:
    try:
        return schema.validate(df, lazy=True)
    except (pa_errors.SchemaError, pa_errors.SchemaErrors) as err:
        raise SchemaValidationError(str(err)) from err
