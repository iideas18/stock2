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


_FIN_RENAME = {
    "报告期": "report_period",
    "公告日期": "announcement_date",
    "营业收入": "revenue",
    "净利润": "net_profit",
    "总资产": "total_assets",
    "净资产": "total_equity",
    "经营活动现金流量净额": "operating_cf",
    "净资产收益率": "roe",
    "毛利率": "gross_margin",
    "净利率": "net_margin",
    "市盈率": "pe",
    "市净率": "pb",
    "市销率": "ps",
}


_LHB_RENAME = {
    "交易日": "trade_date",
    "代码": "code",
    "营业部名称": "seat",
    "买入金额": "buy_amount",
    "卖出金额": "sell_amount",
    "上榜原因": "reason",
}


_NORTH_RENAME = {
    "持股日期": "trade_date",
    "股票代码": "code",
    "持股数量": "hold_shares",
    "持股市值比例": "hold_ratio",
}


_FLOW_RENAME = {
    "日期": "trade_date",
    "主力净流入-净额": "main_net_inflow",
    "大单净流入-净额": "big_order_inflow",
}


class AkShareSource(IDataSource):
    def __init__(self) -> None:
        self._limiter = RateLimiter(min_interval=0.2)

    # ---------- OHLCV ----------
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

    # ---------- Fundamentals (PIT) ----------
    def get_fundamentals_pit(self, code: str, fields: list[str]) -> pd.DataFrame:
        raw = self._fetch_fundamentals_pit(code)
        if raw is None or raw.empty:
            return _empty_like(schemas.FUNDAMENTALS_PIT_SCHEMA)

        df = raw.rename(columns=_FIN_RENAME).copy()
        df["code"] = code
        df["report_period"] = pd.to_datetime(df["report_period"])
        df["announcement_date"] = pd.to_datetime(df["announcement_date"])
        wanted = ["code", "report_period", "announcement_date"] + [
            f for f in fields if f in df.columns
        ]
        df = df[wanted]
        return _validate(schemas.FUNDAMENTALS_PIT_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_fundamentals_pit(self, code: str) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_financial_abstract(symbol=code)

    # ---------- LHB ----------
    def get_lhb(self, start: date, end: date) -> pd.DataFrame:
        raw = self._fetch_lhb(start, end)
        if raw is None or raw.empty:
            return _empty_like(schemas.LHB_SCHEMA)

        df = raw.rename(columns=_LHB_RENAME).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["buy_amount"] = df["buy_amount"].astype(float)
        df["sell_amount"] = df["sell_amount"].astype(float)
        df["seat"] = df["seat"].astype(str)
        df["reason"] = df["reason"].astype(str)
        df = df[["trade_date", "code", "seat", "buy_amount", "sell_amount", "reason"]]
        return _validate(schemas.LHB_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_lhb(self, start: date, end: date) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_lhb_detail_em(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )

    # ---------- North bound ----------
    def get_north_bound(self, start: date, end: date) -> pd.DataFrame:
        raw = self._fetch_north_bound()
        if raw is None or raw.empty:
            return _empty_like(schemas.NORTH_BOUND_SCHEMA)

        df = raw.rename(columns=_NORTH_RENAME).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df[
            (df["trade_date"] >= pd.Timestamp(start))
            & (df["trade_date"] <= pd.Timestamp(end))
        ]
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["hold_shares"] = df["hold_shares"].astype(float)
        df["hold_ratio"] = pd.to_numeric(df.get("hold_ratio"), errors="coerce")
        df = df[["trade_date", "code", "hold_shares", "hold_ratio"]]
        return _validate(schemas.NORTH_BOUND_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_north_bound(self) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_hsgt_hold_stock_em()

    # ---------- Money flow ----------
    def get_money_flow(self, code: str, start: date, end: date) -> pd.DataFrame:
        raw = self._fetch_money_flow(code)
        if raw is None or raw.empty:
            return _empty_like(schemas.MONEY_FLOW_SCHEMA)

        df = raw.rename(columns=_FLOW_RENAME).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df[
            (df["trade_date"] >= pd.Timestamp(start))
            & (df["trade_date"] <= pd.Timestamp(end))
        ]
        df["code"] = code
        df["main_net_inflow"] = df["main_net_inflow"].astype(float)
        df["big_order_inflow"] = df["big_order_inflow"].astype(float)
        df = df[["trade_date", "code", "main_net_inflow", "big_order_inflow"]]
        return _validate(schemas.MONEY_FLOW_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_money_flow(self, code: str) -> pd.DataFrame:
        self._limiter.wait()
        market = "sh" if code.startswith(("6", "9")) else "sz"
        return ak.stock_individual_fund_flow(stock=code, market=market)

    # ---------- Index member ----------
    def get_index_member(self, index_code: str, at: date) -> list[str]:
        raw = self._fetch_index_member(index_code)
        if raw is None or raw.empty:
            return []
        col = "品种代码" if "品种代码" in raw.columns else raw.columns[0]
        return sorted(raw[col].astype(str).str.zfill(6).unique().tolist())

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_index_member(self, index_code: str) -> pd.DataFrame:
        self._limiter.wait()
        return ak.index_stock_cons(symbol=index_code)

    # ---------- Trade calendar ----------
    def get_trade_calendar(self, start: date, end: date) -> list[date]:
        raw = self._fetch_trade_calendar()
        if raw is None or raw.empty:
            return []
        ts = pd.to_datetime(raw["trade_date"])
        mask = (ts >= pd.Timestamp(start)) & (ts <= pd.Timestamp(end))
        return [d.date() for d in ts[mask]]

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_trade_calendar(self) -> pd.DataFrame:
        self._limiter.wait()
        return ak.tool_trade_date_hist_sina()


def _empty_like(schema: Any) -> pd.DataFrame:
    cols = list(schema.columns.keys())
    return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})


def _validate(schema: Any, df: pd.DataFrame) -> pd.DataFrame:
    try:
        return schema.validate(df, lazy=True)
    except (pa_errors.SchemaError, pa_errors.SchemaErrors) as err:
        raise SchemaValidationError(str(err)) from err
