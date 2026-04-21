"""Benchmark OHLCV loader.

Returns wide DataFrame:
    index = date (DatetimeIndex)
    columns = one per successfully-loaded benchmark code
    values = close price
Missing/failed benchmarks are skipped with a warning.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Sequence

import pandas as pd

from instock.datasource.base import IDataSource

log = logging.getLogger(__name__)


def load_benchmarks(
    source: IDataSource,
    codes: Sequence[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    series_list = []
    for c in codes:
        try:
            df = source.get_ohlcv(c, start, end, adjust="qfq")
            if df.empty:
                log.warning("benchmark %s: empty", c)
                continue
            s = (
                df.set_index("date")["close"]
                .rename(c)
                .sort_index()
            )
            series_list.append(s)
        except Exception as exc:  # noqa: BLE001
            log.warning("benchmark %s: failed (%s)", c, exc)
    if not series_list:
        return pd.DataFrame()
    return pd.concat(series_list, axis=1).sort_index()
