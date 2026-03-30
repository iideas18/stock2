#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
#%%
import logging
import concurrent.futures
import pandas as pd
import datetime
import os
import os.path
import re
import sys
import time
import heapq

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.lib.trade_time as trd
from instock.core.singleton_stock import stock_hist_data
from instock.core.stockfetch import fetch_stock_top_entity_data

__author__ = 'myh '
__date__ = '2023/3/10 '


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


logger = logging.getLogger(__name__)


def _ensure_logging_configured():
    """Ensure we have at least one handler when running standalone.

    When called from execute_daily_job.py, logging is already configured.
    """
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
        )


def _parse_yyyy_mm_dd(date_str: str) -> datetime.date:
    tmp_year, tmp_month, tmp_day = date_str.split("-")
    return datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()


def _iter_trade_dates_from_argv():
    """Yield trade dates from argv.

    Note: this runs dates sequentially. The per-stock strategy checks already use
    concurrency; running multiple dates and multiple strategies concurrently tends
    to oversubscribe threads and slow everything down.
    """
    argv = sys.argv

    # 区间作业 python strategy_data_daily_job.py 2023-03-01 2023-03-21
    if len(argv) == 3 and _DATE_RE.match(str(argv[1])) and _DATE_RE.match(str(argv[2])):
        start_date = _parse_yyyy_mm_dd(argv[1])
        end_date = _parse_yyyy_mm_dd(argv[2])
        run_date = start_date
        while run_date <= end_date:
            if trd.is_trade_date(run_date):
                yield run_date
            run_date += datetime.timedelta(days=1)
        return

    # N个时间作业 python strategy_data_daily_job.py 2023-03-01,2023-03-02
    if len(argv) == 2:
        raw = str(argv[1])
        if raw:
            parts = [p.strip() for p in raw.split(',') if p.strip()]
            if parts and all(_DATE_RE.match(p) for p in parts):
                for p in parts:
                    run_date = _parse_yyyy_mm_dd(p)
                    if trd.is_trade_date(run_date):
                        yield run_date
                return

    # 当前时间作业 python strategy_data_daily_job.py
    run_date, run_date_nph = trd.get_trade_date_last()
    yield run_date_nph


def _env_int(name: str, default: int) -> int:
    try:
        v = os.environ.get(name)
        if v is None or str(v).strip() == "":
            return default
        return int(str(v).strip())
    except Exception:
        return default


def _default_stock_workers() -> int:
    cpu = os.cpu_count() or 4
    # Strategy checks often run Python-level logic; too many threads hurts.
    return max(4, min(16, cpu * 2))


def _prepare_one_strategy(date, strategy, stocks_data, stock_tops=None):
    try:
        t0 = time.perf_counter()
        table_name = strategy['name']
        strategy_func = strategy['func']
        results = run_check(strategy_func, table_name, stocks_data, date, stock_tops=stock_tops)
        if results is None:
            logger.info(
                "strategy=%s date=%s matched=0 cost=%.3fs",
                table_name,
                date.strftime('%Y-%m-%d'),
                time.perf_counter() - t0,
            )
            return

        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            t_del0 = time.perf_counter()
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            t_del = time.perf_counter() - t_del0
            cols_type = None
        else:
            t_del = 0.0
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES[0]['columns'])

        data = pd.DataFrame(results)
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        data.columns = columns
        # Add backtest columns without creating extra rows.
        for col in tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns']):
            if col not in data.columns:
                data[col] = None
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        t_ins0 = time.perf_counter()
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        t_ins = time.perf_counter() - t_ins0

        logger.info(
            "strategy=%s date=%s stocks=%d matched=%d delete=%.3fs insert=%.3fs total=%.3fs",
            table_name,
            date.strftime('%Y-%m-%d'),
            len(stocks_data),
            len(results),
            t_del,
            t_ins,
            time.perf_counter() - t0,
        )

    except Exception as e:
        logger.exception("strategy_data_daily_job.prepare处理异常：%s策略 %s", strategy, e)


def prepare(date):
    """Run all strategies for a single date.

    Performance: load `stocks_data` once per date and reuse it across strategies.
    """
    try:
        t0 = time.perf_counter()
        logger.info("strategy job start date=%s", date.strftime('%Y-%m-%d'))

        t_load0 = time.perf_counter()
        stocks_data = stock_hist_data(date=date).get_data()
        t_load = time.perf_counter() - t_load0
        if not stocks_data:
            logger.warning(
                "strategy job empty stocks date=%s load=%.3fs",
                date.strftime('%Y-%m-%d'),
                t_load,
            )
            return

        logger.info(
            "loaded stocks date=%s count=%d cost=%.3fs",
            date.strftime('%Y-%m-%d'),
            len(stocks_data),
            t_load,
        )

        # Fetch top list once per date; only used by a specific strategy.
        stock_tops = None
        try:
            t_top0 = time.perf_counter()
            stock_tops = fetch_stock_top_entity_data(date)
            if stock_tops is not None:
                stock_tops = set(stock_tops)
                logger.info(
                    "loaded stock tops date=%s count=%d cost=%.3fs",
                    date.strftime('%Y-%m-%d'),
                    len(stock_tops),
                    time.perf_counter() - t_top0,
                )
        except Exception:
            stock_tops = None

        for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
            _prepare_one_strategy(date, strategy, stocks_data, stock_tops=stock_tops)

        logger.info(
            "strategy job done date=%s total=%.3fs",
            date.strftime('%Y-%m-%d'),
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("strategy_data_daily_job.prepare处理异常：%s %s", date, e)


def run_check(strategy_fun, table_name, stocks, date, workers=None, stock_tops=None):
    if workers is None:
        workers = _env_int("INSTOCK_STRATEGY_STOCK_WORKERS", _default_stock_workers())

    profile = _env_int("INSTOCK_STRATEGY_PROFILE", 0) == 1
    slow_top_n = _env_int("INSTOCK_STRATEGY_PROFILE_TOP", 20)
    slow_threshold_ms = _env_int("INSTOCK_STRATEGY_PROFILE_THRESHOLD_MS", 250)

    is_check_high_tight = strategy_fun.__name__ == 'check_high_tight' and stock_tops is not None
    data = []
    slow_heap = []  # (elapsed_ms, code)
    try:
        t0 = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            t_submit0 = time.perf_counter()

            def _call_one(k):
                t1 = time.perf_counter()
                try:
                    if is_check_high_tight:
                        ok = strategy_fun(k, stocks[k], date=date, istop=(k[1] in stock_tops))
                    else:
                        ok = strategy_fun(k, stocks[k], date=date)
                    return k, bool(ok), (time.perf_counter() - t1) * 1000.0, None
                except Exception as e:
                    return k, False, (time.perf_counter() - t1) * 1000.0, e

            if is_check_high_tight:
                future_to_data = {executor.submit(_call_one, k): k for k in stocks}
            else:
                future_to_data = {executor.submit(_call_one, k): k for k in stocks}

            t_submit = time.perf_counter() - t_submit0
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    k, ok, elapsed_ms, err = future.result()
                    if err is not None:
                        logger.error(
                            "strategy_data_daily_job.run_check处理异常：%s代码 %s 策略=%s",
                            stock[1],
                            err,
                            table_name,
                        )
                    if ok:
                        data.append(k)

                    if profile and elapsed_ms >= float(slow_threshold_ms):
                        if len(slow_heap) < slow_top_n:
                            heapq.heappush(slow_heap, (elapsed_ms, k[1]))
                        else:
                            heapq.heappushpop(slow_heap, (elapsed_ms, k[1]))
                except Exception as e:
                    logger.error(
                        "strategy_data_daily_job.run_check处理异常：%s代码 %s 策略=%s",
                        stock[1],
                        e,
                        table_name,
                    )

            total = time.perf_counter() - t0

            logger.info(
                "run_check done strategy=%s date=%s workers=%d stocks=%d matched=%d submit=%.3fs total=%.3fs",
                table_name,
                date.strftime('%Y-%m-%d'),
                workers,
                len(stocks),
                len(data),
                t_submit,
                total,
            )

            if profile and slow_heap:
                slow_heap.sort(reverse=True)
                msg = ", ".join([f"{code}:{ms:.0f}ms" for ms, code in slow_heap])
                logger.info(
                    "run_check slow strategy=%s date=%s threshold_ms=%d top=%d [%s]",
                    table_name,
                    date.strftime('%Y-%m-%d'),
                    slow_threshold_ms,
                    slow_top_n,
                    msg,
                )
    except Exception as e:
        logger.exception("strategy_data_daily_job.run_check处理异常：%s 策略=%s", e, table_name)
    if not data:
        return None
    else:
        return data


def main():
    _ensure_logging_configured()
    for date in _iter_trade_dates_from_argv():
        prepare(date)


# main函数入口
if __name__ == '__main__':
    main()

# %%
