#!/usr/local/bin/python
# -*- coding: utf-8 -*-


import logging
import datetime
import concurrent.futures
import sys
import time
import re
import instock.lib.trade_time as trd

__author__ = 'myh '
__date__ = '2023/3/10 '


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_yyyy_mm_dd(date_str: str) -> datetime.date:
    tmp_year, tmp_month, tmp_day = date_str.split("-")
    return datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()


def _argv_has_date_range(argv) -> bool:
    return len(argv) == 3 and _DATE_RE.match(str(argv[1])) and _DATE_RE.match(str(argv[2]))


def _argv_has_date_list(argv) -> bool:
    if len(argv) != 2:
        return False
    raw = str(argv[1])
    if not raw:
        return False
    parts = raw.split(',')
    return all(_DATE_RE.match(p.strip()) for p in parts)


# 通用函数，获得日期参数，支持批量作业。
def run_with_args(run_fun, *args):
    def _run_once(run_date: datetime.date):
        if run_fun.__name__.startswith('save_nph'):
            run_fun(run_date, False, *args)
        elif run_fun.__name__.startswith('save_after_close'):
            run_fun(run_date, *args)
        else:
            run_fun(run_date, *args)

    if _argv_has_date_range(sys.argv):
        # 区间作业 python xxx.py 2023-03-01 2023-03-21
        start_date = _parse_yyyy_mm_dd(sys.argv[1])
        end_date = _parse_yyyy_mm_dd(sys.argv[2])
        run_date = start_date
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                while run_date <= end_date:
                    if trd.is_trade_date(run_date):
                        executor.submit(_run_once, run_date)
                        time.sleep(2)
                    run_date += datetime.timedelta(days=1)
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
    elif _argv_has_date_list(sys.argv):
        # N个时间作业 python xxx.py 2023-03-01,2023-03-02
        dates = sys.argv[1].split(',')
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for date in dates:
                    run_date = _parse_yyyy_mm_dd(date.strip())
                    if trd.is_trade_date(run_date):
                        executor.submit(_run_once, run_date)
                        time.sleep(2)
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
    else:
        # 当前时间作业 python xxx.py
        try:
            run_date, run_date_nph = trd.get_trade_date_last()
            if run_fun.__name__.startswith('save_nph'):
                run_fun(run_date_nph, False, *args)
            elif run_fun.__name__.startswith('save_after_close'):
                run_fun(run_date, *args)
            else:
                run_fun(run_date_nph, *args)
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
