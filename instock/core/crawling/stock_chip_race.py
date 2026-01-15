#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2025/2/26 12:18
Desc: 通达信抢筹
http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp
"""

import pandas as pd
import requests
from instock.core.singleton_proxy import proxys


def _normalize_positional_df(df: pd.DataFrame, expected_cols) -> pd.DataFrame:
    """Normalize upstream DataFrame to a stable positional schema.

    The upstream endpoint sometimes changes the number of returned fields.
    We keep the first N columns when there are extras, and pad missing columns
    with NULLs when there are fewer.
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    # If the DataFrame was created from a list, columns are usually 0..n-1.
    if all(isinstance(c, int) for c in out.columns):
        if out.shape[1] >= len(expected_cols):
            out = out.iloc[:, :len(expected_cols)]
            out.columns = expected_cols
        else:
            out.columns = expected_cols[:out.shape[1]]
            for col in expected_cols[out.shape[1]:]:
                out[col] = None
            out = out[expected_cols]
        return out

    # Already has named columns; ensure full set exists and order.
    for col in expected_cols:
        if col not in out.columns:
            out[col] = None
    return out[expected_cols]

def stock_chip_race_open(date: str = "") -> pd.DataFrame:
    """
    通达信竞价抢筹_早盘抢筹
    http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp
    :return: 早盘抢筹
    :rtype: pandas.DataFrame
    """
    url = "http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp"
    #sort:1抢筹委托金额, 2抢筹成交金额, 3开盘金额, 4抢筹幅度, 5抢筹占比
    if date=="":
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 0,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC"}]
    else:
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 0,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC", "date": date}]
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 TdxW",
    }

    r = requests.post(url, proxies = proxys().get_proxies(), json=params,headers=headers)
    data_json = r.json()
    data = data_json["datas"]
    if not data:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data)
    temp_df = _normalize_positional_df(
        temp_df,
        [
            "代码",
            "名称",
            "昨收",
            "今开",
            "开盘金额",
            "抢筹幅度",
            "抢筹委托金额",
            "抢筹成交金额",
            "最新价",
            "_",
        ],
    )

    temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce") / 10000
    temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce") / 10000
    temp_df["开盘金额"] = pd.to_numeric(temp_df["开盘金额"], errors="coerce")
    temp_df["抢筹幅度"] = pd.to_numeric(temp_df["抢筹幅度"], errors="coerce")
    temp_df["抢筹委托金额"] = pd.to_numeric(temp_df["抢筹委托金额"], errors="coerce")
    temp_df["抢筹成交金额"] = pd.to_numeric(temp_df["抢筹成交金额"], errors="coerce")
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")

    temp_df["抢筹幅度"] = round(temp_df["抢筹幅度"] * 100, 2)
    temp_df["最新价"] = round(temp_df["最新价"], 2)
    temp_df["涨跌幅"] = round((temp_df["最新价"] / temp_df["昨收"] - 1) * 100, 2)
    temp_df["抢筹占比"] = round((temp_df["抢筹成交金额"] / temp_df["开盘金额"]) * 100, 2)

    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "昨收",
            "今开",
            "开盘金额",
            "抢筹幅度",
            "抢筹委托金额",
            "抢筹成交金额",
            "抢筹占比",
        ]
    ]

    return temp_df

def stock_chip_race_end(date: str = "") -> pd.DataFrame:
    """
    通达信竞价抢筹_尾盘抢筹
    http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp
    :return: 尾盘抢筹
    :rtype: pandas.DataFrame
    """
    url = "http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp"
    #sort:1抢筹委托金额, 2抢筹成交金额, 3开盘金额, 4抢筹幅度, 5抢筹占比
    if date=="":
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 1,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC"}]
    else:
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 1,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC", "date": date}]
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": "TdxW",
    }

    r = requests.post(url, proxies = proxys().get_proxies(), json=params,headers=headers)
    data_json = r.json()
    data = data_json["datas"]
    if not data:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data)
    temp_df = _normalize_positional_df(
        temp_df,
        [
            "代码",
            "名称",
            "昨收",
            "今开",
            "收盘金额",
            "抢筹幅度",
            "抢筹委托金额",
            "抢筹成交金额",
            "最新价",
            "_",
        ],
    )

    temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce") / 10000
    temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce") / 10000
    temp_df["收盘金额"] = pd.to_numeric(temp_df["收盘金额"], errors="coerce")
    temp_df["抢筹幅度"] = pd.to_numeric(temp_df["抢筹幅度"], errors="coerce")
    temp_df["抢筹委托金额"] = pd.to_numeric(temp_df["抢筹委托金额"], errors="coerce")
    temp_df["抢筹成交金额"] = pd.to_numeric(temp_df["抢筹成交金额"], errors="coerce")
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")

    temp_df["抢筹幅度"] = round(temp_df["抢筹幅度"] * 100, 2)
    temp_df["最新价"] = round(temp_df["最新价"], 2)
    temp_df["涨跌幅"] = round((temp_df["最新价"] / temp_df["昨收"] - 1) * 100, 2)
    temp_df["抢筹占比"] = round((temp_df["抢筹成交金额"] / temp_df["收盘金额"]) * 100, 2)

    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "昨收",
            "今开",
            "收盘金额",
            "抢筹幅度",
            "抢筹委托金额",
            "抢筹成交金额",
            "抢筹占比",
        ]
    ]

    return temp_df

if __name__ == "__main__":
    fund_chip_race_open_df = stock_chip_race_open()
    print(fund_chip_race_open_df)

    fund_chip_race_end_df = stock_chip_race_end()
    print(fund_chip_race_end_df)
