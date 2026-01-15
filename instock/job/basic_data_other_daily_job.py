#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
#%%
import logging
import concurrent.futures
import os.path
import sys
import pandas as pd

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf

__author__ = 'myh '
__date__ = '2023/3/10 '

# 每日股票龙虎榜
def save_nph_stock_lhb_data(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stock_lhb_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_lHB['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_lHB['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_lhb_data处理异常：{e}")
    stock_spot_buy(date)

# 每日股票龙虎榜(新浪)
def save_nph_stock_top_data(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stock_top_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_TOP['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_TOP['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")

        # Sina LHB "个股上榜统计" does not provide daily change_rate.
        # After inserting, backfill change_rate from our daily spot table (TABLE_CN_STOCK_SPOT).
        try:
            date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
            spot_table = tbs.TABLE_CN_STOCK_SPOT['name']
            if mdb.checkTableIsExist(spot_table) and mdb.checkTableIsExist(table_name):
                update_sql = (
                    f"UPDATE `{table_name}` t "
                    f"JOIN `{spot_table}` s ON t.`date` = s.`date` AND t.`code` = s.`code` "
                    f"SET t.`change_rate` = s.`change_rate` "
                    f"WHERE t.`date` = '{date_str}' AND t.`change_rate` IS NULL"
                )
                mdb.executeSql(update_sql)
        except Exception as e:
            logging.error(f"basic_data_other_daily_job.save_stock_top_data 回填change_rate异常：{e}")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_top_data处理异常：{e}")
    stock_spot_buy(date)


# 每日股票资金流向
def save_nph_stock_fund_flow_data(date, before=True):
    if before:
        return

    try:
        times = tuple(range(4))
        results = run_check_stock_fund_flow(times)
        if results is None:
            return

        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    r.drop(columns=['name', 'new_price'], inplace=True)
                    data = pd.merge(data, r, on=['code'], how='left')

        if data is None or len(data.index) == 0:
            return

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        table_name = tbs.TABLE_CN_STOCK_FUND_FLOW['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_fund_flow_data处理异常：{e}")


def run_check_stock_fund_flow(times):
    data = {}
    try:
        for k in times :
            _data = stf.fetch_stocks_fund_flow(k)
            if _data is not None:
                data[k] = _data
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_fund_flow处理异常：{e}")
    # try:
    #     with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
    #         future_to_data = {executor.submit(stf.fetch_stocks_fund_flow, k): k for k in times}
    #         for future in concurrent.futures.as_completed(future_to_data):
    #             _time = future_to_data[future]
    #             try:
    #                 _data_ = future.result()
    #                 if _data_ is not None:
    #                     data[_time] = _data_
    #             except Exception as e:
    #                 logging.error(f"basic_data_other_daily_job.run_check_stock_fund_flow处理异常：代码{e}")
    # except Exception as e:
    #     logging.error(f"basic_data_other_daily_job.run_check_stock_fund_flow处理异常：{e}")
    if not data:
        return None
    else:
        return data


# 每日行业资金流向
def save_nph_stock_sector_fund_flow_data(date, before=True):
    if before:
        return

    # times = tuple(range(2))
    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
    #     {executor.submit(stock_sector_fund_flow_data, date, k): k for k in times}
    stock_sector_fund_flow_data(date, 0)
    stock_sector_fund_flow_data(date, 1)

def stock_sector_fund_flow_data(date, index_sector):
    try:
        times = tuple(range(3))
        results = run_check_stock_sector_fund_flow(index_sector, times)
        if results is None:
            return

        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    data = pd.merge(data, r, on=['name'], how='left')

        if data is None or len(data.index) == 0:
            return

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        if index_sector == 0:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY
        else:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT
        table_name = tbs_table['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs_table['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`name`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_sector_fund_flow_data处理异常：{e}")


def run_check_stock_sector_fund_flow(index_sector, times):
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
            future_to_data = {executor.submit(stf.fetch_stocks_sector_fund_flow, index_sector, k): k for k in times}
            for future in concurrent.futures.as_completed(future_to_data):
                _time = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[_time] = _data_
                except Exception as e:
                    logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：代码{e}")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：{e}")
    if not data:
        return None
    else:
        return data


# 每日股票分红配送
def save_nph_stock_bonus(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stocks_bonus(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_BONUS['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BONUS['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_bonus处理异常：{e}")


# 基本面选股
def stock_spot_buy(date):
    try:
        _table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        sql = f'''SELECT * FROM `{_table_name}` WHERE `date` = '{date}' and 
                `pe9` > 0 and `pe9` <= 20 and `pbnewmrq` <= 10 and `roe_weight` >= 15'''
        with mdb.get_connection() as conn:
            data = pd.read_sql(sql=sql, con=conn)
        data = data.drop_duplicates(subset="code", keep="last")
        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_SPOT_BUY['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT_BUY['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_spot_buy处理异常：{e}")


# 每日早盘抢筹
def stock_chip_race_open_data(date):
    try:
        data = stf.fetch_stock_chip_race_open(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_chip_race_open_data：{e}")


# 每日涨停原因
def stock_limitup_reason_data(date):
    try:
        data = stf.fetch_stock_limitup_reason(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_LIMITUP_REASON['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_limitup_reason_data：{e}")

def main():
    print("开始执行股票其它基础数据表每日任务")
    runt.run_with_args(save_nph_stock_top_data)
    print("股票龙虎榜数据表每日任务完成")
    runt.run_with_args(save_nph_stock_lhb_data)
    runt.run_with_args(save_nph_stock_bonus)
    print("股票分红配送数据表每日任务完成") 
    runt.run_with_args(save_nph_stock_fund_flow_data)
    print("股票资金流向数据表每日任务完成")
    runt.run_with_args(save_nph_stock_sector_fund_flow_data)
    print("股票板块资金流向数据表每日任务完成")
    runt.run_with_args(stock_chip_race_open_data)
    print("股票早盘抢筹数据表每日任务完成")
    runt.run_with_args(stock_limitup_reason_data)
    print("股票每日涨停原因数据表完成")

# main函数入口
if __name__ == '__main__':
    main()

 # %%
