#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
#%%
import logging
import os.path
import sys
from abc import ABC

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
from tornado import gen

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(format='%(asctime)s %(message)s', filename=os.path.join(log_path, 'stock_web.log'))
logging.getLogger().setLevel(logging.ERROR)
import instock.lib.torndb as torndb
import instock.lib.database as mdb
import instock.lib.version as version
import instock.job.init_job as init_job
import instock.web.dataTableHandler as dataTableHandler
import instock.web.dataIndicatorsHandler as dataIndicatorsHandler
import instock.web.base as webBase
import instock.web.limitupReasonMindmapHandler as limitupReasonMindmapHandler
import instock.web.fetchHistHandler as fetchHistHandler

__author__ = 'myh '
__date__ = '2023/3/10 '


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # 设置路由
            (r"/", HomeHandler),
            (r"/instock/", HomeHandler),
            # 使用datatable 展示报表数据模块。
            (r"/instock/api_data", dataTableHandler.GetStockDataHandler),
            (r"/instock/data", dataTableHandler.GetStockHtmlHandler),
            # 获得股票指标数据。
            (r"/instock/data/indicators", dataIndicatorsHandler.GetDataIndicatorsHandler),
            # 涨停原因揭密 mindmap
            (r"/instock/data/limitup_reason/mindmap", limitupReasonMindmapHandler.LimitupReasonMindmapHandler),
            # 加入关注
            (r"/instock/control/attention", dataIndicatorsHandler.SaveCollectHandler),

            # 历史数据抓取控制面板（1年）
            (r"/instock/control/fetch_hist", fetchHistHandler.FetchHistControlPanelHandler),
            (r"/instock/api_fetch_hist_1y", fetchHistHandler.FetchHistOneYearApiHandler),

            # 批量抓取历史数据 + 运行每日作业
            (r"/instock/api_fetch_hist_batch_start", fetchHistHandler.FetchHistBatchStartHandler),
            (r"/instock/api_fetch_hist_batch_status", fetchHistHandler.FetchHistBatchStatusHandler),
            (r"/instock/api_run_daily_job_start", fetchHistHandler.RunDailyJobStartHandler),
            (r"/instock/api_run_daily_job_status", fetchHistHandler.RunDailyJobStatusHandler),
        ]
        settings = dict(  # 配置
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=False,  # True,
            # cookie加密
            cookie_secret="027bb1b670eddf0392cdda8709268a17b58b7",
            debug=True,
        )
        super(Application, self).__init__(handlers, **settings)

        # Ensure required tables exist (safe: uses CREATE TABLE IF NOT EXISTS).
        try:
            init_job.main()
        except Exception as e:
            logging.error(f"web_service.Application 初始化数据库表异常：{e}")
        # Have one global connection to the blog DB across all handlers
        self.db = torndb.Connection(**mdb.MYSQL_CONN_TORNDB)


# 首页handler。
class HomeHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        self.render("index.html",
                    stockVersion=version.__version__,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


def main():
    # tornado.options.parse_command_line()
    tornado.options.options.logging = None

    http_server = tornado.httpserver.HTTPServer(Application())
    port = 9988
    http_server.listen(port)

    print(f"服务已启动，web地址 : http://localhost:{port}/")
    logging.error(f"服务已启动，web地址 : http://localhost:{port}/")

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()

# %%
