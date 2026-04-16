#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import time
import random
from urllib.parse import urlparse, urlunparse
from instock.core.singleton_proxy import proxys

__author__ = 'myh '
__date__ = '2025/12/31 '

class eastmoney_fetcher:
    """
    东方财富网数据获取器
    封装了Cookie管理、会话管理和请求发送功能
    """

    def __init__(self):
        """初始化获取器"""
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.session = self._create_session()
        self.proxies = proxys().get_proxies()
        self.cookie_refresh_interval = 30 * 60
        self.last_cookie_refresh_at = 0

    def _get_cookie(self):
        """
        获取东方财富网的Cookie
        优先级：环境变量 > 文件 > 默认Cookie
        """
        # 1. 尝试从环境变量获取
        cookie = os.environ.get('EAST_MONEY_COOKIE')
        if cookie:
            # print("环境变量中的Cookie: 已设置")
            return cookie

        # 2. 尝试从文件获取
        cookie_file = Path(os.path.join(self.base_dir, 'config', 'eastmoney_cookie.txt'))
        if cookie_file.exists():
            with open(cookie_file, 'r') as f:
                cookie = f.read().strip()
            if cookie:
                # print("文件中的Cookie: 已设置")
                return cookie

        # 3. 默认Cookie（可能过期，仅作为备选）
        return 'st_si=78948464251292; st_psi=20260205091253851-119144370567-1089607836; st_pvi=07789985376191; st_sp=2026-02-05%2009%3A11%3A13; st_inirUrl=https%3A%2F%2Fxuangu.eastmoney.com%2FResult; st_sn=12; st_asi=20260205091253851-119144370567-1089607836-webznxg.dbssk.qxg-1'

    def _create_session(self):
        """创建并配置会话"""
        session = requests.Session()

        # 配置连接池
        retry_strategy = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=50,  # 增加连接池大小
            pool_maxsize=50  # 增加连接池最大大小
        )

        # 为http和https请求添加适配器
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 设置请求头
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Connection': 'keep-alive',
        }
        session.headers.update(headers)
        # 直接设置 Cookie 请求头，避免生成错误的 Cookie=... 形式
        session.headers.update({'Cookie': self._get_cookie()})
        return session

    def _is_eastmoney_url(self, url):
        return "eastmoney.com" in urlparse(url).netloc

    def _build_cookie_header(self):
        cookie_pairs = []
        for cookie in self.session.cookies:
            cookie_pairs.append(f"{cookie.name}={cookie.value}")
        return "; ".join(cookie_pairs)

    def _refresh_cookie_from_site(self, force=False):
        """从东方财富首页轻量刷新 Cookie，避免长期依赖静态 Cookie。"""
        if not force and time.time() - self.last_cookie_refresh_at < self.cookie_refresh_interval:
            return

        refresh_url = "https://quote.eastmoney.com/"
        last_error = None
        for current_proxies in (self.proxies, None):
            try:
                response = self.session.get(
                    refresh_url,
                    proxies=current_proxies,
                    timeout=8,
                )
                response.raise_for_status()
                cookie_header = self._build_cookie_header()
                if cookie_header:
                    self.session.headers.update({'Cookie': cookie_header})
                self.last_cookie_refresh_at = time.time()
                return
            except requests.exceptions.RequestException as e:
                last_error = e
                if current_proxies is not None:
                    continue

        if force and last_error is not None:
            print(f"Cookie 刷新失败: {last_error}")

    def _normalize_url(self, url):
        """东方财富接口优先走 HTTPS，避免部分 HTTP 入口被服务端直接断开。"""
        if url.startswith("http://") and "eastmoney.com" in url:
            return "https://" + url[len("http://"):]
        return url

    def _candidate_urls(self, url):
        """为部分 Eastmoney 行情接口准备多个可替代主机。"""
        normalized_url = self._normalize_url(url)
        parsed = urlparse(normalized_url)
        hosts = [parsed.netloc]

        if parsed.path == "/api/qt/clist/get" and parsed.netloc.endswith("push2.eastmoney.com"):
            fallback_hosts = [
                "push2.eastmoney.com",
                "80.push2.eastmoney.com",
                "82.push2.eastmoney.com",
                "88.push2.eastmoney.com",
            ]
            hosts.extend(host for host in fallback_hosts if host not in hosts)

        return [urlunparse(parsed._replace(netloc=host)) for host in hosts]

    def _send_request(self, method, url, retry=3, timeout=10, **kwargs):
        """发送请求，优先使用代理，连接异常时回退到直连。"""
        candidate_urls = self._candidate_urls(url)
        last_error = None
        is_eastmoney_request = any(self._is_eastmoney_url(request_url) for request_url in candidate_urls)

        if is_eastmoney_request:
            self._refresh_cookie_from_site()

        for i in range(retry):
            for request_url in candidate_urls:
                for current_proxies in (self.proxies, None):
                    try:
                        response = self.session.request(
                            method=method,
                            url=request_url,
                            proxies=current_proxies,
                            timeout=timeout,
                            **kwargs,
                        )
                        response.raise_for_status()
                        return response
                    except requests.exceptions.RequestException as e:
                        last_error = e
                        if current_proxies is not None:
                            print(f"请求错误: {e}, 尝试直连")
                            continue

                        if request_url != candidate_urls[-1]:
                            print(f"请求错误: {e}, 切换主机重试")
                            continue

                        print(f"请求错误: {e}, 第 {i + 1}/{retry} 次重试")
                        if i < retry - 1:
                            if is_eastmoney_request:
                                self._refresh_cookie_from_site(force=True)
                            time.sleep(random.uniform(1, 3))
                        break

        raise last_error

    def make_request(self, url, params=None, retry=3, timeout=10):
        """
        发送请求
        :param url: 请求URL
        :param params: 请求参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        return self._send_request(
            method="GET",
            url=url,
            params=params,
            retry=retry,
            timeout=timeout,
        )

    def make_post_request(self, url, data=None, json=None, params=None, retry=3, timeout=60):
        """
        发送POST请求
        :param url: 请求URL
        :param data: 请求数据（表单形式）
        :param json: 请求数据（JSON形式）
        :param params: URL参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        return self._send_request(
            method="POST",
            url=url,
            params=params,
            data=data,
            json=json,
            retry=retry,
            timeout=timeout,
        )

    def update_cookie(self, new_cookie):
        """
        更新Cookie
        :param new_cookie: 新的Cookie值
        """
        self.session.headers.update({'Cookie': new_cookie})
