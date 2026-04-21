from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable

import requests
from playwright.sync_api import sync_playwright

from instock.core import eastmoney_cookie_store as cookie_store


TARGET_HOSTS = (
    "push2.eastmoney.com",
    "80.push2.eastmoney.com",
    "82.push2.eastmoney.com",
    "88.push2.eastmoney.com",
)


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    reason: str


def is_eastmoney_cookie(cookie: dict) -> bool:
    normalized_domain = cookie["domain"].lstrip(".")
    return normalized_domain == "eastmoney.com" or normalized_domain.endswith(".eastmoney.com")


def cookie_matches_target_host(cookie: dict, scheme: str, host: str, request_path: str = "/") -> bool:
    normalized_domain = cookie["domain"].lstrip(".")
    is_domain_cookie = cookie["domain"].startswith(".")
    path = cookie.get("path") or "/"
    if cookie.get("secure") and scheme != "https":
        return False
    if not request_path.startswith(path):
        return False
    if is_domain_cookie:
        return host == normalized_domain or host.endswith("." + normalized_domain)
    return host == normalized_domain


def collect_cookie_string(cookies: Iterable[dict]) -> str:
    selected: dict[str, str] = {}
    ordered_pairs: list[tuple[str, str]] = []
    eastmoney_cookies = [cookie for cookie in cookies if is_eastmoney_cookie(cookie)]
    for host in TARGET_HOSTS:
        matched = [
            cookie for cookie in eastmoney_cookies
            if cookie_matches_target_host(cookie, "https", host, "/")
        ]
        matched.sort(key=lambda item: (item["name"], item["domain"].lstrip("."), item.get("path") or "/", item["value"]))
        for cookie in matched:
            if cookie["name"] in selected:
                continue
            selected[cookie["name"]] = cookie["value"]
            ordered_pairs.append((cookie["name"], cookie["value"]))
    return "; ".join(f"{name}={value}" for name, value in ordered_pairs)


def validate_push2_payload(payload: dict) -> ValidationResult:
    try:
        record = payload["data"]["diff"][0]
        total = payload["data"]["total"]
    except (KeyError, IndexError, TypeError):
        return ValidationResult(False, "malformed-json")
    if not isinstance(total, int) or total <= 0:
        return ValidationResult(False, "empty-total")
    for field in ("f12", "f14", "f2"):
        if field not in record:
            return ValidationResult(False, f"missing-{field}")
        if record[field] is None or record[field] == "":
            return ValidationResult(False, f"empty-{field}")
    return ValidationResult(True, "ok")


VALIDATION_URL = "https://push2.eastmoney.com/api/qt/clist/get"
VALIDATION_PARAMS = {
    "pn": "1",
    "pz": "1",
    "po": "1",
    "np": "1",
    "fltt": "2",
    "invt": "2",
    "fid": "f12",
    "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
    "fields": "f12,f14,f2",
    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
}


@dataclass(frozen=True)
class AcquisitionResult:
    exit_code: int
    cookie: str | None
    validation_reason: str
    observed_status_cookie_names: tuple[str, ...] = ()


def make_fake_clock(values):
    values = iter(values)
    return lambda: next(values)


def validate_cookie(cookie_string: str, session: requests.Session | None = None, sleep_fn=time.sleep) -> ValidationResult:
    session = session or requests.Session()
    transient_statuses = {429, 500, 502, 503, 504}
    backoffs = [0.0, 0.5, 1.0, 2.0]
    for attempt, delay in enumerate(backoffs, start=1):
        if delay:
            sleep_fn(delay)
        response = session.get(
            VALIDATION_URL,
            params=VALIDATION_PARAMS,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Cookie": cookie_string,
            },
            timeout=8,
        )
        if response.status_code in transient_statuses and attempt < len(backoffs):
            continue
        if response.status_code != 200:
            return ValidationResult(False, f"http-{response.status_code}")
        try:
            payload = response.json()
        except ValueError:
            return ValidationResult(False, "invalid-json")
        return validate_push2_payload(payload)
    return ValidationResult(False, "retry-exhausted")


def acquire_cookie_from_context(context, page, browser, deadline=None, timeout_seconds=None, now_fn=time.monotonic, sleep_fn=time.sleep, validate_fn=validate_cookie):
    attempted = set()
    next_validation_not_before = 0.0
    pending_candidate = None

    while True:
        if page.is_closed() or not browser.is_connected():
            return AcquisitionResult(exit_code=3, cookie=None, validation_reason="browser-closed")

        try:
            cookies = context.cookies()
        except Exception as exc:
            if "closed" in str(exc).lower():
                return AcquisitionResult(exit_code=3, cookie=None, validation_reason="browser-closed")
            raise

        now = now_fn()

        candidate = collect_cookie_string(cookies)
        if candidate and candidate not in attempted:
            try:
                cookie_store.reject_control_characters(candidate)
            except ValueError:
                candidate = None

        if candidate and candidate not in attempted:
            if now >= next_validation_not_before:
                attempted.add(candidate)
                result = validate_fn(candidate)
                next_validation_not_before = now + 3.0
                if result.ok:
                    status_names = tuple(sorted(cookie["name"] for cookie in cookies if cookie["name"].startswith("st_")))
                    return AcquisitionResult(exit_code=0, cookie=candidate, validation_reason=result.reason, observed_status_cookie_names=status_names)
            elif now <= deadline:
                pending_candidate = candidate

        if now > deadline:
            if pending_candidate and pending_candidate not in attempted:
                attempted.add(pending_candidate)
                result = validate_fn(pending_candidate)
                if result.ok:
                    return AcquisitionResult(exit_code=0, cookie=pending_candidate, validation_reason=result.reason)
                return AcquisitionResult(exit_code=4, cookie=None, validation_reason=result.reason)
            return AcquisitionResult(exit_code=3 if not attempted else 4, cookie=None, validation_reason="timeout")

        sleep_fn(2.0)


def acquire_cookie_via_browser(browser_name: str, timeout_seconds: int, playwright_factory=None, now_fn=time.monotonic, sleep_fn=time.sleep, validate_fn=validate_cookie):
    target_url = "https://quote.eastmoney.com/center/gridlist.html#hs_a_board"
    if playwright_factory is None:
        playwright_factory = sync_playwright
    with playwright_factory() as pw:
        if browser_name == "chromium":
            browser = pw.chromium.launch()
        elif browser_name == "chrome":
            browser = pw.chromium.launch(channel="chrome")
        else:
            browser = pw.chromium.launch(channel="msedge")

        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            deadline = now_fn() + timeout_seconds
            return acquire_cookie_from_context(
                context=context,
                page=page,
                browser=browser,
                deadline=deadline,
                now_fn=now_fn,
                sleep_fn=sleep_fn,
                validate_fn=validate_fn,
            )
        finally:
            close_ctx = getattr(context, "close", None)
            if callable(close_ctx):
                close_ctx()
            close_browser = getattr(browser, "close", None)
            if callable(close_browser):
                close_browser()
