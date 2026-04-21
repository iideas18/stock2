from instock.core import eastmoney_cookie_manager as manager


def test_cookie_matches_domain_cookie_for_subdomain():
    cookie = {"name": "st_si", "value": "1", "domain": ".eastmoney.com", "path": "/", "secure": True}
    assert manager.cookie_matches_target_host(cookie, "https", "push2.eastmoney.com", "/") is True


def test_cookie_matches_host_only_cookie_only_for_exact_host():
    cookie = {"name": "foo", "value": "1", "domain": "push2.eastmoney.com", "path": "/", "secure": False}
    assert manager.cookie_matches_target_host(cookie, "https", "push2.eastmoney.com", "/") is True
    assert manager.cookie_matches_target_host(cookie, "https", "80.push2.eastmoney.com", "/") is False


def test_collect_cookie_string_is_deterministic_across_host_priority():
    cookies = [
        {"name": "b", "value": "2", "domain": ".eastmoney.com", "path": "/", "secure": True},
        {"name": "a", "value": "1", "domain": "push2.eastmoney.com", "path": "/", "secure": True},
    ]
    assert manager.collect_cookie_string(cookies) == "a=1; b=2"


def test_validate_push2_payload_accepts_zero_price_but_not_empty_strings():
    payload = {"data": {"total": 1, "diff": [{"f12": "600519", "f14": "贵州茅台", "f2": 0}]}}
    assert manager.validate_push2_payload(payload).ok is True

    payload["data"]["diff"][0]["f14"] = ""
    assert manager.validate_push2_payload(payload).ok is False


class FakePage:
    def __init__(self):
        self._closed = False
    def is_closed(self):
        return self._closed


class FakeBrowser:
    def __init__(self):
        self._connected = True
    def is_connected(self):
        return self._connected


class FakeContext:
    def __init__(self, snapshots):
        self._snapshots = list(snapshots)
        self._idx = 0
    def cookies(self):
        snapshot = self._snapshots[min(self._idx, len(self._snapshots) - 1)]
        self._idx += 1
        return snapshot


def test_acquire_cookie_validates_each_unique_candidate_once(monkeypatch):
    calls = []

    def fake_validate(cookie_string, *_args, **_kwargs):
        calls.append(cookie_string)
        return manager.ValidationResult(ok=cookie_string == "st_si=2", reason="ok" if cookie_string == "st_si=2" else "bad")

    result = manager.acquire_cookie_from_context(
        context=FakeContext([
            [{"name": "st_si", "value": "1", "domain": ".eastmoney.com", "path": "/", "secure": True}],
            [{"name": "st_si", "value": "1", "domain": ".eastmoney.com", "path": "/", "secure": True}],
            [{"name": "st_si", "value": "2", "domain": ".eastmoney.com", "path": "/", "secure": True}],
        ]),
        page=FakePage(),
        browser=FakeBrowser(),
        deadline=6.0,
        now_fn=manager.make_fake_clock([0.0, 0.0, 2.0, 3.1, 4.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=fake_validate,
    )

    assert result.exit_code == 0
    assert calls == ["st_si=1", "st_si=2"]


def test_acquire_cookie_returns_exit_code_3_when_browser_is_closed():
    page = FakePage()
    page._closed = True

    result = manager.acquire_cookie_from_context(
        context=FakeContext([[]]),
        page=page,
        browser=FakeBrowser(),
        timeout_seconds=3,
        now_fn=manager.make_fake_clock([0.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=lambda *_args, **_kwargs: manager.ValidationResult(ok=False, reason="should-not-run"),
    )

    assert result.exit_code == 3


def test_validate_cookie_retries_transient_statuses(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}
        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = 0
        def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return FakeResponse(502)
            if self.calls == 2:
                return FakeResponse(429)
            return FakeResponse(200, {"data": {"total": 1, "diff": [{"f12": "600519", "f14": "贵州茅台", "f2": 0}]}})

    sleeps = []
    result = manager.validate_cookie("st_si=1", session=FakeSession(), sleep_fn=sleeps.append)

    assert result.ok is True
    assert sleeps == [0.5, 1.0]


def test_acquire_cookie_skips_control_character_candidate_and_keeps_polling():
    calls = []

    def fake_validate(cookie_string, *_args, **_kwargs):
        calls.append(cookie_string)
        return manager.ValidationResult(ok=True, reason="ok")

    result = manager.acquire_cookie_from_context(
        context=FakeContext([
            [{"name": "st_si", "value": "bad\nvalue", "domain": ".eastmoney.com", "path": "/", "secure": True}],
            [{"name": "st_si", "value": "good", "domain": ".eastmoney.com", "path": "/", "secure": True}],
        ]),
        page=FakePage(),
        browser=FakeBrowser(),
        deadline=6.0,
        now_fn=manager.make_fake_clock([0.0, 2.0, 4.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=fake_validate,
    )

    assert result.exit_code == 0
    assert calls == ["st_si=good"]


def test_acquire_cookie_maps_closed_context_exception_to_exit_code_3():
    class RaisingContext:
        def cookies(self):
            raise RuntimeError("browser has been closed")

    result = manager.acquire_cookie_from_context(
        context=RaisingContext(),
        page=FakePage(),
        browser=FakeBrowser(),
        deadline=3.0,
        now_fn=manager.make_fake_clock([0.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=lambda *_args, **_kwargs: manager.ValidationResult(ok=False, reason="should-not-run"),
    )

    assert result.exit_code == 3


def test_acquire_cookie_via_browser_starts_deadline_after_goto(monkeypatch):
    events = []
    captured = {}

    class FakePageWithGoto(FakePage):
        def goto(self, url, wait_until):
            events.append(("goto", url, wait_until))

    class FakeContextWithPage(FakeContext):
        def new_page(self):
            return FakePageWithGoto()

    class FakeBrowserWithContext(FakeBrowser):
        def new_context(self):
            return FakeContextWithPage([[{"name": "st_si", "value": "ok", "domain": ".eastmoney.com", "path": "/", "secure": True}]])
        def close(self):
            events.append(("browser-close",))

    class FakeBrowserType:
        def launch(self, **_kwargs):
            return FakeBrowserWithContext()

    class FakePlaywright:
        chromium = FakeBrowserType()
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(manager, "sync_playwright", lambda: FakePlaywright())
    monkeypatch.setattr(
        manager,
        "acquire_cookie_from_context",
        lambda **kwargs: captured.update(kwargs) or manager.AcquisitionResult(exit_code=0, cookie="st_si=ok", validation_reason="ok"),
    )

    result = manager.acquire_cookie_via_browser("chromium", 300, now_fn=manager.make_fake_clock([10.0]))

    assert result.exit_code == 0
    assert events[0][0] == "goto"
    assert captured["deadline"] == 310.0
