from __future__ import annotations

from instock.monitoring import webhook as wh


class _FakeResp:
    def __init__(self, status: int):
        self.status_code = status
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_empty_url_false():
    assert wh.post_webhook("", {"a": 1}) is False


def test_success_true(monkeypatch):
    captured = {}
    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeResp(200)
    monkeypatch.setattr(wh.requests, "post", fake_post)
    assert wh.post_webhook("http://x", {"a": 1}) is True
    assert captured["url"] == "http://x"
    assert captured["json"] == {"a": 1}


def test_exception_swallowed(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("network down")
    monkeypatch.setattr(wh.requests, "post", boom)
    assert wh.post_webhook("http://x", {"a": 1}) is False


def test_http_error_swallowed(monkeypatch):
    monkeypatch.setattr(
        wh.requests, "post",
        lambda url, json, timeout: _FakeResp(500),
    )
    assert wh.post_webhook("http://x", {"a": 1}) is False
