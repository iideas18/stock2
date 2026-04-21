from pathlib import Path

from instock.core import eastmoney_cookie_store as store
from instock.core.eastmoney_fetcher import eastmoney_fetcher


def test_fetcher_prefers_env_cookie(monkeypatch, tmp_path):
    cookie_path = tmp_path / "eastmoney_cookie.txt"
    cookie_path.write_text("file=1\n", encoding="utf-8")
    monkeypatch.setattr(store, "COOKIE_FILE", cookie_path)
    monkeypatch.setenv("EAST_MONEY_COOKIE", "env=1")

    fetcher = eastmoney_fetcher()

    assert fetcher.session.headers["Cookie"] == "env=1"


def test_fetcher_uses_file_cookie_when_env_missing(monkeypatch, tmp_path):
    cookie_path = tmp_path / "eastmoney_cookie.txt"
    cookie_path.write_text("file=1\n", encoding="utf-8")
    monkeypatch.setattr(store, "COOKIE_FILE", cookie_path)
    monkeypatch.delenv("EAST_MONEY_COOKIE", raising=False)

    fetcher = eastmoney_fetcher()

    assert fetcher.session.headers["Cookie"] == "file=1"
