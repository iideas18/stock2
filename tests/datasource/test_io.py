import time
import pytest
from instock.datasource.io import with_retry, RateLimiter
from instock.datasource.base import DataSourceError


def test_with_retry_returns_on_success():
    calls = {"n": 0}

    @with_retry(max_attempts=3, base_delay=0.0)
    def ok():
        calls["n"] += 1
        return 42

    assert ok() == 42
    assert calls["n"] == 1


def test_with_retry_retries_then_succeeds():
    calls = {"n": 0}

    @with_retry(max_attempts=3, base_delay=0.0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("boom")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_with_retry_raises_datasource_error_after_exhaustion():
    @with_retry(max_attempts=2, base_delay=0.0)
    def always_fails():
        raise RuntimeError("down")

    with pytest.raises(DataSourceError):
        always_fails()


def test_rate_limiter_enforces_min_interval():
    rl = RateLimiter(min_interval=0.05)
    t0 = time.perf_counter()
    rl.wait()
    rl.wait()
    elapsed = time.perf_counter() - t0
    assert elapsed >= 0.05
