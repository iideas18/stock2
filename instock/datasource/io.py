"""Retry / rate-limit helpers used by every DataSource implementation."""
from __future__ import annotations

import functools
import logging
import time
from typing import Callable, TypeVar

from .base import DataSourceError

log = logging.getLogger(__name__)
T = TypeVar("T")


def with_retry(max_attempts: int = 3, base_delay: float = 0.5) -> Callable:
    """Exponential-backoff retry. Wraps final failure in DataSourceError."""

    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    sleep_for = base_delay * (2 ** (attempt - 1))
                    log.warning(
                        "retry %s/%s on %s: %s",
                        attempt, max_attempts, fn.__name__, exc,
                    )
                    time.sleep(sleep_for)
            raise DataSourceError(
                f"{fn.__name__} failed after {max_attempts} attempts: {last_exc}"
            ) from last_exc

        return wrapper

    return deco


class RateLimiter:
    """Simple monotonic-time min-interval limiter."""

    def __init__(self, min_interval: float = 0.2) -> None:
        self._min = min_interval
        self._last = 0.0

    def wait(self) -> None:
        now = time.perf_counter()
        delta = now - self._last
        if delta < self._min:
            time.sleep(self._min - delta)
        self._last = time.perf_counter()
