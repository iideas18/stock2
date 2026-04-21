"""Retry / rate-limit helpers used by every DataSource implementation."""
from __future__ import annotations

import functools
import logging
import os
import time
from pathlib import Path
from typing import Callable, TypeVar

import pandas as pd

from .base import DataSourceError

log = logging.getLogger(__name__)
T = TypeVar("T")

_LOG_DEFAULT = Path("data/log/api_calls.parquet")


def log_call(source: str, ok: bool, path: Path | None = None) -> None:
    """Append one structured row to the api_calls parquet log. Never raises."""
    try:
        target = Path(path) if path is not None else Path(
            os.environ.get("INSTOCK_API_CALL_LOG", _LOG_DEFAULT)
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        row = pd.DataFrame(
            [{"ts": pd.Timestamp.now(), "source": source, "ok": bool(ok)}]
        )
        if target.exists():
            old = pd.read_parquet(target)
            pd.concat([old, row], ignore_index=True).to_parquet(
                target, index=False
            )
        else:
            row.to_parquet(target, index=False)
    except Exception as exc:  # noqa: BLE001
        log.warning("log_call failed: %s", exc)


def with_retry(max_attempts: int = 3, base_delay: float = 0.5) -> Callable:
    """Exponential-backoff retry. Wraps final failure in DataSourceError."""

    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            source_name = fn.__module__.split('.')[-1].replace('_source', '')
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = fn(*args, **kwargs)
                    log_call(source_name, ok=True)
                    return result
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
            log_call(source_name, ok=False)
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
