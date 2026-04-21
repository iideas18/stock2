"""Run-all-checks entry point with per-check exception guard."""
from __future__ import annotations

import logging

import pandas as pd

from .status import StatusCheck, StatusRow

log = logging.getLogger(__name__)

_REGISTRY: list[StatusCheck] = []


def register_check(check: StatusCheck) -> None:
    _REGISTRY.append(check)


def clear_registry() -> None:
    _REGISTRY.clear()


def run_all_checks() -> list[StatusRow]:
    rows: list[StatusRow] = []
    for chk in _REGISTRY:
        try:
            rows.append(chk.run())
        except Exception as exc:
            rows.append(StatusRow(
                name=getattr(chk, "name", chk.__class__.__name__),
                status="RED",
                message=f"check failed: {exc}",
                metric_value=None,
                as_of=pd.Timestamp.now(),
            ))
    return rows
