"""Fire-and-forget webhook poster. Never raises."""
from __future__ import annotations

import logging

import requests

log = logging.getLogger(__name__)


def post_webhook(url: str, payload: dict, timeout: float = 5.0) -> bool:
    if not url:
        return False
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return True
    except Exception as exc:
        log.warning("webhook post failed: %s", exc)
        return False
