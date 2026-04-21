"""Cron entry: run all checks, record, fire webhook on RED transitions."""
from __future__ import annotations

import logging
import os
import sys

from instock.monitoring import get_monitoring_root
from instock.monitoring.runner import run_all_checks
from instock.monitoring.state import AlertStateStore
from instock.monitoring.webhook import post_webhook

log = logging.getLogger(__name__)


def run_once() -> int:
    url = os.environ.get("INSTOCK_WEBHOOK_URL", "")
    store = AlertStateStore(get_monitoring_root())
    fired = 0
    for row in run_all_checks():
        prev = store.last_status(row.name)
        store.record(row)
        if row.status == "RED" and (prev is None or prev.status != "RED"):
            if url:
                if post_webhook(url, row.to_dict()):
                    fired += 1
    return fired


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        run_once()
        return 0
    except Exception as exc:
        log.error("monitoring_check_job failed: %s", exc)
        return 0  # cron must exit 0


if __name__ == "__main__":
    sys.exit(main())
