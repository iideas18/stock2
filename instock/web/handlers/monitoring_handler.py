"""Monitoring dashboard: current rows + ACK button."""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
import tornado.web

from instock.monitoring import get_monitoring_root
from instock.monitoring.registry_bootstrap import register_default_checks
from instock.monitoring.runner import run_all_checks
from instock.monitoring.state import AlertStateStore
from instock.monitoring.status import StatusRow

log = logging.getLogger(__name__)


def _ack_merged(row: StatusRow, store: AlertStateStore) -> dict:
    prev = store.last_status(row.name)
    display_status = row.status
    ack_flag = False
    if prev is not None and prev.status == "ACK" and row.status == "RED":
        display_status = "RED (ack'd)"
        ack_flag = True
    return {
        "name": row.name,
        "status": display_status,
        "raw_status": row.status,
        "message": row.message,
        "metric_value": row.metric_value,
        "as_of": row.as_of,
        "ack": ack_flag,
    }


class MonitoringHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        register_default_checks()
        store = AlertStateStore(get_monitoring_root())
        rows = [_ack_merged(r, store) for r in run_all_checks()]
        self.render("monitoring.html", rows=rows)


class MonitoringAckHandler(tornado.web.RequestHandler):
    def post(self) -> None:
        name = self.get_body_argument("check_name", "")
        if not name:
            self.set_status(400)
            self.write("check_name required")
            return
        store = AlertStateStore(get_monitoring_root())
        store.record(StatusRow(
            name=name, status="ACK", message="acknowledged via UI",
            metric_value=None, as_of=pd.Timestamp.now(),
        ))
        self.redirect("/monitoring")
