from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import tornado.testing
import tornado.web

from instock.monitoring import runner as rn
from instock.monitoring.status import StatusCheck, StatusRow
from instock.web.handlers.monitoring_handler import (
    MonitoringAckHandler,
    MonitoringHandler,
)


class _RedCheck(StatusCheck):
    name = "c1"
    def run(self) -> StatusRow:
        return StatusRow(
            name=self.name, status="RED", message="bad",
            metric_value=None, as_of=pd.Timestamp.now(),
        )


class _App(tornado.web.Application):
    def __init__(self, template_path):
        super().__init__(
            [
                (r"/monitoring", MonitoringHandler),
                (r"/monitoring/ack", MonitoringAckHandler),
            ],
            template_path=str(template_path),
        )


class TestMonitoring(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = Path(tempfile.mkdtemp())
        os.environ["INSTOCK_MONITORING_ROOT"] = str(self.tmp)
        rn.clear_registry()
        rn.register_check(_RedCheck())

    def tearDown(self):
        super().tearDown()
        rn.clear_registry()

    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp)

    def test_red_renders(self):
        resp = self.fetch("/monitoring")
        assert resp.code == 200
        assert b"c1" in resp.body
        assert b"RED" in resp.body

    def test_ack_round_trip(self):
        # ACK
        resp = self.fetch(
            "/monitoring/ack", method="POST", body="check_name=c1",
            follow_redirects=False,
        )
        assert resp.code == 302  # redirect
        # page now shows (ack'd)
        resp = self.fetch("/monitoring")
        assert b"ack" in resp.body.lower()
