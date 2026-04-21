from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import tornado.testing
import tornado.web

from instock.web.handlers.backtest_handler import (
    BacktestDetailHandler,
    BacktestListHandler,
)


def _seed(root: Path, run_id: str):
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "report.html").write_text("<html>demo report</html>")
    metrics = root / "_metrics.parquet"
    row = pd.DataFrame([{
        "run_id": run_id, "strategy": "demo",
        "start_date": pd.Timestamp("2026-01-01"),
        "end_date": pd.Timestamp("2026-03-31"),
        "sharpe": 1.5, "max_dd": -0.1,
    }])
    if metrics.exists():
        old = pd.read_parquet(metrics)
        row = pd.concat([old, row], ignore_index=True)
    row.to_parquet(metrics, index=False)


class _App(tornado.web.Application):
    def __init__(self, template_path, static_path):
        super().__init__(
            [
                (r"/backtests", BacktestListHandler),
                (r"/backtests/([^/]+)", BacktestDetailHandler),
            ],
            template_path=str(template_path),
            static_path=str(static_path),
        )


class TestBacktest(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        os.environ["INSTOCK_BACKTEST_ROOT"] = str(self.tmp)
        _seed(self.tmp, "run-abc")
        super().setUp()

    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp, self.tmp)

    def test_list_has_run_id(self):
        resp = self.fetch("/backtests")
        assert resp.code == 200
        assert b"run-abc" in resp.body

    def test_detail_has_iframe(self):
        resp = self.fetch("/backtests/run-abc")
        assert resp.code == 200
        assert b"<iframe" in resp.body

    def test_unknown_404(self):
        resp = self.fetch("/backtests/missing")
        assert resp.code == 404
