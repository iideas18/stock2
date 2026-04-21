from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import tornado.testing
import tornado.web

from instock.web.handlers.strategy_handler import (
    StrategyDetailHandler,
    StrategyListHandler,
)


class _App(tornado.web.Application):
    def __init__(self, template_path):
        super().__init__(
            [
                (r"/strategies", StrategyListHandler),
                (r"/strategies/([^/]+)", StrategyDetailHandler),
            ],
            template_path=str(template_path),
        )


def _seed_holding(root: Path, strategy: str, dates, codes_by_date):
    for d, codes in zip(dates, codes_by_date):
        year = d.year
        path = root / strategy / f"{year}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([
            {"date": d, "code": c, "weight": 1.0 / len(codes),
             "score": 0.0, "strategy": strategy}
            for c in codes
        ])
        if path.exists():
            old = pd.read_parquet(path)
            df = pd.concat([old, df], ignore_index=True)
        df.to_parquet(path, index=False)


class TestStrategy(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = Path(tempfile.mkdtemp())
        os.environ["INSTOCK_HOLDING_ROOT"] = str(self.tmp)
        # Recent dates so read_holding over last 30d finds them
        today = pd.Timestamp.now().normalize()
        _seed_holding(
            self.tmp, "demo",
            [today - pd.Timedelta(days=7), today],
            [["000001", "000002"], ["000001", "000003"]],
        )

    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp)

    def test_list_200(self):
        resp = self.fetch("/strategies")
        assert resp.code == 200
        assert b"demo" in resp.body

    def test_detail_has_diff(self):
        resp = self.fetch("/strategies/demo")
        assert resp.code == 200
        assert b"000003" in resp.body
        assert b"000002" in resp.body

    def test_unknown_404(self):
        resp = self.fetch("/strategies/missing")
        assert resp.code == 404
