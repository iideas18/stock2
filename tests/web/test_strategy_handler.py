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


class TestStrategyDiffAnchor(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = Path(tempfile.mkdtemp())
        os.environ["INSTOCK_HOLDING_ROOT"] = str(self.tmp)
        today = pd.Timestamp.now().normalize()
        _seed_holding(
            self.tmp, "demo",
            [
                today - pd.Timedelta(days=7),
                today - pd.Timedelta(days=3),
                today,
            ],
            [
                ["000001", "000002"],
                ["000001", "000002", "000003"],
                ["000001", "000003"],
            ],
        )

    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp)

    def test_detail_diff_uses_7d_anchor(self):
        resp = self.fetch("/strategies/demo")
        assert resp.code == 200
        body = resp.body.decode()
        # diff vs today-7 ([000001,000002]):
        #   added=[000003], removed=[000002]
        # If diff were vs today-3 ([000001,000002,000003]):
        #   added=[], removed=[000002] — 000003 would NOT appear as added.
        # Ensure 000003 is shown in the "added" section.
        import re
        # crude section split — added appears before removed in template
        added_idx = body.find("Added") if "Added" in body else body.find("added")
        removed_idx = (
            body.find("Removed") if "Removed" in body else body.find("removed")
        )
        assert added_idx != -1 and removed_idx != -1
        added_section = body[added_idx:removed_idx]
        removed_section = body[removed_idx:]
        assert "000003" in added_section, (
            "000003 should be in added section (diff vs 7d anchor)"
        )
        assert "000002" in removed_section
