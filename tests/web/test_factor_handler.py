from __future__ import annotations

from pathlib import Path

import tornado.testing
import tornado.web

from instock.web.handlers.factor_handler import (
    FactorDetailHandler,
    FactorListHandler,
)


class _App(tornado.web.Application):
    def __init__(self, template_path):
        super().__init__(
            [
                (r"/factors", FactorListHandler),
                (r"/factors/([^/]+)", FactorDetailHandler),
            ],
            template_path=str(template_path),
        )


class _Base(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp)


class TestFactorList(_Base):
    def test_list_200(self):
        resp = self.fetch("/factors")
        assert resp.code == 200
        assert b"factor" in resp.body.lower()


class TestFactorDetail(_Base):
    def test_unknown_404(self):
        resp = self.fetch("/factors/does_not_exist")
        assert resp.code == 404

    def test_known_name_appears(self):
        from instock.factors import bootstrap
        bootstrap.register_default_factors()
        resp = self.fetch("/factors/mom_20d")
        assert resp.code == 200
        assert b"mom_20d" in resp.body
