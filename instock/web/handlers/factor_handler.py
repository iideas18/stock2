"""List + detail pages for registered factors."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import tornado.web

from instock.factors import bootstrap as fbootstrap
from instock.factors.registry import get_all as get_all_factors
from instock.factors.storage import read_factor

log = logging.getLogger(__name__)


def _ensure_registry() -> None:
    fbootstrap.register_default_factors()


class FactorListHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        _ensure_registry()
        items = []
        for name, f in sorted(get_all_factors().items()):
            items.append({
                "name": name,
                "description": getattr(f, "description", "") or "",
            })
        self.render("factors_list.html", factors=items)


class FactorDetailHandler(tornado.web.RequestHandler):
    def get(self, name: str) -> None:
        _ensure_registry()
        factors = get_all_factors()
        if name not in factors:
            self.set_status(404)
            self.write(f"unknown factor: {name}")
            return
        end = pd.Timestamp.now()
        start = end - pd.Timedelta(days=30)
        try:
            df = read_factor(name, start, end)
            latest = None if df.empty else pd.Timestamp(df["date"].max())
            n_rows = int(len(df))
        except Exception as exc:
            log.warning("factor read failed for %s: %s", name, exc)
            latest = None
            n_rows = 0
        report_url = None
        candidate = Path("data/factor_reports") / f"{name}.html"
        if candidate.exists():
            report_url = f"/static/factor_reports/{name}.html"
        self.render(
            "factor_detail.html",
            name=name,
            description=getattr(factors[name], "description", "") or "",
            latest=latest,
            n_rows=n_rows,
            report_url=report_url,
        )
