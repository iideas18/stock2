"""Backtest runs list + per-run detail page (iframe report)."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import tornado.web

log = logging.getLogger(__name__)


def _bt_root() -> Path:
    return Path(os.environ.get("INSTOCK_BACKTEST_ROOT", "data/backtests"))


class BacktestListHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        import pandas as pd
        path = _bt_root() / "_metrics.parquet"
        rows: list[dict] = []
        if path.exists():
            try:
                rows = pd.read_parquet(path).sort_values(
                    "end_date", ascending=False
                ).to_dict("records")
            except Exception as exc:  # noqa: BLE001
                log.warning("metrics read failed: %s", exc)
        self.render("backtests_list.html", rows=rows)


class BacktestDetailHandler(tornado.web.RequestHandler):
    def get(self, run_id: str) -> None:
        run_dir = _bt_root() / "runs" / run_id
        report = run_dir / "report.html"
        if not report.exists():
            self.set_status(404)
            self.write(f"unknown run_id: {run_id}")
            return
        report_url = f"/static/runs/{run_id}/report.html"
        self.render(
            "backtest_detail.html", run_id=run_id, report_url=report_url
        )
