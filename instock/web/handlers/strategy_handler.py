"""List + detail pages for strategies (reads Sub-2 HoldingSchedule)."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import tornado.web

from instock.portfolio.storage import read_holding

log = logging.getLogger(__name__)


def _strategies_root() -> Path:
    return Path(os.environ.get("INSTOCK_HOLDING_ROOT", "data/holdings"))


def _list_strategies() -> list[str]:
    root = _strategies_root()
    if not root.exists():
        return []
    return sorted(d.name for d in root.iterdir() if d.is_dir())


class StrategyListHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        self.render("strategies_list.html", strategies=_list_strategies())


class StrategyDetailHandler(tornado.web.RequestHandler):
    def get(self, name: str) -> None:
        if name not in _list_strategies():
            self.set_status(404)
            self.write(f"unknown strategy: {name}")
            return
        end = pd.Timestamp.now()
        start = end - pd.Timedelta(days=30)
        try:
            df = read_holding(name, start, end)
        except Exception as exc:
            log.warning("read_holding failed: %s", exc)
            df = pd.DataFrame(
                columns=["date", "code", "weight", "score", "strategy"]
            )
        latest_date = None
        latest_rows: list[dict] = []
        added: list[str] = []
        removed: list[str] = []
        if not df.empty:
            latest_date = df["date"].max()
            latest = df[df["date"] == latest_date]
            latest_rows = latest[
                ["code", "weight", "score"]
            ].sort_values("weight", ascending=False).to_dict("records")
            anchor = latest_date - pd.Timedelta(days=7)
            prev_candidates = df[df["date"] <= anchor]["date"]
            if not prev_candidates.empty:
                prev_date = prev_candidates.max()
                prev = df[df["date"] == prev_date]
                added = sorted(set(latest["code"]) - set(prev["code"]))
                removed = sorted(set(prev["code"]) - set(latest["code"]))
        run_ids: list[str] = []
        try:
            from instock.backtest.storage import read_metrics
            mdf = read_metrics()
            if not mdf.empty and "strategy" in mdf.columns:
                run_ids = mdf[mdf["strategy"] == name]["run_id"].tolist()
        except Exception:
            run_ids = []
        self.render(
            "strategy_detail.html",
            name=name, latest_date=latest_date,
            latest_rows=latest_rows, added=added, removed=removed,
            run_ids=run_ids,
        )
