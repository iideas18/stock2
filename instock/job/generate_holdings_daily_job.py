"""Daily holding-generation job.

Usage:
    python -m instock.job.generate_holdings_daily_job [YYYY-MM-DD [YYYY-MM-DD]]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

from instock.portfolio import storage
from instock.portfolio.pipeline import StrategyConfig, StrategyPipeline

log = logging.getLogger(__name__)


def run(
    start: date, end: date, configs: list[StrategyConfig]
) -> dict[str, Exception]:
    errors: dict[str, Exception] = {}
    for cfg in configs:
        try:
            df = StrategyPipeline().run(start, end, cfg)
            if df.empty:
                log.warning("strategy %s produced no rows", cfg.name)
                continue
            storage.write_holding(cfg.name, df)
            log.info("strategy %s: %s rows written", cfg.name, len(df))
        except Exception as exc:
            log.exception("strategy %s failed", cfg.name)
            errors[cfg.name] = exc
    return errors


def _parse(argv: list[str]) -> tuple[date, date]:
    if len(argv) == 0:
        today = date.today()
        return today, today
    if len(argv) == 1:
        d = datetime.strptime(argv[0], "%Y-%m-%d").date()
        return d, d
    s = datetime.strptime(argv[0], "%Y-%m-%d").date()
    e = datetime.strptime(argv[1], "%Y-%m-%d").date()
    return s, e


def _default_configs() -> list[StrategyConfig]:
    """MVP: a single default strategy using every factor currently registered."""
    from instock.factors.bootstrap import register_default_factors
    from instock.factors.registry import get_all

    register_default_factors()
    names = list(get_all().keys())
    return [StrategyConfig(name="default", factors=names)]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start, end = _parse(sys.argv[1:])
    errs = run(start, end, _default_configs())
    if errs:
        sys.exit(1)
