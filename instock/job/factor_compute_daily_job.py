"""Daily factor-compute job.

Usage:
    python -m instock.job.factor_compute_daily_job [YYYY-MM-DD [YYYY-MM-DD]]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

from instock.factors import storage
from instock.factors.registry import get_all

log = logging.getLogger(__name__)


def _resolve_universe() -> list[str]:
    """Default universe: CSI 300. Extended later."""
    from instock.datasource.registry import get_source
    return get_source().get_index_member("000300", date.today())


def run(start: date, end: date) -> dict[str, Exception]:
    """Run every registered factor for [start, end]; return per-factor errors."""
    errors: dict[str, Exception] = {}
    universe = _resolve_universe()
    for name, factor in get_all().items():
        try:
            raw = factor.compute(universe, start, end)
            if raw.empty:
                log.warning("factor %s produced no rows", name)
                continue
            processed = factor.preprocess(raw)
            storage.write_factor(name, processed)
            log.info("factor %s: %s rows written", name, len(processed))
        except Exception as exc:
            log.exception("factor %s failed", name)
            errors[name] = exc
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Side-effect import: register all factor classes.
    import instock.factors.technical.momentum  # noqa: F401
    import instock.factors.fundamental.valuation  # noqa: F401
    import instock.factors.lhb.heat  # noqa: F401
    import instock.factors.flow.north  # noqa: F401

    errs = run(*_parse(sys.argv[1:]))
    if errs:
        sys.exit(1)
