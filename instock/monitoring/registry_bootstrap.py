"""Idempotent default check registration."""
from __future__ import annotations

import logging

from .runner import clear_registry, register_check
from .status import ArtifactFreshness, DataSourceRate, ICIRDecay

log = logging.getLogger(__name__)

_REGISTERED = False


def register_default_checks(*, force: bool = False) -> None:
    global _REGISTERED
    if _REGISTERED and not force:
        return
    clear_registry()

    # Factor freshness + IC_IR
    from instock.factors import bootstrap as fb
    from instock.factors.registry import get_all as get_all_factors
    from instock.factors.storage import _factor_dir

    fb.register_default_factors()
    for name in get_all_factors():
        def _loader(n=name):
            import pandas as pd
            d = _factor_dir(n)
            if not d.exists():
                return None
            years = sorted(d.glob("*.parquet"))
            if not years:
                return None
            df = pd.read_parquet(years[-1])
            return None if df.empty else pd.Timestamp(df["date"].max())
        register_check(ArtifactFreshness(
            name=f"factor.{name}", loader=_loader,
            max_age_days_yellow=2, max_age_days_red=7,
        ))
        register_check(ICIRDecay(factor=name))

    # Metrics freshness
    from instock.backtest.storage import _root as _bt_root

    def _metrics_loader():
        import pandas as pd
        p = _bt_root() / "_metrics.parquet"
        if not p.exists():
            return None
        df = pd.read_parquet(p)
        if df.empty:
            return None
        col = "end_date" if "end_date" in df.columns else df.columns[-1]
        return pd.Timestamp(df[col].max())

    register_check(ArtifactFreshness(
        name="backtest.metrics", loader=_metrics_loader,
        max_age_days_yellow=2, max_age_days_red=14,
    ))

    # Data source rate
    register_check(DataSourceRate(source="akshare"))
    _REGISTERED = True
