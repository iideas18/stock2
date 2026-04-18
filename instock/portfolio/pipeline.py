"""StrategyPipeline: orchestrates factor-combiner -> filter -> selector ->
weighter -> constraint -> holding frame.

Dependencies are pluggable; defaults chosen to match the spec's recommended
baseline (equal-rank combiner, top-10% selection, equal weights, 3% single-
name cap, weekly-Friday rebalance).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable

import pandas as pd

from instock.datasource.registry import get_source
from instock.factors.storage import read_factor
from instock.refdata import industry as refdata_industry
from instock.refdata import listing as refdata_listing
from instock.refdata import st as refdata_st
from instock.refdata.schemas import RefdataNotAvailable

from .combiner import EqualRankCombiner, FactorCombiner
from .constraints import (
    ConstraintContext,
    MaxWeightConstraint,
    PortfolioConstraint,
)
from .filters import (
    FilterChain,
    FilterContext,
    NewListingFilter,
    SuspendedFilter,
    UniverseFilter,
)
from .schedule import RebalanceSchedule, WeeklyRebalance
from .selector import Selector, TopQuantileSelector
from .weighter import EqualWeighter, Weighter, WeighterContext

log = logging.getLogger(__name__)

_OUTPUT_COLUMNS = ["date", "code", "weight", "score", "strategy"]


@dataclass
class StrategyConfig:
    name: str
    factors: list[str]
    combiner: FactorCombiner = field(default_factory=EqualRankCombiner)
    filters: list[UniverseFilter] = field(default_factory=lambda: [
        SuspendedFilter(),
        NewListingFilter(min_days=60),
    ])
    selector: Selector = field(
        default_factory=lambda: TopQuantileSelector(0.1)
    )
    weighter: Weighter = field(default_factory=EqualWeighter)
    constraints: list[PortfolioConstraint] = field(
        default_factory=lambda: [MaxWeightConstraint(0.03)]
    )
    schedule: RebalanceSchedule = field(
        default_factory=lambda: WeeklyRebalance("FRI")
    )
    universe_resolver: Callable[[date], list[str]] | None = None


class StrategyPipeline:
    """Executes one StrategyConfig over [start, end]."""

    def _build_filter_context(
        self, ohlcv: pd.DataFrame, at: date
    ) -> FilterContext:
        try:
            listing_dates = refdata_listing.read_listing_dates()
        except RefdataNotAvailable:
            listing_dates = None
        try:
            st_flags = refdata_st.read_st_flags(at)
        except RefdataNotAvailable:
            st_flags = None
        return FilterContext(
            ohlcv_panel=ohlcv,
            listing_dates=listing_dates,
            st_flags=st_flags,
        )

    def _build_constraint_context(self, at: date) -> ConstraintContext:
        try:
            industry_map = refdata_industry.read_industry_map(at)
        except RefdataNotAvailable:
            industry_map = None
        return ConstraintContext(industry_map=industry_map)

    def run(
        self, start: date, end: date, config: StrategyConfig
    ) -> pd.DataFrame:
        source = get_source()

        cal_dates = source.get_trade_calendar(start, end)

        rebal = config.schedule.rebalance_dates(start, end, cal_dates)
        if not rebal:
            log.warning("strategy %s: no rebalance dates in window", config.name)
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)

        panel = self._load_factor_panel(config.factors, start, end)
        if panel.empty:
            log.warning("strategy %s: empty factor panel", config.name)
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)
        scores_panel = config.combiner.combine(panel)

        resolver = config.universe_resolver or _default_universe_resolver()

        ohlcv = self._load_ohlcv_panel(start, end)
        fctx = self._build_filter_context(ohlcv, start)

        rows = []
        for d in rebal:
            ts = pd.Timestamp(d)
            universe = resolver(d)
            chain = FilterChain(config.filters)
            filtered = chain.apply(universe, ts, fctx)
            if not filtered:
                log.warning("strategy %s %s: empty after filters",
                            config.name, d)
                continue

            scores_t = (
                scores_panel[scores_panel["date"] == ts]
                .set_index("code")["score"]
            )
            selected = config.selector.select(scores_t, filtered)
            if not selected:
                log.warning("strategy %s %s: empty selection",
                            config.name, d)
                continue

            wctx = WeighterContext(
                price_panel=ohlcv, mcap_panel=None, scores=scores_t
            )
            weights = config.weighter.weigh(selected, wctx)

            cctx = self._build_constraint_context(d)
            for c in config.constraints:
                weights = c.apply(weights, cctx)

            for code, w in weights.items():
                rows.append({
                    "date": ts,
                    "code": code,
                    "weight": float(w),
                    "score": (
                        float(scores_t[code])
                        if code in scores_t.index else None
                    ),
                    "strategy": config.name,
                })

        if not rows:
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)
        return pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)

    def _load_factor_panel(
        self, factor_names: list[str], start: date, end: date
    ) -> pd.DataFrame:
        """Read each factor and outer-join into a wide panel."""
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        frames = []
        for name in factor_names:
            df = read_factor(name, start_ts, end_ts)
            if df.empty:
                continue
            df = df.rename(columns={"value": name})[["date", "code", name]]
            frames.append(df)
        if not frames:
            return pd.DataFrame(columns=["date", "code"])
        panel = frames[0]
        for f in frames[1:]:
            panel = panel.merge(f, on=["date", "code"], how="outer")
        return panel

    def _load_ohlcv_panel(
        self, start: date, end: date
    ) -> pd.DataFrame:
        """Fetch an OHLCV panel covering [start - 120d, end] for filter use.

        120-day lookback is a conservative buffer for NewListingFilter(60).
        Note: passes "ALL" as code; the akshare source today fetches per-code.
        Sub-3 backtester will replace this with a proper batch loader.
        Unit tests monkeypatch `get_source().get_ohlcv` to bypass this.
        """
        source = get_source()
        lookback = start - timedelta(days=120)
        try:
            return source.get_ohlcv("ALL", lookback, end)  # type: ignore[arg-type]
        except Exception as exc:
            log.warning("failed to load OHLCV panel: %s", exc)
            return pd.DataFrame(columns=["date", "code", "volume"])


def _default_universe_resolver() -> Callable[[date], list[str]]:
    """Fallback: sub-1's CSI-300-as-of-end resolver, but closured per-date."""
    from instock.job.factor_compute_daily_job import _resolve_universe

    def _f(at: date) -> list[str]:
        return _resolve_universe(at)

    return _f
