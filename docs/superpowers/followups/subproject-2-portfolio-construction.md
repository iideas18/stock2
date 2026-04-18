# Sub-project 2 (Portfolio Construction) — Follow-up Items

Source: design spec
`docs/superpowers/specs/2026-04-18-portfolio-construction-design.md`,
implementation plan
`docs/superpowers/plans/2026-04-18-portfolio-construction.md`,
and per-task review findings collected during execution.

---

## A. Cross-cutting (touches Sub-1) — IMPORTANT

- [ ] **Factor registry bootstrap is broken in Sub-1**.
      `instock/job/factor_compute_daily_job._default_configs` (and any other
      caller) relies on side-effect imports of factor modules to populate the
      registry. None of the factor modules call `register()` at module scope,
      so `get_all()` returns `{}` and Sub-1's daily job is a silent no-op.
      Sub-2's `generate_holdings_daily_job._default_configs` worked around it
      by inlining explicit instantiation+registration. Fix: introduce a shared
      `instock.factors.bootstrap.register_default_factors()` helper that
      instantiates and `register()`s all six factor classes; call it from both
      Sub-1 and Sub-2 daily jobs. Then drop the inline workaround.

## B. Entry gates for Sub-3 (must-fix before backtester work)

- [ ] **Real industry-map data source.** `IndustryExposureConstraint` is a
      no-op when `industry_map=None`. Pick a source (akshare
      `stock_board_industry_name_em` + membership API), load into a
      `dict[str, str]` or DataFrame, and inject via `ConstraintContext`.
- [ ] **Real stock-listing-date source.** `NewListingFilter` approximates
      listing date by the earliest OHLCV observation, which is wrong for any
      code whose history doesn't reach back into the panel. Replace with an
      authoritative source once a fundamentals/ref-data pipeline exists.
- [ ] **ST / 涨跌停 (limit-up/down) filters.** Requires per-date ST flag
      and prior-close vs limit ratio. Add as `STFilter` and `LimitFilter`
      classes plugged into the `FilterChain`.
- [ ] **`StrategyPipeline._load_ohlcv_panel("ALL", ...)` placeholder.**
      Currently calls `source.get_ohlcv("ALL", ...)`, which the akshare source
      does not actually support (per-code only). Sub-3's backtester batch
      data-panel preloader is the natural place to fix this.

## C. MaxWeightConstraint robustness — IMPORTANT

(Surfaced by Task 7 review; deferred because the case is impossible under
`EqualWeighter` thanks to the `n*max_weight >= 1` feasibility check, but
becomes real with future weighted variants.)

- [ ] Guard against `unsat` having zero total weight (would yield `0/0` →
      NaN propagation through the redistribution).
- [ ] Detect "overflow remaining but `unsat` exhausted" and raise rather than
      silently dropping weight.
- [ ] Unify epsilon constants (currently `1e-9` for feasibility, `1e-12`
      inside the loop).

## D. IndustryExposureConstraint robustness

- [ ] Log a warning when `non_capped` is empty and `freed > 0` (currently
      silent, drops weight).
- [ ] Single-pass scaling can push a previously OK industry over cap;
      document this MVP limitation (or iterate to fixed point like
      `MaxWeightConstraint`).
- [ ] `_unknown` bucket for codes missing from `industry_map` could silently
      exceed cap — consider raising or warning.
- [ ] Replace `w.index[~industries.isin(capped_ind)]` with
      `industries[~industries.isin(capped_ind)].index` to be order-safe.

## E. Storage hardening

- [ ] `read_holding` does not sort multi-year concat result; sort by
      `[date, code]` for deterministic downstream consumption.
- [ ] No check that `df["strategy"]` matches the `strategy` arg passed to
      `write_holding` (mirrors Sub-1 `factors/storage.py`'s laxness, which is
      itself worth a follow-up).
- [ ] Atomic Parquet writes (tmp file + `os.replace`) to avoid corruption on
      crash mid-write — mirrors Sub-1 follow-up.

## F. Selector / Schedule small wins

- [ ] Document tie-break determinism in `TopQuantileSelector` /
      `TopNSelector` (relies on pandas stable sort + `universe` order).
- [ ] Add `ValueError` regression tests for `quantile`/`n` validation.
- [ ] Add explicit test for `WeeklyRebalance` "target > end with fallback in
      window" case (currently only implicit).
- [ ] Add test for Monday weekday (`weekday=0`, `range(1)` boundary).
- [ ] Document in `RebalanceSchedule` docstring that `trade_calendar`
      ordering is irrelevant (only set membership matters).

## G. Combiner small wins

- [ ] Add a multi-date cross-section test for `EqualRankCombiner` (current
      tests all use a single date).

## H. Pipeline performance (premature for MVP, log only)

- [ ] `scores_panel[scores_panel["date"] == ts]` is O(N·R) over the rebalance
      loop. A `groupby("date")`-once dict would make it O(N+R). Irrelevant at
      MVP universe sizes (CSI-300, weekly rebalance), but worth replacing
      when universes grow.

## I. Nice-to-haves (any time)

- [ ] `MonthlyRebalance` schedule implementation.
- [ ] `MarketCapWeighter` (needs market-cap data in `WeighterContext`;
      akshare `stock_zh_a_spot_em` has it).
- [ ] `InverseVolWeighter` (computable from OHLCV already in
      `WeighterContext.price_panel`).
- [ ] `ICWeightedCombiner` reading historical IC from Sub-1 evaluator output
      (must be rolling to avoid look-ahead).
- [ ] pandas `DataFrameGroupBy.apply` `FutureWarning` cleanup across the
      package (mirrors Sub-1 follow-up).

## J. Deferred per MVP scope (will need a design revisit)

- cvxpy-based optimizer (mean-variance, tracking-error constraints).
- Short side / market-neutral portfolios.
- MySQL mirror of `HoldingSchedule` for Sub-4 Web queries (decide in Sub-4
  whether the Parquet-only read is fast enough).
