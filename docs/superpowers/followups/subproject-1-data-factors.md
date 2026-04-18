# Sub-project 1 (Data & Factor Engineering) — Follow-up Items

Source: Final code review of `feature/subproject-1-data-factors` (tag `subproject-1-data-factors-mvp`, commits `9b4da11..00f34f1`).

Verdict was **Approved with minor issues** — all items below are non-blocking and deferred to follow-up work or sub-project 2.

---

## A. Docstring / naming cleanups (cheap, any time)

- [x] `instock/factors/flow/north.py` — `NorthHoldingChgFactor.description` currently says "Pct change of northbound hold_shares over ~{window} calendar days" but implementation uses `shift(1)` (last observed snapshot). Update description to reflect "pct change vs. previous observed snapshot" to avoid misleading N-day semantics.
- [x] `instock/factors/evaluator.py::evaluate_from_frames` — docstring must state the date-alignment contract: caller is responsible for indexing `returns_df` by the factor-available date (typically `ret[T]` = realized return over `T → T+1`). Evaluator does not shift.
- [ ] `instock/datasource/io.py::RateLimiter` — docstring note "not thread-safe; one instance per thread" (internal `self._last` read/write is unlocked).
- [ ] Dead imports:
  - `instock/factors/evaluator.py:3` — `field` from dataclasses unused.
  - `instock/datasource/akshare_source.py` — `DataSourceError` imported but only `SchemaValidationError` referenced at module level (it's re-raised inside `io.py`).
  - `tests/factors/test_daily_job.py:2` — `MagicMock` unused.

## B. "未纳入 MVP" additions to plan (documentation)

The original plan listed deliberate exclusions; these were found during review and should be appended:

- [ ] **Evaluator decay curve is a stub.** `evaluator.py:56` returns `ic_mean` for lag=1 and `NaN` for all other lags. Real decay (compute IC at lag-N shifted returns) deferred. Test only asserts attribute existence.
- [ ] **Daily job uses today's index membership for historical backfill.** ~~`factor_compute_daily_job.py:_resolve_universe` calls `get_index_member("000300", date.today())` regardless of the compute window → survivorship / membership look-ahead when backfilling.~~ **RESOLVED**: `_resolve_universe(at)` now accepts an explicit as-of date; `run(start, end)` passes `end` as the anchor so backfill uses end-of-window membership. Still a simplification (true point-in-time would use membership per each date in the window), but removes the today-anchored look-ahead.
- [ ] **PIT valuation is O(D×N) Python loop.** `fundamental/valuation.py` iterates each day × each latest-row. For CSI-300 × multi-year this will be slow. Replace with single `pd.merge_asof(by="code", on="date", left=dates_df, right=pit_df.sort_values("announcement_date"))` style join.

## C. Missing tests (ship with sub-project 2 or as a follow-up PR)

- [ ] `instock/datasource/tushare_source.py` — one test asserting all 7 methods raise `NotImplementedError("TushareSource not enabled")`. Guards against accidental implementation drift.
- [ ] `AkShareSource` untested methods — `get_north_bound`, `get_money_flow`, `get_index_member` have no unit tests (only `get_ohlcv`, `get_fundamentals_pit`, `get_lhb`, `get_trade_calendar` have fixtures). Add fixture-driven smoke tests with mocked akshare calls.
- [x] **Retry ↔ SchemaValidationError regression test.** The design deliberately splits `_fetch_*` (retry-decorated) from public method (validate-only). Add a test that patches `_fetch_ohlcv` to return a bad-schema frame and asserts:
  1. `SchemaValidationError` is raised (not `DataSourceError`)
  2. `_fetch_ohlcv` was called exactly once (not retried)
- [ ] `factor_compute_daily_job.run()` error path — register two factors, make one raise, assert the other still writes and `errors` dict contains only the failing one.
- [ ] PIT edge case — `PEFactor.compute` with `start` before any `announcement_date`, expect empty DataFrame with correct columns.
- [ ] Storage roundtrip — existing tests cover partition split; add a roundtrip that asserts `read_factor` output is sorted ascending by (date, code).

## D. Non-blocking robustness improvements

- [ ] `storage.write_factor` is non-atomic: a crash mid-`to_parquet` can corrupt the yearly file. Switch to tmp-file + `os.replace` pattern.
- [ ] `lhb/heat.py:44` emits 0.0 rows for every universe name on every date (full panel). Fine for IC analysis; if storage cost becomes an issue, switch to sparse (only hits). Document the choice either way.
- [ ] pandas `DataFrameGroupBy.apply` FutureWarnings in `preprocess.py` and `evaluator.py` (3 sites). Silence by passing `include_groups=False` or selecting columns pre-apply. Harmless today, will break when pandas drops the behavior.

## E. Explicitly deferred from original plan (unchanged from plan's own list)

These are already acknowledged in the plan's "未纳入本 MVP" section — listed here for completeness:

- Full factor list: `rsi_14`, `macd_hist`, `atr_20`, `turnover_20d`, `amihud_illiq`, `ps`, `gross_margin`, `net_margin`, `net_profit_yoy`, `lhb_seat_winrate`, `lhb_top_seat_hit`, `main_net_inflow_5d`, `big_order_ratio`. Each is a small extension of the existing base classes.
- Old job migration to new tables + reconciliation scripts (operational, per plan "run-for-a-week-first" rule).
- Real industry map data source for `preprocess.neutralize` (interface ready, caller passes `industry_map=None` for now).

---

## Priority suggestion for sub-project 2 kickoff

Must-do before doing serious historical research:
- B-2 (job universe look-ahead)
- C-3 (SchemaValidationError regression test — enforce the design invariant)
- A-2 (evaluator date-alignment contract — users will get this wrong otherwise)

Nice-to-have: everything else in A, plus C-1/2/4 as a small hygiene PR.
