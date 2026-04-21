# Sub-project 3 (Backtest Engine) — Follow-up Items

Source: `docs/superpowers/specs/2026-04-18-backtest-engine-design.md`,
`docs/superpowers/plans/2026-04-18-backtest-engine.md`, per-task review.

## A. Known MVP trade-offs

- [ ] **lot_drag_bps = 0.0 placeholder**: Executor does not yet accumulate
      cash residual from lot-size rounding. Metrics reports 0 until wired in.
- [ ] **walkforward runner only generates windows**: IS/OOS NAV splicing +
      drift diagnostics deferred to Sub-3.5. `window_bounds` is the only
      MVP surface.
- [ ] **st_flags / industry as-of = snapshot**: Look-ahead caveat carried
      over from Sub-2.5. Report watermark noted.
- [ ] **fingerprint covers config only when holding loaded in-memory**:
      CLI path does not hash the HoldingSchedule Parquet source files.
      Add `read_holding` + SHA plumbing when we productionize.

## B. Small wins (any time)

- [ ] `SlippageModel.VolumePctSlippage` as an alternative to bps.
- [ ] `turnover_daily` in NAV is a rough proxy (cash_delta / total_value);
      replace with Σ|Δw| directly.
- [ ] Benchmarks: derive index by index_code mapping (000300.SH vs hs300).
- [ ] Report: monthly return heatmap, per-code P&L table.
- [ ] Metrics: rolling-window sharpe, rolling beta vs bench.

## C. Deferred / out-of-scope

- Parameter auto-tuning inside walk-forward (nested CV, bayesian search).
- Level-2 order book simulation; option / futures support.
- T+0 strategies; multi-market.
- Live replay / paper trading hookup.

## D. Sub-4 entry hand-off

Sub-4 Web should:
1. Read `<INSTOCK_BACKTEST_ROOT>/_metrics.parquet` for the strategy grid.
2. Embed `<run_id>/report.html` in an iframe.
3. Surface fingerprint prefix as an audit key.
4. Wire `backtest_run_job` into the daily scheduler (post-holding-gen).
