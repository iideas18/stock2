# Sub-project 2.5 (Data Alignment) — Follow-up Items

Source: `docs/superpowers/specs/2026-04-18-data-alignment-design.md`,
`docs/superpowers/plans/2026-04-18-data-alignment.md`, per-task review.

## A. Known MVP trade-offs (documented risks)

- [ ] **Current-snapshot ST look-ahead**: 2024 年 ST 股票在 2020 年回测被错误标。
      Sub-3 启动时在 HTML 报告里加 "st=current snapshot" 水印。
- [ ] **Current-snapshot industry look-ahead**: 同上语义。水印同。
- [ ] **OhlcvPanelStore 首次预热慢**: akshare 0.2s/req × N codes × K 年。
      `OhlcvPanelStore.warm_cache(...)` 已提供；Sub-3 启动前手动跑一遍。

## B. Small wins (any time)

- [ ] `refdata.industry` / `refdata.st` 只 glob 当前目录，未用 as-of 索引；
      快照多时（>几百）考虑加 manifest 文件。
- [ ] `OhlcvPanelStore._missing_codes` 目前只按 `nunique(date) < needed` 判；
      这对"中间有若干 trade-day 缺失"的股票会误报为 fully-missing，过度拉。
      改为"对齐 trade_calendar 的 (code, date) 缺失集"更精确。
- [ ] `listing_dates_job` 增量缺口告警阈值目前未实现；接到 Sub-4 监控时再加。
- [ ] `LimitFilter` 仅判 limit-up；limit-down 同日卖出约束留给 Sub-3。
- [ ] `LimitFilter` 名称略宽于行为；如要扩 limit-down 可拆 `LimitUpFilter` /
      `LimitDownFilter`。

## C. Deferred / out-of-scope

- 历史 ST / 历史行业快照（tushare 商业接口或累积 N 年自拉）。
- 申万行业、证监会行业。
- Tushare 实现 listing / ST / industry 三件。
- `instock/portfolio/pipeline.py::_load_ohlcv_panel("ALL", ...)` 仍是 Sub-2
  遗留占位；Sub-3 backtester 引入 `OhlcvPanelStore` 注入后即可清理。

## D. Sub-3 entry hand-off

Sub-3 backtester should:

1. Receive an `OhlcvPanelStore` instance instead of calling
   `IDataSource.get_ohlcv` directly.
2. Query `read_industry_map(at)` / `read_st_flags(at)` /
   `read_listing_dates()` as-of each rebalance date.
3. Emit report watermark:
   "refdata as of YYYY-MM-DD; historical ST/industry approximated".
4. Default `FilterChain` should be `[Suspended, NewListing(refdata), ST,
   Limit]` to match the contract Sub-2.5 ships.
