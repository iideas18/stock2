# Sub-4 follow-ups

## Deferred
1. Legacy cutover: old `dataTableHandler` → new strategy browser
   (after 1–2 weeks parallel run).
2. Move existing handlers into `instock/web/handlers/` (purely cosmetic).
3. LRU caching at Parquet readers if any page > 2s.
4. User-editable alert thresholds (YAML).
5. Alert cooldown / silencing windows.
6. History chart per check (timeline graph).
7. Auth if portal ever exposed beyond trusted network.

## Known gaps
- `_default_icir_loader` returns empty until an ohlcv-based fwd_ret join
  is wired in; unit tests currently exercise only injected loaders.
- Evaluator HTML report path in `FactorDetailHandler` assumes
  `data/factor_reports/<name>.html` — Sub-1 doesn't emit there by default.
  Either wire Sub-1 evaluator to write into that directory, or adjust the
  handler to locate the latest evaluator output.
