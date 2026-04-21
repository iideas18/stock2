# Sub-project 4 — Research Web & Monitoring (Design)

**Date**: 2026-04-21
**Status**: Approved for implementation planning
**Depends on**: Sub-1 (factors), Sub-2 (HoldingSchedule), Sub-3 (backtest engine)

---

## 1. Scope

Sub-4 delivers a **Tornado-hosted research portal** serving the InStock
research workflow: factor browsing, strategy inspection, backtest review,
and subsystem monitoring. Runs as new handlers in the existing
`instock/web/` app — no new service, no new process, no login.

### In scope (MVP)

1. **Factor browser** — factor list + per-factor detail with latest-date
   coverage and embedded Sub-1 evaluator HTML report.
2. **Strategy browser** — strategy list + per-strategy detail with latest
   `HoldingSchedule`, 7-day diff, and the list of its backtest run_ids.
3. **Backtest viewer** — runs list (from `_metrics.parquet`) + per-run page
   embedding `<run_id>/report.html` via iframe.
4. **Monitoring dashboard** — subsystem status rows (GREEN/YELLOW/RED),
   30-day IC_IR sparkline per factor, job-run history table, per-row ack.
5. **Webhook alerter** — posts JSON on RED-transition only; env-driven URL.
6. **Scheduler snippets** — cron `.sh` files for factor_compute /
   generate_holdings / backtest_run / refdata_* / monitoring_check.

### Explicitly out of scope

- **Legacy cutover** — old selection pages (`dataTableHandler`, etc.)
  untouched; cutover decided after 1–2 weeks of parallel running, becomes
  a Sub-4 follow-up.
- **Auth** — internal tool; no login, no sessions.
- **Interactive charting** — all charts are static PNG inline (base64),
  matching Sub-1/Sub-3 report style.
- **User-editable alert thresholds** — thresholds live in Python
  constants; adjust by code change.
- **External scheduler** (APScheduler/Prefect/airflow).
- **Per-user alert rules, cooldowns, silencing windows** beyond the
  one built-in rule (fire only on RED transition).

---

## 2. Architecture

Read-on-request; no caches in MVP. If a page is measured slow, add
`functools.lru_cache` at the Parquet reader (not the handler).

```
Browser ─HTTP──> Tornado (instock/web/web_service.py)
                     │
                     ├─ FactorHandler       ─┐
                     ├─ StrategyHandler      ├─ read Parquet
                     ├─ BacktestHandler      │    via existing readers
                     └─ MonitoringHandler   ─┘
                           │
                           └─> instock/monitoring/runner.run_all_checks()

cron (*/15 min) ─> monitoring_check_job ──┬─> StatusCheck.run()
                                          ├─> AlertStateStore.record()
                                          └─> webhook.post()  [on RED transition]
```

---

## 3. Components & file layout

```
instock/web/
  web_service.py                 # EDIT: register new routes
  handlers/                      # NEW subpackage
    __init__.py
    factor_handler.py            # /factors, /factors/<name>
    strategy_handler.py          # /strategies, /strategies/<name>
    backtest_handler.py          # /backtests, /backtests/<run_id>
    monitoring_handler.py        # /monitoring, POST /monitoring/ack
  templates/
    factors_list.html
    factor_detail.html
    strategies_list.html
    strategy_detail.html
    backtests_list.html
    backtest_detail.html
    monitoring.html

instock/monitoring/              # NEW package
  __init__.py
  status.py                      # StatusCheck ABC + implementations
  state.py                       # AlertStateStore (Parquet append-only)
  webhook.py                     # post_webhook(url, payload)
  runner.py                      # run_all_checks() + CLI entry

instock/job/
  monitoring_check_job.py        # NEW — cron entry

cron/
  daily_factor_compute.sh        # NEW
  daily_generate_holdings.sh     # NEW
  daily_backtest_run.sh          # NEW
  weekly_refdata_refresh.sh      # NEW
  monitoring_check.sh            # NEW (*/15 min)
```

Existing handlers (`dataIndicatorsHandler.py`, etc.) stay at their current
path. Optional future refactor to `handlers/` is out of scope.

---

## 4. Key interfaces

### `StatusCheck` (ABC)

```python
@dataclass(frozen=True)
class StatusRow:
    name: str
    status: str              # "GREEN" | "YELLOW" | "RED" | "ACK"
    message: str
    metric_value: float | None
    as_of: pd.Timestamp

class StatusCheck(ABC):
    name: str
    def run(self) -> StatusRow: ...
```

### Built-in checks (MVP)

- `ArtifactFreshness(name, loader, max_age_days_yellow, max_age_days_red)`
  — reused for: one per registered factor; one per strategy; one for
  `_metrics.parquet`; one per refdata (industry / listing / st).
- `ICIRDecay(factor, window_days=30, threshold=0.1)` — calls Sub-1
  evaluator's rolling IC_IR helper.
- `DataSourceRate(source="akshare", window="1d", threshold=0.9)` —
  reads `data/log/api_calls.parquet`.
- `DataSourceRate` requires adding a one-line structured log write to the
  existing `RateLimiter` / retry decorator (the only cross-cutting edit
  outside `instock/web/` and `instock/monitoring/`).

### `AlertStateStore`

```python
class AlertStateStore:
    def __init__(self, root: Path): ...
    def record(self, row: StatusRow) -> None: ...     # append
    def last_status(self, check_name: str) -> StatusRow | None: ...
    def history(self, check_name: str, n: int = 30) -> pd.DataFrame: ...
```

Storage: `<INSTOCK_MONITORING_ROOT>/_alerts.parquet`, append-only with
schema `(check_name, status, message, metric_value, as_of,
recorded_at)`. `INSTOCK_MONITORING_ROOT` defaults to `data/monitoring`.

### Webhook

```python
def post_webhook(url: str, payload: dict, timeout: float = 5.0) -> bool:
    # returns False on any failure (caller logs); never raises
```

### `run_all_checks`

```python
def run_all_checks() -> list[StatusRow]:
    # discovers checks from a module-level registry;
    # per-check try/except wraps failures into a RED StatusRow.
```

---

## 5. Data flow

### Page render (all 4 viewers)

1. Handler reads Parquet via existing readers (`read_factor`,
   `read_holding`, `bt_storage.read_run`, `pd.read_parquet`).
2. Coerces to a small dict/list.
3. Calls `self.render(template, **data)`.

### Monitoring dashboard

`GET /monitoring`:
1. `runner.run_all_checks()` runs live (~seconds).
2. `AlertStateStore.last_status(name)` picks up prior ACKs.
3. UI shows current row, merging ACK state so an acknowledged RED
   displays as "RED (ack'd)" until status flips.

`POST /monitoring/ack` with `{check_name}`: appends a row with
`status="ACK"` and `as_of=now()`. Ack clears automatically when the next
check flips to GREEN (new GREEN row supersedes the ACK).

### Monitoring job (cron, every 15 min)

```
for check in registry:
    row = check.run()           # try/except baked in
    prev = state.last_status(check.name)
    state.record(row)
    if row.status == "RED" and (prev is None or prev.status != "RED"):
        webhook.post(INSTOCK_WEBHOOK_URL, row.to_dict())
```

Fires only on `GREEN/YELLOW/ACK → RED` transitions. Consecutive RED rows
do not re-fire. Empty `INSTOCK_WEBHOOK_URL` disables webhook.

---

## 6. Error handling

### Handlers

- Missing Parquet → "no data yet" banner, HTTP 200.
- Empty registry → empty list + help text.
- Corrupt Parquet → log + "failed to load: <short reason>" row.
- Unknown factor / strategy / run_id → 404 with back-link.

### Monitoring

- Check raises → caught in `runner`; row becomes
  `RED("check failed: <exc>")`. One broken check cannot blank the page.
- Webhook post fails → WARNING log, swallow. Cron job always exits 0.

---

## 7. Testing

One test file per unit.

### `tests/web/`

- `test_factor_handler.py` — `AsyncHTTPTestCase`, `tmp_path` fixture
  Parquet, assert 200 + factor name in body.
- `test_strategy_handler.py` — similar + 7-day diff assertion.
- `test_backtest_handler.py` — list page shows run_ids; detail page
  contains `<iframe`.
- `test_monitoring_handler.py` — RED row renders, ACK POST round-trips.

### `tests/monitoring/`

- `test_status_artifact_freshness.py` — fresh = GREEN, stale = RED.
- `test_status_icir_decay.py` — below threshold = RED.
- `test_status_datasource_rate.py` — below threshold = RED; missing log
  = YELLOW with "no data yet".
- `test_state_store.py` — record/load/history roundtrip.
- `test_webhook.py` — monkeypatch `requests.post`, assert payload shape;
  assert swallow on failure.
- `test_monitoring_check_job.py` — prev=GREEN, new=RED → webhook called;
  prev=RED, new=RED → not called; prev=None, new=RED → called.

No real-data smoke tests (Sub-1/2/3 cover that).

---

## 8. Deployment

- `web_service.py` already runs under supervisor (`supervisor/`). Deploy
  = pull + restart; new routes live.
- cron snippets land in `cron/`; operator wires into crontab.
- Env vars added to `README.md`:
  - `INSTOCK_WEBHOOK_URL` (empty → no webhook)
  - `INSTOCK_MONITORING_ROOT` (default `data/monitoring`)
- No DB migrations. Alert state is Parquet.

---

## 9. Success criteria (MVP acceptance)

- Researcher can, in a browser:
  - list and inspect every registered factor, including its evaluator
    report;
  - list and inspect every strategy with its latest holdings + diff;
  - list and open every completed backtest;
  - see one dashboard that shows whether today's factor / holding /
    backtest / refdata jobs succeeded and whether IC_IR is intact.
- A RED transition on any configured check results in exactly one
  webhook POST.
- Full test suite stays green.
- No new FutureWarnings.

---

## 10. Follow-ups (deferred)

1. Legacy page cutover (old selection table → HoldingSchedule) after
   1–2 weeks of parallel running.
2. Optional handler refactor: move existing `dataIndicatorsHandler` etc.
   into `instock/web/handlers/`.
3. LRU caching at factor reader if any page measured > 2 s.
4. User-editable alert thresholds (UI + YAML).
5. Alert cooldown / silencing windows.
6. History chart for AlertStateStore (per-check timeline graph).
7. Authentication if portal ever exposed beyond a trusted network.
