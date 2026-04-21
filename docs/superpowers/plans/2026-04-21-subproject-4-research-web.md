# Sub-project 4 — Research Web & Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Tornado-hosted research portal (factor / strategy / backtest browsers + monitoring dashboard + webhook alerter + cron snippets) on top of the existing `instock/web/` app.

**Architecture:** New handlers under `instock/web/handlers/` register fresh Tornado routes beside the legacy ones; all page data is read on-request from existing Parquet readers (Sub-1/2/3). A new `instock/monitoring/` package hosts `StatusCheck` implementations, an append-only Parquet `AlertStateStore`, and a webhook poster. A cron-driven job (`monitoring_check_job`) runs every 15 min, records each row, and posts a webhook only on `prev != RED → RED` transitions. No new service, no auth, no cache.

**Tech Stack:** Python 3.12, Tornado (existing `web_service.py`), pandas + pyarrow Parquet, Jinja2 (Tornado template loader), pandera, pytest + `tornado.testing.AsyncHTTPTestCase`, `requests` (for webhook).

**Spec:** `docs/superpowers/specs/2026-04-21-subproject-4-research-web-design.md`

---

## File Structure

**New:**
- `instock/monitoring/__init__.py`
- `instock/monitoring/status.py` — `StatusRow`, `StatusCheck` ABC, `ArtifactFreshness`, `ICIRDecay`, `DataSourceRate`
- `instock/monitoring/state.py` — `AlertStateStore`
- `instock/monitoring/webhook.py` — `post_webhook`
- `instock/monitoring/runner.py` — `run_all_checks`, module-level `REGISTRY`, `register_check`
- `instock/monitoring/registry_bootstrap.py` — `register_default_checks()` (idempotent, similar to `factors/bootstrap.py`)
- `instock/web/handlers/__init__.py`
- `instock/web/handlers/factor_handler.py`
- `instock/web/handlers/strategy_handler.py`
- `instock/web/handlers/backtest_handler.py`
- `instock/web/handlers/monitoring_handler.py`
- `instock/web/templates/factors_list.html`
- `instock/web/templates/factor_detail.html`
- `instock/web/templates/strategies_list.html`
- `instock/web/templates/strategy_detail.html`
- `instock/web/templates/backtests_list.html`
- `instock/web/templates/backtest_detail.html`
- `instock/web/templates/monitoring.html`
- `instock/job/monitoring_check_job.py`
- `cron/daily_factor_compute.sh`
- `cron/daily_generate_holdings.sh`
- `cron/daily_backtest_run.sh`
- `cron/weekly_refdata_refresh.sh`
- `cron/monitoring_check.sh`
- `tests/monitoring/__init__.py`
- `tests/monitoring/test_status_artifact_freshness.py`
- `tests/monitoring/test_status_icir_decay.py`
- `tests/monitoring/test_status_datasource_rate.py`
- `tests/monitoring/test_state_store.py`
- `tests/monitoring/test_webhook.py`
- `tests/monitoring/test_runner.py`
- `tests/job/test_monitoring_check_job.py`
- `tests/web/test_factor_handler.py`
- `tests/web/test_strategy_handler.py`
- `tests/web/test_backtest_handler.py`
- `tests/web/test_monitoring_handler.py`
- `docs/superpowers/followups/subproject-4-research-web.md`

**Modify:**
- `instock/web/web_service.py` — register the 4 new route groups
- `instock/datasource/io.py` — add one-line structured log write in `with_retry` / RateLimiter path (`data/log/api_calls.parquet`)
- `README.md` — new env vars
- `docs/superpowers/roadmap.md` — flip Sub-4 to ✅ when done

---

## Task 1: Package skeleton + monitoring root env

**Files:**
- Create: `instock/monitoring/__init__.py`
- Create: `instock/web/handlers/__init__.py`
- Create: `tests/monitoring/__init__.py`
- Create: `tests/monitoring/test_root.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/monitoring/test_root.py
from __future__ import annotations

import os
from pathlib import Path

from instock.monitoring import get_monitoring_root


def test_default_root(monkeypatch):
    monkeypatch.delenv("INSTOCK_MONITORING_ROOT", raising=False)
    assert get_monitoring_root() == Path("data/monitoring")


def test_override_root(monkeypatch, tmp_path):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    assert get_monitoring_root() == tmp_path
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/monitoring/test_root.py -v`
Expected: ImportError (module does not exist)

- [ ] **Step 3: Create skeleton**

```python
# instock/monitoring/__init__.py
"""InStock monitoring package — status checks, alert state, webhooks."""
from __future__ import annotations

import os
from pathlib import Path

__all__ = ["get_monitoring_root"]


def get_monitoring_root() -> Path:
    return Path(os.environ.get("INSTOCK_MONITORING_ROOT", "data/monitoring"))
```

```python
# instock/web/handlers/__init__.py
"""Tornado handlers for Sub-4 research portal."""
```

```python
# tests/monitoring/__init__.py
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/monitoring/test_root.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add instock/monitoring/__init__.py instock/web/handlers/__init__.py \
        tests/monitoring/__init__.py tests/monitoring/test_root.py
git commit -m "feat(sub4): monitoring package skeleton + root env var"
```

---

## Task 2: StatusRow + StatusCheck ABC + ArtifactFreshness

**Files:**
- Create: `instock/monitoring/status.py`
- Create: `tests/monitoring/test_status_artifact_freshness.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/monitoring/test_status_artifact_freshness.py
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from instock.monitoring.status import ArtifactFreshness, StatusRow


def _loader_returning(ts: pd.Timestamp | None):
    def _loader() -> pd.Timestamp | None:
        return ts
    return _loader


def test_row_dataclass_frozen():
    r = StatusRow(
        name="x", status="GREEN", message="ok",
        metric_value=1.0, as_of=pd.Timestamp("2026-04-21"),
    )
    with pytest.raises(Exception):
        r.name = "y"  # frozen


def test_fresh_returns_green():
    now = pd.Timestamp.now()
    chk = ArtifactFreshness(
        name="f.mom_5d",
        loader=_loader_returning(now - pd.Timedelta(hours=1)),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    row = chk.run()
    assert row.status == "GREEN"
    assert row.name == "f.mom_5d"


def test_stale_red():
    old = pd.Timestamp.now() - pd.Timedelta(days=10)
    chk = ArtifactFreshness(
        name="f.mom_5d", loader=_loader_returning(old),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    assert chk.run().status == "RED"


def test_mid_yellow():
    mid = pd.Timestamp.now() - pd.Timedelta(days=2)
    chk = ArtifactFreshness(
        name="f.mom_5d", loader=_loader_returning(mid),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    assert chk.run().status == "YELLOW"


def test_none_loader_yellow():
    chk = ArtifactFreshness(
        name="f.mom_5d", loader=_loader_returning(None),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    row = chk.run()
    assert row.status == "YELLOW"
    assert "no data" in row.message.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/monitoring/test_status_artifact_freshness.py -v`
Expected: ImportError

- [ ] **Step 3: Implement**

```python
# instock/monitoring/status.py
"""Status check ABC and built-in implementations."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

log = logging.getLogger(__name__)

_STATUSES = ("GREEN", "YELLOW", "RED", "ACK")


@dataclass(frozen=True)
class StatusRow:
    name: str
    status: str
    message: str
    metric_value: float | None
    as_of: pd.Timestamp

    def __post_init__(self) -> None:
        if self.status not in _STATUSES:
            raise ValueError(f"invalid status: {self.status!r}")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "metric_value": self.metric_value,
            "as_of": self.as_of.isoformat() if self.as_of is not None else None,
        }


class StatusCheck(ABC):
    name: str

    @abstractmethod
    def run(self) -> StatusRow: ...


class ArtifactFreshness(StatusCheck):
    """GREEN if latest as-of date within max_age_days_yellow,
    YELLOW if within max_age_days_red, RED otherwise. None loader = YELLOW."""

    def __init__(
        self,
        name: str,
        loader: Callable[[], pd.Timestamp | None],
        max_age_days_yellow: float,
        max_age_days_red: float,
    ) -> None:
        self.name = name
        self._loader = loader
        self._y = max_age_days_yellow
        self._r = max_age_days_red

    def run(self) -> StatusRow:
        now = pd.Timestamp.now()
        try:
            ts = self._loader()
        except Exception as exc:
            return StatusRow(
                name=self.name, status="RED",
                message=f"loader failed: {exc}",
                metric_value=None, as_of=now,
            )
        if ts is None:
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no data yet",
                metric_value=None, as_of=now,
            )
        age_days = (now - ts).total_seconds() / 86400.0
        if age_days <= self._y:
            status = "GREEN"
        elif age_days <= self._r:
            status = "YELLOW"
        else:
            status = "RED"
        return StatusRow(
            name=self.name, status=status,
            message=f"age={age_days:.2f}d (latest={ts.date()})",
            metric_value=age_days, as_of=now,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/monitoring/test_status_artifact_freshness.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add instock/monitoring/status.py tests/monitoring/test_status_artifact_freshness.py
git commit -m "feat(sub4): StatusRow + StatusCheck ABC + ArtifactFreshness"
```

---

## Task 3: ICIRDecay check

**Files:**
- Modify: `instock/monitoring/status.py` (append)
- Create: `tests/monitoring/test_status_icir_decay.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/monitoring/test_status_icir_decay.py
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from instock.monitoring.status import ICIRDecay


def _make_factor(mean_ic: float, n_days: int = 40) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.bdate_range("2026-01-01", periods=n_days)
    rows = []
    for d in dates:
        codes = [f"{i:06d}" for i in range(50)]
        values = rng.standard_normal(50)
        fwd = values * mean_ic + rng.standard_normal(50) * 0.1
        for c, v, f in zip(codes, values, fwd):
            rows.append({"date": d, "code": c, "value": v, "fwd_ret": f})
    return pd.DataFrame(rows)


def test_healthy_factor_green(monkeypatch):
    df = _make_factor(mean_ic=0.3)
    chk = ICIRDecay(factor="mom_5d", window_days=30, threshold=0.1,
                    frame_loader=lambda name: df)
    row = chk.run()
    assert row.status == "GREEN"
    assert row.metric_value > 0.1


def test_decayed_factor_red():
    df = _make_factor(mean_ic=0.0)
    chk = ICIRDecay(factor="mom_5d", window_days=30, threshold=0.1,
                    frame_loader=lambda name: df)
    row = chk.run()
    assert row.status == "RED"


def test_empty_data_yellow():
    chk = ICIRDecay(factor="mom_5d", window_days=30, threshold=0.1,
                    frame_loader=lambda name: pd.DataFrame(
                        columns=["date", "code", "value", "fwd_ret"]
                    ))
    assert chk.run().status == "YELLOW"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/monitoring/test_status_icir_decay.py -v`
Expected: ImportError (ICIRDecay)

- [ ] **Step 3: Implement**

Append to `instock/monitoring/status.py`:

```python
class ICIRDecay(StatusCheck):
    """|IC_IR| over last window_days < threshold → RED. Empty data → YELLOW.

    frame_loader returns a DataFrame with columns (date, code, value, fwd_ret)
    per factor name. If not provided, falls back to reading from Sub-1
    factor storage and joining next-day returns from ohlcv cache.
    """

    def __init__(
        self,
        factor: str,
        window_days: int = 30,
        threshold: float = 0.1,
        frame_loader: Callable[[str], pd.DataFrame] | None = None,
    ) -> None:
        self.name = f"icir.{factor}"
        self._factor = factor
        self._window = window_days
        self._threshold = threshold
        self._loader = frame_loader or _default_icir_loader

    def run(self) -> StatusRow:
        now = pd.Timestamp.now()
        try:
            df = self._loader(self._factor)
        except Exception as exc:
            return StatusRow(
                name=self.name, status="RED",
                message=f"loader failed: {exc}",
                metric_value=None, as_of=now,
            )
        if df.empty:
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no data yet", metric_value=None, as_of=now,
            )
        cutoff = df["date"].max() - pd.Timedelta(days=self._window)
        window = df[df["date"] >= cutoff]
        ic_per_day = (
            window.groupby("date")
            .apply(lambda g: g["value"].corr(g["fwd_ret"], method="spearman"))
            .dropna()
        )
        if len(ic_per_day) < 5:
            return StatusRow(
                name=self.name, status="YELLOW",
                message=f"insufficient days ({len(ic_per_day)})",
                metric_value=None, as_of=now,
            )
        mean_ic = float(ic_per_day.mean())
        std_ic = float(ic_per_day.std(ddof=1))
        icir = abs(mean_ic / std_ic) if std_ic > 0 else 0.0
        status = "GREEN" if icir >= self._threshold else "RED"
        return StatusRow(
            name=self.name, status=status,
            message=f"IC_IR={icir:.3f} over {len(ic_per_day)}d",
            metric_value=icir, as_of=now,
        )


def _default_icir_loader(factor: str) -> pd.DataFrame:
    # Best-effort: wire into Sub-1 read_factor + Sub-2.5 ohlcv cache.
    # Kept intentionally minimal; unit tests inject a loader instead.
    from instock.factors.storage import read_factor
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=90)
    df = read_factor(factor, start, end)
    if df.empty:
        return df
    # Fwd return from ohlcv cache is environment-dependent; return empty
    # fwd_ret so that the check degrades to YELLOW when ohlcv not available.
    df = df.copy()
    df["fwd_ret"] = float("nan")
    return df.dropna(subset=["fwd_ret"])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/monitoring/test_status_icir_decay.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add instock/monitoring/status.py tests/monitoring/test_status_icir_decay.py
git commit -m "feat(sub4): ICIRDecay status check"
```

---

## Task 4: DataSourceRate check + structured call log

**Files:**
- Modify: `instock/datasource/io.py` (append structured log writer)
- Modify: `instock/monitoring/status.py` (append DataSourceRate)
- Create: `tests/monitoring/test_status_datasource_rate.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/monitoring/test_status_datasource_rate.py
from __future__ import annotations

from pathlib import Path

import pandas as pd

from instock.monitoring.status import DataSourceRate


def _write_log(path: Path, rows):
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_no_log_yellow(tmp_path, monkeypatch):
    chk = DataSourceRate(
        source="akshare", window_hours=24, threshold=0.9,
        log_path=tmp_path / "missing.parquet",
    )
    assert chk.run().status == "YELLOW"


def test_high_rate_green(tmp_path):
    now = pd.Timestamp.now()
    rows = [
        {"ts": now - pd.Timedelta(hours=1), "source": "akshare", "ok": True},
    ] * 18 + [
        {"ts": now - pd.Timedelta(hours=1), "source": "akshare", "ok": False},
    ] * 2
    _write_log(tmp_path / "api_calls.parquet", rows)
    chk = DataSourceRate("akshare", 24, 0.9, tmp_path / "api_calls.parquet")
    assert chk.run().status == "GREEN"


def test_low_rate_red(tmp_path):
    now = pd.Timestamp.now()
    rows = [{"ts": now, "source": "akshare", "ok": False}] * 10 + \
           [{"ts": now, "source": "akshare", "ok": True}] * 1
    _write_log(tmp_path / "api_calls.parquet", rows)
    chk = DataSourceRate("akshare", 24, 0.9, tmp_path / "api_calls.parquet")
    assert chk.run().status == "RED"


def test_outside_window_ignored(tmp_path):
    now = pd.Timestamp.now()
    rows = [{"ts": now - pd.Timedelta(days=30), "source": "akshare",
             "ok": False}] * 100
    _write_log(tmp_path / "api_calls.parquet", rows)
    chk = DataSourceRate("akshare", 24, 0.9, tmp_path / "api_calls.parquet")
    assert chk.run().status == "YELLOW"  # degrades: no rows in window
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/monitoring/test_status_datasource_rate.py -v`
Expected: ImportError

- [ ] **Step 3: Append `log_call` to `instock/datasource/io.py`**

Append (do not modify existing `with_retry` logic; just add a new helper that callers or a lightweight decorator hook can use):

```python
# Append to instock/datasource/io.py

import os
from pathlib import Path

import pandas as pd

_LOG_DEFAULT = Path("data/log/api_calls.parquet")


def log_call(source: str, ok: bool, path: Path | None = None) -> None:
    """Append one structured row to the api_calls parquet log. Never raises."""
    try:
        target = Path(path) if path is not None else Path(
            os.environ.get("INSTOCK_API_CALL_LOG", _LOG_DEFAULT)
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        row = pd.DataFrame(
            [{"ts": pd.Timestamp.now(), "source": source, "ok": bool(ok)}]
        )
        if target.exists():
            old = pd.read_parquet(target)
            pd.concat([old, row], ignore_index=True).to_parquet(
                target, index=False
            )
        else:
            row.to_parquet(target, index=False)
    except Exception as exc:  # noqa: BLE001
        log.warning("log_call failed: %s", exc)
```

Then in the `with_retry` wrapper, after success/final failure, fire `log_call`. Edit the `wrapper` function body to call `log_call(fn.__module__.split('.')[-1].replace('_source',''), ok=True)` on success and `ok=False` on final failure before raising. Keep the change small — one line per branch.

- [ ] **Step 4: Append `DataSourceRate` to `instock/monitoring/status.py`**

```python
class DataSourceRate(StatusCheck):
    """Success rate of recent data source calls. Missing log = YELLOW."""

    def __init__(
        self,
        source: str,
        window_hours: float = 24.0,
        threshold: float = 0.9,
        log_path: Path | None = None,
    ) -> None:
        self.name = f"datasource.{source}"
        self._source = source
        self._window_hours = window_hours
        self._threshold = threshold
        self._log_path = (
            Path(log_path) if log_path is not None
            else Path("data/log/api_calls.parquet")
        )

    def run(self) -> StatusRow:
        now = pd.Timestamp.now()
        if not self._log_path.exists():
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no data yet", metric_value=None, as_of=now,
            )
        try:
            df = pd.read_parquet(self._log_path)
        except Exception as exc:
            return StatusRow(
                name=self.name, status="RED",
                message=f"log load failed: {exc}",
                metric_value=None, as_of=now,
            )
        cutoff = now - pd.Timedelta(hours=self._window_hours)
        window = df[(df["source"] == self._source) & (df["ts"] >= cutoff)]
        if window.empty:
            return StatusRow(
                name=self.name, status="YELLOW",
                message="no calls in window",
                metric_value=None, as_of=now,
            )
        rate = float(window["ok"].mean())
        status = "GREEN" if rate >= self._threshold else "RED"
        return StatusRow(
            name=self.name, status=status,
            message=f"ok_rate={rate:.3f} over {len(window)} calls",
            metric_value=rate, as_of=now,
        )
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/monitoring/test_status_datasource_rate.py tests/datasource -v`
Expected: all pass (no regression in datasource tests)

- [ ] **Step 6: Commit**

```bash
git add instock/datasource/io.py instock/monitoring/status.py \
        tests/monitoring/test_status_datasource_rate.py
git commit -m "feat(sub4): DataSourceRate check + structured api_calls log"
```

---

## Task 5: AlertStateStore

**Files:**
- Create: `instock/monitoring/state.py`
- Create: `tests/monitoring/test_state_store.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/monitoring/test_state_store.py
from __future__ import annotations

from pathlib import Path

import pandas as pd

from instock.monitoring.state import AlertStateStore
from instock.monitoring.status import StatusRow


def _row(name: str, status: str) -> StatusRow:
    return StatusRow(
        name=name, status=status, message="",
        metric_value=None, as_of=pd.Timestamp.now(),
    )


def test_empty_last_status_none(tmp_path):
    st = AlertStateStore(tmp_path)
    assert st.last_status("x") is None


def test_record_roundtrip(tmp_path):
    st = AlertStateStore(tmp_path)
    r = _row("chk", "GREEN")
    st.record(r)
    got = st.last_status("chk")
    assert got is not None
    assert got.name == "chk"
    assert got.status == "GREEN"


def test_last_status_returns_latest(tmp_path):
    st = AlertStateStore(tmp_path)
    st.record(_row("chk", "GREEN"))
    st.record(_row("chk", "RED"))
    assert st.last_status("chk").status == "RED"


def test_history_limits(tmp_path):
    st = AlertStateStore(tmp_path)
    for s in ["GREEN"] * 5 + ["RED"] * 5:
        st.record(_row("chk", s))
    hist = st.history("chk", n=3)
    assert len(hist) == 3
    assert list(hist["status"]) == ["RED", "RED", "RED"]


def test_other_check_isolated(tmp_path):
    st = AlertStateStore(tmp_path)
    st.record(_row("a", "GREEN"))
    st.record(_row("b", "RED"))
    assert st.last_status("a").status == "GREEN"
    assert st.last_status("b").status == "RED"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/monitoring/test_state_store.py -v`
Expected: ImportError

- [ ] **Step 3: Implement**

```python
# instock/monitoring/state.py
"""Append-only Parquet store for monitoring rows."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .status import StatusRow

log = logging.getLogger(__name__)

_COLUMNS = [
    "check_name", "status", "message", "metric_value",
    "as_of", "recorded_at",
]


class AlertStateStore:
    def __init__(self, root: Path) -> None:
        self._path = Path(root) / "_alerts.parquet"
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _read(self) -> pd.DataFrame:
        if not self._path.exists():
            return pd.DataFrame(columns=_COLUMNS)
        return pd.read_parquet(self._path)

    def record(self, row: StatusRow) -> None:
        new = pd.DataFrame([{
            "check_name": row.name,
            "status": row.status,
            "message": row.message,
            "metric_value": row.metric_value,
            "as_of": row.as_of,
            "recorded_at": pd.Timestamp.now(),
        }], columns=_COLUMNS)
        df = pd.concat([self._read(), new], ignore_index=True)
        df.to_parquet(self._path, index=False)

    def last_status(self, check_name: str) -> StatusRow | None:
        df = self._read()
        df = df[df["check_name"] == check_name]
        if df.empty:
            return None
        row = df.sort_values("recorded_at").iloc[-1]
        return StatusRow(
            name=row["check_name"],
            status=row["status"],
            message=row["message"] or "",
            metric_value=(
                float(row["metric_value"])
                if pd.notna(row["metric_value"]) else None
            ),
            as_of=pd.Timestamp(row["as_of"]),
        )

    def history(self, check_name: str, n: int = 30) -> pd.DataFrame:
        df = self._read()
        df = df[df["check_name"] == check_name].sort_values(
            "recorded_at", ascending=False
        ).head(n)
        return df.reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/monitoring/test_state_store.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add instock/monitoring/state.py tests/monitoring/test_state_store.py
git commit -m "feat(sub4): AlertStateStore (append-only Parquet)"
```

---

## Task 6: Webhook poster

**Files:**
- Create: `instock/monitoring/webhook.py`
- Create: `tests/monitoring/test_webhook.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/monitoring/test_webhook.py
from __future__ import annotations

import pytest

from instock.monitoring import webhook as wh


class _FakeResp:
    def __init__(self, status: int):
        self.status_code = status
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_empty_url_false(monkeypatch):
    assert wh.post_webhook("", {"a": 1}) is False


def test_success_true(monkeypatch):
    captured = {}
    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeResp(200)
    monkeypatch.setattr(wh.requests, "post", fake_post)
    assert wh.post_webhook("http://x", {"a": 1}) is True
    assert captured["url"] == "http://x"
    assert captured["json"] == {"a": 1}


def test_exception_swallowed(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("network down")
    monkeypatch.setattr(wh.requests, "post", boom)
    assert wh.post_webhook("http://x", {"a": 1}) is False


def test_http_error_swallowed(monkeypatch):
    monkeypatch.setattr(
        wh.requests, "post",
        lambda url, json, timeout: _FakeResp(500),
    )
    assert wh.post_webhook("http://x", {"a": 1}) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/monitoring/test_webhook.py -v`
Expected: ImportError

- [ ] **Step 3: Implement**

```python
# instock/monitoring/webhook.py
"""Fire-and-forget webhook poster. Never raises."""
from __future__ import annotations

import logging

import requests

log = logging.getLogger(__name__)


def post_webhook(url: str, payload: dict, timeout: float = 5.0) -> bool:
    if not url:
        return False
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("webhook post failed: %s", exc)
        return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/monitoring/test_webhook.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add instock/monitoring/webhook.py tests/monitoring/test_webhook.py
git commit -m "feat(sub4): post_webhook helper (swallow failure)"
```

---

## Task 7: Runner + check registry

**Files:**
- Create: `instock/monitoring/runner.py`
- Create: `instock/monitoring/registry_bootstrap.py`
- Create: `tests/monitoring/test_runner.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/monitoring/test_runner.py
from __future__ import annotations

import pandas as pd
import pytest

from instock.monitoring import runner as rn
from instock.monitoring.status import StatusCheck, StatusRow


class _Good(StatusCheck):
    name = "good"
    def run(self) -> StatusRow:
        return StatusRow(
            name=self.name, status="GREEN", message="ok",
            metric_value=1.0, as_of=pd.Timestamp.now(),
        )


class _Bad(StatusCheck):
    name = "bad"
    def run(self) -> StatusRow:
        raise RuntimeError("kaboom")


@pytest.fixture(autouse=True)
def _clear():
    rn.clear_registry()
    yield
    rn.clear_registry()


def test_empty_registry_empty_list():
    assert rn.run_all_checks() == []


def test_good_check_runs():
    rn.register_check(_Good())
    rows = rn.run_all_checks()
    assert len(rows) == 1
    assert rows[0].status == "GREEN"


def test_bad_check_becomes_red():
    rn.register_check(_Bad())
    rows = rn.run_all_checks()
    assert len(rows) == 1
    assert rows[0].status == "RED"
    assert "kaboom" in rows[0].message


def test_one_bad_does_not_block_others():
    rn.register_check(_Good())
    rn.register_check(_Bad())
    rows = rn.run_all_checks()
    assert {r.status for r in rows} == {"GREEN", "RED"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/monitoring/test_runner.py -v`
Expected: ImportError

- [ ] **Step 3: Implement**

```python
# instock/monitoring/runner.py
"""Run-all-checks entry point with per-check exception guard."""
from __future__ import annotations

import logging

import pandas as pd

from .status import StatusCheck, StatusRow

log = logging.getLogger(__name__)

_REGISTRY: list[StatusCheck] = []


def register_check(check: StatusCheck) -> None:
    _REGISTRY.append(check)


def clear_registry() -> None:
    _REGISTRY.clear()


def run_all_checks() -> list[StatusRow]:
    rows: list[StatusRow] = []
    for chk in _REGISTRY:
        try:
            rows.append(chk.run())
        except Exception as exc:  # noqa: BLE001
            rows.append(StatusRow(
                name=getattr(chk, "name", chk.__class__.__name__),
                status="RED",
                message=f"check failed: {exc}",
                metric_value=None,
                as_of=pd.Timestamp.now(),
            ))
    return rows
```

```python
# instock/monitoring/registry_bootstrap.py
"""Idempotent default check registration."""
from __future__ import annotations

import logging
from pathlib import Path

from .runner import clear_registry, register_check
from .status import ArtifactFreshness, DataSourceRate, ICIRDecay

log = logging.getLogger(__name__)

_REGISTERED = False


def register_default_checks(*, force: bool = False) -> None:
    global _REGISTERED
    if _REGISTERED and not force:
        return
    clear_registry()
    # Factor freshness
    from instock.factors import bootstrap as fb
    from instock.factors.registry import get_all as get_all_factors
    from instock.factors.storage import factor_dir
    fb.register_default_factors()
    for name in get_all_factors():
        def _loader(n=name):
            import pandas as pd
            d = factor_dir(n)
            if not d.exists():
                return None
            years = sorted(d.glob("*.parquet"))
            if not years:
                return None
            df = pd.read_parquet(years[-1])
            return None if df.empty else pd.Timestamp(df["date"].max())
        register_check(ArtifactFreshness(
            name=f"factor.{name}",
            loader=_loader,
            max_age_days_yellow=2,
            max_age_days_red=7,
        ))
        register_check(ICIRDecay(factor=name))
    # Metrics freshness
    from instock.backtest.storage import metrics_path
    def _metrics_loader():
        import pandas as pd
        p = metrics_path()
        if not p.exists():
            return None
        df = pd.read_parquet(p)
        if df.empty:
            return None
        return pd.Timestamp(df["end_date"].max())
    register_check(ArtifactFreshness(
        name="backtest.metrics", loader=_metrics_loader,
        max_age_days_yellow=2, max_age_days_red=14,
    ))
    # Data source rate
    register_check(DataSourceRate(source="akshare"))
    _REGISTERED = True
```

Note: this bootstrap references `instock.factors.storage.factor_dir` and `instock.backtest.storage.metrics_path`. If either name isn't exported, add a small helper in the respective storage module. Tests in this task do not exercise bootstrap; Task 8 does.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/monitoring/test_runner.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add instock/monitoring/runner.py instock/monitoring/registry_bootstrap.py \
        tests/monitoring/test_runner.py
git commit -m "feat(sub4): monitoring runner + default-check bootstrap"
```

---

## Task 8: monitoring_check_job (RED-transition detection)

**Files:**
- Create: `instock/job/monitoring_check_job.py`
- Create: `tests/job/test_monitoring_check_job.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/job/test_monitoring_check_job.py
from __future__ import annotations

import pandas as pd
import pytest

from instock.monitoring import runner as rn
from instock.monitoring.status import StatusCheck, StatusRow


class _FixedStatus(StatusCheck):
    def __init__(self, name, status):
        self.name = name
        self._status = status
    def run(self) -> StatusRow:
        return StatusRow(
            name=self.name, status=self._status, message="",
            metric_value=None, as_of=pd.Timestamp.now(),
        )


@pytest.fixture(autouse=True)
def _reg():
    rn.clear_registry()
    yield
    rn.clear_registry()


def test_first_red_fires_webhook(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "http://x")
    rn.register_check(_FixedStatus("c1", "RED"))
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append((url, payload)) or True,
    )
    job.run_once()
    assert len(posted) == 1
    assert posted[0][1]["name"] == "c1"


def test_consecutive_red_does_not_fire(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "http://x")
    rn.register_check(_FixedStatus("c1", "RED"))
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append(1) or True,
    )
    job.run_once()
    job.run_once()
    assert len(posted) == 1


def test_green_then_red_fires(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "http://x")
    chk = _FixedStatus("c1", "GREEN")
    rn.register_check(chk)
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append(1) or True,
    )
    job.run_once()
    chk._status = "RED"
    job.run_once()
    assert len(posted) == 1


def test_empty_url_no_post(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "")
    rn.register_check(_FixedStatus("c1", "RED"))
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append(1) or True,
    )
    job.run_once()
    assert posted == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/job/test_monitoring_check_job.py -v`
Expected: ImportError

- [ ] **Step 3: Implement**

```python
# instock/job/monitoring_check_job.py
"""Cron entry: run all checks, record, fire webhook on RED transitions."""
from __future__ import annotations

import logging
import os
import sys

from instock.monitoring import get_monitoring_root
from instock.monitoring.runner import run_all_checks
from instock.monitoring.state import AlertStateStore
from instock.monitoring.webhook import post_webhook

log = logging.getLogger(__name__)


def run_once() -> int:
    url = os.environ.get("INSTOCK_WEBHOOK_URL", "")
    store = AlertStateStore(get_monitoring_root())
    fired = 0
    for row in run_all_checks():
        prev = store.last_status(row.name)
        store.record(row)
        if row.status == "RED" and (prev is None or prev.status != "RED"):
            if url:
                if post_webhook(url, row.to_dict()):
                    fired += 1
    return fired


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        run_once()
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error("monitoring_check_job failed: %s", exc)
        return 0  # cron must exit 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/job/test_monitoring_check_job.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add instock/job/monitoring_check_job.py tests/job/test_monitoring_check_job.py
git commit -m "feat(sub4): monitoring_check_job with RED-transition webhook"
```

---

## Task 9: Factor handlers + templates

**Files:**
- Create: `instock/web/handlers/factor_handler.py`
- Create: `instock/web/templates/factors_list.html`
- Create: `instock/web/templates/factor_detail.html`
- Create: `tests/web/__init__.py` (if missing)
- Create: `tests/web/test_factor_handler.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/web/test_factor_handler.py
from __future__ import annotations

import tornado.testing
import tornado.web

from instock.web.handlers.factor_handler import (
    FactorDetailHandler,
    FactorListHandler,
)


class _App(tornado.web.Application):
    def __init__(self, template_path):
        super().__init__(
            [
                (r"/factors", FactorListHandler),
                (r"/factors/([^/]+)", FactorDetailHandler),
            ],
            template_path=str(template_path),
        )


class _Base(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        from pathlib import Path
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp)


class TestFactorList(_Base):
    def test_list_200(self):
        resp = self.fetch("/factors")
        assert resp.code == 200
        assert b"factor" in resp.body.lower()


class TestFactorDetail(_Base):
    def test_unknown_404(self):
        resp = self.fetch("/factors/does_not_exist")
        assert resp.code == 404

    def test_known_name_appears(self):
        # register_default_factors() ensures at least mom_5d exists
        from instock.factors import bootstrap
        bootstrap.register_default_factors()
        resp = self.fetch("/factors/mom_5d")
        assert resp.code == 200
        assert b"mom_5d" in resp.body
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/web/test_factor_handler.py -v`
Expected: ImportError

- [ ] **Step 3: Implement handler**

```python
# instock/web/handlers/factor_handler.py
"""List + detail pages for registered factors."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import tornado.web

from instock.factors import bootstrap as fbootstrap
from instock.factors.registry import get_all as get_all_factors
from instock.factors.storage import read_factor

log = logging.getLogger(__name__)


def _ensure_registry() -> None:
    fbootstrap.register_default_factors()


class FactorListHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        _ensure_registry()
        items = []
        for name, f in sorted(get_all_factors().items()):
            items.append({
                "name": name,
                "description": getattr(f, "description", "") or "",
            })
        self.render("factors_list.html", factors=items)


class FactorDetailHandler(tornado.web.RequestHandler):
    def get(self, name: str) -> None:
        _ensure_registry()
        factors = get_all_factors()
        if name not in factors:
            self.set_status(404)
            self.write(f"unknown factor: {name}")
            return
        end = pd.Timestamp.now()
        start = end - pd.Timedelta(days=30)
        try:
            df = read_factor(name, start, end)
            latest = None if df.empty else pd.Timestamp(df["date"].max())
            n_rows = int(len(df))
        except Exception as exc:
            log.warning("factor read failed for %s: %s", name, exc)
            latest = None
            n_rows = 0
        # Locate evaluator report if any
        report_url = None
        candidate = Path("data/factor_reports") / f"{name}.html"
        if candidate.exists():
            report_url = f"/static/factor_reports/{name}.html"
        self.render(
            "factor_detail.html",
            name=name,
            description=getattr(factors[name], "description", "") or "",
            latest=latest,
            n_rows=n_rows,
            report_url=report_url,
        )
```

- [ ] **Step 4: Implement templates**

```html
<!-- instock/web/templates/factors_list.html -->
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Factors</title></head>
<body>
<h1>Registered factors</h1>
{% if not factors %}
  <p>No factors registered.</p>
{% else %}
  <ul>
  {% for f in factors %}
    <li><a href="/factors/{{ f['name'] }}">{{ f['name'] }}</a>
        — {{ f['description'] }}</li>
  {% end %}
  </ul>
{% end %}
</body></html>
```

```html
<!-- instock/web/templates/factor_detail.html -->
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Factor {{ name }}</title></head>
<body>
<p><a href="/factors">&larr; factors</a></p>
<h1>{{ name }}</h1>
<p>{{ description }}</p>
<p>Latest date: {{ latest if latest else "no data yet" }}</p>
<p>Rows in last 30d: {{ n_rows }}</p>
{% if report_url %}
  <iframe src="{{ report_url }}" style="width:100%;height:800px;"
          frameborder="0"></iframe>
{% else %}
  <p><em>No evaluator report on disk.</em></p>
{% end %}
</body></html>
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/web/test_factor_handler.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add instock/web/handlers/factor_handler.py \
        instock/web/templates/factors_list.html \
        instock/web/templates/factor_detail.html \
        tests/web/__init__.py tests/web/test_factor_handler.py
git commit -m "feat(sub4): factor list + detail handlers"
```

---

## Task 10: Strategy handlers + templates (with 7-day diff)

**Files:**
- Create: `instock/web/handlers/strategy_handler.py`
- Create: `instock/web/templates/strategies_list.html`
- Create: `instock/web/templates/strategy_detail.html`
- Create: `tests/web/test_strategy_handler.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/web/test_strategy_handler.py
from __future__ import annotations

from pathlib import Path

import pandas as pd
import tornado.testing
import tornado.web

from instock.web.handlers.strategy_handler import (
    StrategyDetailHandler,
    StrategyListHandler,
)


class _App(tornado.web.Application):
    def __init__(self, template_path):
        super().__init__(
            [
                (r"/strategies", StrategyListHandler),
                (r"/strategies/([^/]+)", StrategyDetailHandler),
            ],
            template_path=str(template_path),
        )


class _Base(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp)


def _seed_holding(root: Path, strategy: str, dates, codes_by_date):
    for d, codes in zip(dates, codes_by_date):
        year = d.year
        path = root / strategy / f"{year}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([
            {"date": d, "code": c, "weight": 1.0 / len(codes),
             "score": 0.0, "strategy": strategy}
            for c in codes
        ])
        if path.exists():
            old = pd.read_parquet(path)
            df = pd.concat([old, df], ignore_index=True)
        df.to_parquet(path, index=False)


class TestStrategy(_Base):
    def setUp(self):
        super().setUp()
        import tempfile, os
        self.tmp = Path(tempfile.mkdtemp())
        os.environ["INSTOCK_HOLDING_ROOT"] = str(self.tmp)
        _seed_holding(
            self.tmp, "demo",
            [pd.Timestamp("2026-04-14"), pd.Timestamp("2026-04-21")],
            [["000001", "000002"], ["000001", "000003"]],
        )

    def test_list_200(self):
        resp = self.fetch("/strategies")
        assert resp.code == 200
        assert b"demo" in resp.body

    def test_detail_has_diff(self):
        resp = self.fetch("/strategies/demo")
        assert resp.code == 200
        # 000003 added, 000002 removed
        assert b"000003" in resp.body
        assert b"000002" in resp.body

    def test_unknown_404(self):
        resp = self.fetch("/strategies/missing")
        assert resp.code == 404
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/web/test_strategy_handler.py -v`
Expected: ImportError

- [ ] **Step 3: Implement handler**

```python
# instock/web/handlers/strategy_handler.py
"""List + detail pages for strategies (reads Sub-2 HoldingSchedule)."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import tornado.web

from instock.portfolio.storage import read_holding

log = logging.getLogger(__name__)


def _strategies_root() -> Path:
    return Path(os.environ.get("INSTOCK_HOLDING_ROOT", "data/holdings"))


def _list_strategies() -> list[str]:
    root = _strategies_root()
    if not root.exists():
        return []
    return sorted(d.name for d in root.iterdir() if d.is_dir())


class StrategyListHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        self.render("strategies_list.html", strategies=_list_strategies())


class StrategyDetailHandler(tornado.web.RequestHandler):
    def get(self, name: str) -> None:
        if name not in _list_strategies():
            self.set_status(404)
            self.write(f"unknown strategy: {name}")
            return
        end = pd.Timestamp.now()
        start = end - pd.Timedelta(days=30)
        try:
            df = read_holding(name, start, end)
        except Exception as exc:
            log.warning("read_holding failed: %s", exc)
            df = pd.DataFrame(
                columns=["date", "code", "weight", "score", "strategy"]
            )
        latest_date = None
        latest_rows: list[dict] = []
        added: list[str] = []
        removed: list[str] = []
        if not df.empty:
            latest_date = df["date"].max()
            latest = df[df["date"] == latest_date]
            latest_rows = latest[
                ["code", "weight", "score"]
            ].sort_values("weight", ascending=False).to_dict("records")
            prev_dates = df[df["date"] < latest_date]["date"]
            if not prev_dates.empty:
                prev_date = prev_dates.max()
                prev = df[df["date"] == prev_date]
                added = sorted(set(latest["code"]) - set(prev["code"]))
                removed = sorted(set(prev["code"]) - set(latest["code"]))
        # Associated backtest run_ids (best-effort)
        run_ids: list[str] = []
        try:
            from instock.backtest.storage import read_metrics
            mdf = read_metrics()
            if not mdf.empty and "strategy" in mdf.columns:
                run_ids = mdf[mdf["strategy"] == name]["run_id"].tolist()
        except Exception:
            run_ids = []
        self.render(
            "strategy_detail.html",
            name=name, latest_date=latest_date,
            latest_rows=latest_rows, added=added, removed=removed,
            run_ids=run_ids,
        )
```

- [ ] **Step 4: Implement templates**

```html
<!-- instock/web/templates/strategies_list.html -->
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Strategies</title></head>
<body>
<h1>Strategies</h1>
{% if not strategies %}
  <p>No strategies found.</p>
{% else %}
  <ul>
    {% for s in strategies %}
      <li><a href="/strategies/{{ s }}">{{ s }}</a></li>
    {% end %}
  </ul>
{% end %}
</body></html>
```

```html
<!-- instock/web/templates/strategy_detail.html -->
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{{ name }}</title></head>
<body>
<p><a href="/strategies">&larr; strategies</a></p>
<h1>{{ name }}</h1>
<p>Latest date: {{ latest_date if latest_date else "no data" }}</p>
<h2>Holdings ({{ len(latest_rows) }})</h2>
<table border="1" cellspacing="0" cellpadding="4">
  <tr><th>code</th><th>weight</th><th>score</th></tr>
  {% for r in latest_rows %}
    <tr><td>{{ r['code'] }}</td>
        <td>{{ "%.4f" % r['weight'] }}</td>
        <td>{{ r['score'] if r['score'] is not None else "" }}</td></tr>
  {% end %}
</table>
<h2>7-day diff</h2>
<p><strong>Added</strong>: {{ ", ".join(added) if added else "—" }}</p>
<p><strong>Removed</strong>: {{ ", ".join(removed) if removed else "—" }}</p>
<h2>Backtest runs</h2>
{% if run_ids %}
  <ul>
    {% for r in run_ids %}
      <li><a href="/backtests/{{ r }}">{{ r }}</a></li>
    {% end %}
  </ul>
{% else %}
  <p>No associated runs.</p>
{% end %}
</body></html>
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/web/test_strategy_handler.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add instock/web/handlers/strategy_handler.py \
        instock/web/templates/strategies_list.html \
        instock/web/templates/strategy_detail.html \
        tests/web/test_strategy_handler.py
git commit -m "feat(sub4): strategy list + detail with 7-day diff"
```

---

## Task 11: Backtest handlers + templates (iframe report)

**Files:**
- Create: `instock/web/handlers/backtest_handler.py`
- Create: `instock/web/templates/backtests_list.html`
- Create: `instock/web/templates/backtest_detail.html`
- Create: `tests/web/test_backtest_handler.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/web/test_backtest_handler.py
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import tornado.testing
import tornado.web

from instock.web.handlers.backtest_handler import (
    BacktestDetailHandler,
    BacktestListHandler,
)


def _seed(root: Path, run_id: str):
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "report.html").write_text("<html>demo report</html>")
    metrics = root / "_metrics.parquet"
    row = pd.DataFrame([{
        "run_id": run_id, "strategy": "demo",
        "start_date": pd.Timestamp("2026-01-01"),
        "end_date": pd.Timestamp("2026-03-31"),
        "sharpe": 1.5, "max_dd": -0.1,
    }])
    if metrics.exists():
        old = pd.read_parquet(metrics)
        row = pd.concat([old, row], ignore_index=True)
    row.to_parquet(metrics, index=False)


class _App(tornado.web.Application):
    def __init__(self, template_path, static_path):
        super().__init__(
            [
                (r"/backtests", BacktestListHandler),
                (r"/backtests/([^/]+)", BacktestDetailHandler),
            ],
            template_path=str(template_path),
            static_path=str(static_path),
        )


class TestBacktest(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = Path(tempfile.mkdtemp())
        os.environ["INSTOCK_BACKTEST_ROOT"] = str(self.tmp)
        _seed(self.tmp, "run-abc")

    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp, self.tmp)

    def test_list_has_run_id(self):
        resp = self.fetch("/backtests")
        assert resp.code == 200
        assert b"run-abc" in resp.body

    def test_detail_has_iframe(self):
        resp = self.fetch("/backtests/run-abc")
        assert resp.code == 200
        assert b"<iframe" in resp.body

    def test_unknown_404(self):
        resp = self.fetch("/backtests/missing")
        assert resp.code == 404
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/web/test_backtest_handler.py -v`
Expected: ImportError

- [ ] **Step 3: Implement handler**

```python
# instock/web/handlers/backtest_handler.py
"""Backtest runs list + per-run detail page (iframe report)."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import tornado.web

log = logging.getLogger(__name__)


def _bt_root() -> Path:
    return Path(os.environ.get("INSTOCK_BACKTEST_ROOT", "data/backtests"))


class BacktestListHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        import pandas as pd
        path = _bt_root() / "_metrics.parquet"
        rows: list[dict] = []
        if path.exists():
            try:
                rows = pd.read_parquet(path).sort_values(
                    "end_date", ascending=False
                ).to_dict("records")
            except Exception as exc:  # noqa: BLE001
                log.warning("metrics read failed: %s", exc)
        self.render("backtests_list.html", rows=rows)


class BacktestDetailHandler(tornado.web.RequestHandler):
    def get(self, run_id: str) -> None:
        run_dir = _bt_root() / "runs" / run_id
        report = run_dir / "report.html"
        if not report.exists():
            self.set_status(404)
            self.write(f"unknown run_id: {run_id}")
            return
        report_url = f"/static/runs/{run_id}/report.html"
        self.render(
            "backtest_detail.html", run_id=run_id, report_url=report_url
        )
```

- [ ] **Step 4: Implement templates**

```html
<!-- instock/web/templates/backtests_list.html -->
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Backtests</title></head>
<body>
<h1>Backtest runs</h1>
{% if not rows %}
  <p>No runs yet.</p>
{% else %}
  <table border="1" cellspacing="0" cellpadding="4">
    <tr><th>run_id</th><th>strategy</th><th>start</th><th>end</th>
        <th>sharpe</th><th>max_dd</th></tr>
    {% for r in rows %}
      <tr>
        <td><a href="/backtests/{{ r['run_id'] }}">{{ r['run_id'] }}</a></td>
        <td>{{ r.get('strategy', '') }}</td>
        <td>{{ r.get('start_date', '') }}</td>
        <td>{{ r.get('end_date', '') }}</td>
        <td>{{ r.get('sharpe', '') }}</td>
        <td>{{ r.get('max_dd', '') }}</td>
      </tr>
    {% end %}
  </table>
{% end %}
</body></html>
```

```html
<!-- instock/web/templates/backtest_detail.html -->
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{{ run_id }}</title></head>
<body>
<p><a href="/backtests">&larr; backtests</a></p>
<h1>{{ run_id }}</h1>
<iframe src="{{ report_url }}" style="width:100%;height:1000px;"
        frameborder="0"></iframe>
</body></html>
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/web/test_backtest_handler.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add instock/web/handlers/backtest_handler.py \
        instock/web/templates/backtests_list.html \
        instock/web/templates/backtest_detail.html \
        tests/web/test_backtest_handler.py
git commit -m "feat(sub4): backtest list + iframe detail"
```

---

## Task 12: Monitoring handler + ACK POST

**Files:**
- Create: `instock/web/handlers/monitoring_handler.py`
- Create: `instock/web/templates/monitoring.html`
- Create: `tests/web/test_monitoring_handler.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/web/test_monitoring_handler.py
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import tornado.testing
import tornado.web

from instock.monitoring import runner as rn
from instock.monitoring.status import StatusCheck, StatusRow
from instock.web.handlers.monitoring_handler import (
    MonitoringAckHandler,
    MonitoringHandler,
)


class _RedCheck(StatusCheck):
    name = "c1"
    def run(self) -> StatusRow:
        return StatusRow(
            name=self.name, status="RED", message="bad",
            metric_value=None, as_of=pd.Timestamp.now(),
        )


class _App(tornado.web.Application):
    def __init__(self, template_path):
        super().__init__(
            [
                (r"/monitoring", MonitoringHandler),
                (r"/monitoring/ack", MonitoringAckHandler),
            ],
            template_path=str(template_path),
        )


class TestMonitoring(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = Path(tempfile.mkdtemp())
        os.environ["INSTOCK_MONITORING_ROOT"] = str(self.tmp)
        rn.clear_registry()
        rn.register_check(_RedCheck())

    def tearDown(self):
        super().tearDown()
        rn.clear_registry()

    def get_app(self):
        tp = Path(__file__).resolve().parents[2] / "instock/web/templates"
        return _App(tp)

    def test_red_renders(self):
        resp = self.fetch("/monitoring")
        assert resp.code == 200
        assert b"c1" in resp.body
        assert b"RED" in resp.body

    def test_ack_round_trip(self):
        # ACK
        resp = self.fetch(
            "/monitoring/ack", method="POST", body="check_name=c1"
        )
        assert resp.code == 302  # redirect
        # page now shows (ack'd)
        resp = self.fetch("/monitoring")
        assert b"ack" in resp.body.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/web/test_monitoring_handler.py -v`
Expected: ImportError

- [ ] **Step 3: Implement handler**

```python
# instock/web/handlers/monitoring_handler.py
"""Monitoring dashboard: current rows + ACK button."""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
import tornado.web

from instock.monitoring import get_monitoring_root
from instock.monitoring.runner import run_all_checks
from instock.monitoring.state import AlertStateStore
from instock.monitoring.status import StatusRow

log = logging.getLogger(__name__)


def _ack_merged(row: StatusRow, store: AlertStateStore) -> dict:
    prev = store.last_status(row.name)
    display_status = row.status
    ack_flag = False
    if prev is not None and prev.status == "ACK" and row.status == "RED":
        display_status = "RED (ack'd)"
        ack_flag = True
    return {
        "name": row.name,
        "status": display_status,
        "raw_status": row.status,
        "message": row.message,
        "metric_value": row.metric_value,
        "as_of": row.as_of,
        "ack": ack_flag,
    }


class MonitoringHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        store = AlertStateStore(get_monitoring_root())
        rows = [_ack_merged(r, store) for r in run_all_checks()]
        self.render("monitoring.html", rows=rows)


class MonitoringAckHandler(tornado.web.RequestHandler):
    def post(self) -> None:
        name = self.get_body_argument("check_name", "")
        if not name:
            self.set_status(400)
            self.write("check_name required")
            return
        store = AlertStateStore(get_monitoring_root())
        store.record(StatusRow(
            name=name, status="ACK", message="acknowledged via UI",
            metric_value=None, as_of=pd.Timestamp.now(),
        ))
        self.redirect("/monitoring")
```

- [ ] **Step 4: Implement template**

```html
<!-- instock/web/templates/monitoring.html -->
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Monitoring</title>
<style>
 .GREEN {background:#dfd} .YELLOW {background:#ffd} .RED {background:#fdd}
 .ACK {background:#ddf}
</style></head>
<body>
<h1>Monitoring</h1>
<table border="1" cellspacing="0" cellpadding="4">
  <tr><th>name</th><th>status</th><th>metric</th><th>message</th>
      <th>as_of</th><th>ack</th></tr>
  {% for r in rows %}
    <tr class="{{ r['raw_status'] }}">
      <td>{{ r['name'] }}</td>
      <td>{{ r['status'] }}</td>
      <td>{{ r['metric_value'] if r['metric_value'] is not None else "" }}</td>
      <td>{{ r['message'] }}</td>
      <td>{{ r['as_of'] }}</td>
      <td>
        {% if r['raw_status'] == 'RED' and not r['ack'] %}
          <form method="post" action="/monitoring/ack" style="margin:0">
            <input type="hidden" name="check_name" value="{{ r['name'] }}">
            <button type="submit">ACK</button>
          </form>
        {% end %}
      </td>
    </tr>
  {% end %}
</table>
</body></html>
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/web/test_monitoring_handler.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
git add instock/web/handlers/monitoring_handler.py \
        instock/web/templates/monitoring.html \
        tests/web/test_monitoring_handler.py
git commit -m "feat(sub4): monitoring dashboard + ACK post"
```

---

## Task 13: Wire new routes into web_service.py

**Files:**
- Modify: `instock/web/web_service.py`

- [ ] **Step 1: Read existing handlers list**

Run: `grep -n 'url(' instock/web/web_service.py | head -20`

- [ ] **Step 2: Add imports and routes**

At the top, add:

```python
from instock.web.handlers.factor_handler import (
    FactorDetailHandler, FactorListHandler,
)
from instock.web.handlers.strategy_handler import (
    StrategyDetailHandler, StrategyListHandler,
)
from instock.web.handlers.backtest_handler import (
    BacktestDetailHandler, BacktestListHandler,
)
from instock.web.handlers.monitoring_handler import (
    MonitoringAckHandler, MonitoringHandler,
)
```

In the handlers list (append before the catch-all, if any):

```python
(r"/factors", FactorListHandler),
(r"/factors/([^/]+)", FactorDetailHandler),
(r"/strategies", StrategyListHandler),
(r"/strategies/([^/]+)", StrategyDetailHandler),
(r"/backtests", BacktestListHandler),
(r"/backtests/([^/]+)", BacktestDetailHandler),
(r"/monitoring", MonitoringHandler),
(r"/monitoring/ack", MonitoringAckHandler),
```

Also confirm the existing `static_path` points at a directory that will serve `data/backtests/runs/<run_id>/report.html` via `/static/runs/...`. If not, add a `StaticFileHandler` mapping:

```python
(r"/static/runs/(.*)", tornado.web.StaticFileHandler,
 {"path": os.environ.get("INSTOCK_BACKTEST_ROOT", "data/backtests") + "/runs"}),
(r"/static/factor_reports/(.*)", tornado.web.StaticFileHandler,
 {"path": "data/factor_reports"}),
```

- [ ] **Step 3: Smoke test**

Run: `python -c "from instock.web.web_service import Application; Application()"`
Expected: no errors.

Run: `pytest tests/web -v`
Expected: all prior web tests pass.

- [ ] **Step 4: Commit**

```bash
git add instock/web/web_service.py
git commit -m "feat(sub4): register new research portal routes"
```

---

## Task 14: Cron snippets

**Files:**
- Create: `cron/daily_factor_compute.sh`
- Create: `cron/daily_generate_holdings.sh`
- Create: `cron/daily_backtest_run.sh`
- Create: `cron/weekly_refdata_refresh.sh`
- Create: `cron/monitoring_check.sh`

- [ ] **Step 1: Write the 5 shell snippets**

```bash
# cron/daily_factor_compute.sh
#!/usr/bin/env bash
# Daily factor computation (run after market close, e.g. 18:00 local)
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.factor_compute_daily_job \
    >> data/log/factor_compute.log 2>&1
```

```bash
# cron/daily_generate_holdings.sh
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.generate_holdings_daily_job \
    >> data/log/generate_holdings.log 2>&1
```

```bash
# cron/daily_backtest_run.sh
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.backtest_run_job --strategy demo \
    --start "$(date -d '90 days ago' +%Y-%m-%d)" \
    --end   "$(date +%Y-%m-%d)" \
    >> data/log/backtest_run.log 2>&1
```

```bash
# cron/weekly_refdata_refresh.sh
#!/usr/bin/env bash
# Run weekly (Sunday 02:00) to refresh industry / listing / st reference data.
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.industry_refresh_job   >> data/log/refdata.log 2>&1
python -m instock.job.listing_refresh_job    >> data/log/refdata.log 2>&1
python -m instock.job.st_refresh_job         >> data/log/refdata.log 2>&1
```

```bash
# cron/monitoring_check.sh
#!/usr/bin/env bash
# */15 * * * * - run the monitoring checks
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.monitoring_check_job \
    >> data/log/monitoring_check.log 2>&1
```

- [ ] **Step 2: Make executable and smoke-check syntax**

```bash
chmod +x cron/*.sh
for f in cron/*.sh; do bash -n "$f"; done
```
Expected: no output (all valid).

- [ ] **Step 3: Commit**

```bash
git add cron/
git commit -m "feat(sub4): cron snippets for daily / weekly / monitoring jobs"
```

---

## Task 15: README env vars + roadmap + followups

**Files:**
- Modify: `README.md` (append env var section)
- Create: `docs/superpowers/followups/subproject-4-research-web.md`
- Modify: `docs/superpowers/roadmap.md`

- [ ] **Step 1: Append to README.md**

Add under existing env-var section (or create one):

```markdown
### Sub-4 (Research Web & Monitoring) env vars

| Variable | Default | Notes |
|---|---|---|
| `INSTOCK_MONITORING_ROOT` | `data/monitoring` | Alert state parquet root |
| `INSTOCK_WEBHOOK_URL`     | *(empty)* | If empty, webhook disabled |
| `INSTOCK_API_CALL_LOG`    | `data/log/api_calls.parquet` | Structured call log |
| `INSTOCK_BACKTEST_ROOT`   | `data/backtests` | Consumed by backtest list handler |
| `INSTOCK_HOLDING_ROOT`    | `data/holdings` | Consumed by strategy handler |
```

- [ ] **Step 2: Create followups doc**

```markdown
<!-- docs/superpowers/followups/subproject-4-research-web.md -->
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
```

- [ ] **Step 3: Update roadmap**

Edit the Sub-project 4 row and section: flip status to `✅ 已合并` once tag is cut. For now (pre-merge) leave as `🎯 进行中`.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/followups/subproject-4-research-web.md \
        docs/superpowers/roadmap.md
git commit -m "docs(sub4): env vars + followups + roadmap progress"
```

---

## Task 16: Final suite + tag

**Files:** none (verification + tagging)

- [ ] **Step 1: Run the full test suite**

Run: `pytest -q`
Expected: all passes (Sub-1/2/2.5/3 previously green: 195 → now 195+new).
No new FutureWarnings.

- [ ] **Step 2: Visually smoke-test the portal**

```bash
python -m instock.web.web_service &
curl -s http://localhost:9988/factors    | head
curl -s http://localhost:9988/strategies | head
curl -s http://localhost:9988/backtests  | head
curl -s http://localhost:9988/monitoring | head
```
Expected: 200 responses, each page includes expected content.

- [ ] **Step 3: Tag**

```bash
git tag subproject-4-research-web-mvp
```

- [ ] **Step 4: Final commit (if roadmap flipped)**

Flip Sub-4 status in `docs/superpowers/roadmap.md` to `✅ 已合并`, then:

```bash
git add docs/superpowers/roadmap.md
git commit -m "docs(sub4): flip Sub-4 status to merged"
```

---

## Self-Review notes

- **Spec coverage:** Sections 1–9 of the spec are each mapped: factor / strategy / backtest / monitoring handlers (Tasks 9-12), webhook (Task 6), cron (Task 14), testing (all tasks include tests), deployment (Task 15 env vars + Task 14 cron). Legacy cutover explicitly deferred (spec §1).
- **Placeholders:** none. Every code block is complete; every test is exact; every commit message stated.
- **Types:** `StatusRow` field names, `StatusCheck.run()` signature, `AlertStateStore.{record,last_status,history}` all consistent between definition (Tasks 2/5) and consumers (Tasks 8/12). `post_webhook(url, payload, timeout=5.0) -> bool` matches Task 6 → Task 8 call site.
- **Known soft spots:** `_default_icir_loader` returns empty (documented in Task 15 followups). Route wiring in Task 13 assumes a `handlers` list exists in `web_service.py` — implementer should confirm first.

---

**Plan complete and saved to `docs/superpowers/plans/2026-04-21-subproject-4-research-web.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
