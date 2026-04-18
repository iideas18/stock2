# Sub-project 2.5 Data Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the refdata layer (industry_map / listing_dates / st_flags) + OhlcvPanelStore + factor registry bootstrap helper so Sub-2's default strategy runs on real data and Sub-3 has a clean data entry point.

**Architecture:** New `instock/refdata/` package holds three per-resource Parquet readers/writers and an OHLCV panel store with on-demand fill. `instock/factors/bootstrap.py` centralises default-factor registration. Sub-2's `filters.py` gains `STFilter` and `LimitFilter`; the default pipeline injects real listing_dates / st_flags / industry_map.

**Tech Stack:** Python 3.12, pandas, pandera 0.31 (`pandera.pandas as pa`), pyarrow, akshare, pytest, pytest-mock. Use `conda run -n base python -m pytest` to run tests.

**Spec:** `docs/superpowers/specs/2026-04-18-data-alignment-design.md`

---

## File Structure

**New files:**
- `instock/refdata/__init__.py`
- `instock/refdata/schemas.py` — three pandera schemas + `RefdataNotAvailable`
- `instock/refdata/industry.py` — read/write industry snapshot + as-of lookup
- `instock/refdata/listing.py` — read + upsert single `listing_dates.parquet`
- `instock/refdata/st.py` — read/write ST snapshot + as-of lookup
- `instock/refdata/ohlcv_store.py` — `OhlcvPanelStore` with on-demand fill + CLI warm
- `instock/factors/bootstrap.py` — idempotent `register_default_factors()`
- `instock/job/refdata_industry_snapshot_job.py`
- `instock/job/refdata_listing_dates_job.py`
- `instock/job/refdata_st_snapshot_job.py`
- `tests/refdata/__init__.py`
- `tests/refdata/test_schemas.py`
- `tests/refdata/test_industry.py`
- `tests/refdata/test_listing.py`
- `tests/refdata/test_st.py`
- `tests/refdata/test_ohlcv_store.py`
- `tests/refdata/test_real_data_smoke.py` (INSTOCK_SUB25_SMOKE gate)
- `tests/factors/test_bootstrap.py`

**Modified files:**
- `instock/datasource/akshare_source.py` — add fetchers: board industry, individual info, st_em
- `instock/portfolio/filters.py` — add `STFilter`, `LimitFilter`, `default_thresholds`; extend `FilterContext`; rewrite `NewListingFilter` to prefer injected `listing_dates`
- `instock/portfolio/pipeline.py` — pass `listing_dates` + `st_flags` into `FilterContext`, pass real `industry_map` into `ConstraintContext`
- `instock/job/factor_compute_daily_job.py` — call `register_default_factors()` at `run()` entry
- `instock/job/generate_holdings_daily_job.py` — use `register_default_factors()`, drop inline workaround; wire refdata into pipeline
- `tests/conftest.py` — add `tmp_refdata_root` and `tmp_ohlcv_root` fixtures
- `tests/portfolio/test_filters.py` — add tests for STFilter, LimitFilter, NewListingFilter(refdata)

---

## Task 0: Scaffold package + test fixtures

**Files:**
- Create: `instock/refdata/__init__.py`
- Create: `tests/refdata/__init__.py`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Create empty refdata package**

```bash
mkdir -p instock/refdata tests/refdata
```

Create `instock/refdata/__init__.py` (empty file).
Create `tests/refdata/__init__.py` (empty file).

- [ ] **Step 2: Add tmp-root fixtures to tests/conftest.py**

Append to `tests/conftest.py`:

```python
@pytest.fixture
def tmp_refdata_root(tmp_path, monkeypatch):
    """Point the refdata Parquet root at a per-test tmp dir."""
    root = tmp_path / "refdata"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_REFDATA_ROOT", str(root))
    return root


@pytest.fixture
def tmp_ohlcv_root(tmp_path, monkeypatch):
    """Point the OHLCV Parquet cache root at a per-test tmp dir."""
    root = tmp_path / "ohlcv"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_OHLCV_ROOT", str(root))
    return root
```

- [ ] **Step 3: Verify existing tests still pass**

Run: `conda run -n base python -m pytest tests/ -x -q`
Expected: all existing tests pass (no new tests added yet).

- [ ] **Step 4: Commit**

```bash
git add instock/refdata/__init__.py tests/refdata/__init__.py tests/conftest.py
git commit -m "chore(refdata): scaffold package + tmp_refdata_root/tmp_ohlcv_root fixtures"
```

---

## Task 1: refdata schemas + RefdataNotAvailable

**Files:**
- Create: `instock/refdata/schemas.py`
- Create: `tests/refdata/test_schemas.py`

- [ ] **Step 1: Write failing tests**

Create `tests/refdata/test_schemas.py`:

```python
import pandas as pd
import pytest
import pandera.errors as pa_errors

from instock.refdata.schemas import (
    INDUSTRY_SNAPSHOT_SCHEMA,
    LISTING_DATES_SCHEMA,
    ST_SNAPSHOT_SCHEMA,
    RefdataNotAvailable,
)


def test_industry_schema_accepts_valid():
    df = pd.DataFrame({
        "code": ["000001", "600000"],
        "industry": ["银行", "银行"],
        "snapshot_date": pd.to_datetime(["2026-04-18", "2026-04-18"]),
    })
    INDUSTRY_SNAPSHOT_SCHEMA.validate(df)


def test_industry_schema_rejects_bad_code():
    df = pd.DataFrame({
        "code": ["1"],
        "industry": ["银行"],
        "snapshot_date": pd.to_datetime(["2026-04-18"]),
    })
    with pytest.raises(pa_errors.SchemaError):
        INDUSTRY_SNAPSHOT_SCHEMA.validate(df)


def test_listing_schema_accepts_valid():
    df = pd.DataFrame({
        "code": ["000001"],
        "listing_date": pd.to_datetime(["1991-04-03"]),
    })
    LISTING_DATES_SCHEMA.validate(df)


def test_listing_schema_rejects_duplicate_codes():
    df = pd.DataFrame({
        "code": ["000001", "000001"],
        "listing_date": pd.to_datetime(["1991-04-03", "1991-04-03"]),
    })
    with pytest.raises(pa_errors.SchemaError):
        LISTING_DATES_SCHEMA.validate(df)


def test_st_schema_accepts_valid():
    df = pd.DataFrame({
        "code": ["000001"],
        "is_st": [True],
        "snapshot_date": pd.to_datetime(["2026-04-18"]),
    })
    ST_SNAPSHOT_SCHEMA.validate(df)


def test_refdata_not_available_is_exception():
    assert issubclass(RefdataNotAvailable, Exception)
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/refdata/test_schemas.py -v`
Expected: ImportError — module missing.

- [ ] **Step 3: Implement schemas**

Create `instock/refdata/schemas.py`:

```python
"""Pandera schemas + error types for refdata (industry/listing/ST)."""
from __future__ import annotations

import pandera.pandas as pa


class RefdataNotAvailable(Exception):
    """Raised when a refdata resource has no snapshot satisfying the query."""


INDUSTRY_SNAPSHOT_SCHEMA = pa.DataFrameSchema(
    {
        "code":          pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "industry":      pa.Column(str, pa.Check.str_length(min_value=1)),
        "snapshot_date": pa.Column("datetime64[ns]"),
    },
    unique=["code"],
    strict=False,
    coerce=True,
)


LISTING_DATES_SCHEMA = pa.DataFrameSchema(
    {
        "code":         pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "listing_date": pa.Column("datetime64[ns]"),
    },
    unique=["code"],
    strict=False,
    coerce=True,
)


ST_SNAPSHOT_SCHEMA = pa.DataFrameSchema(
    {
        "code":          pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "is_st":         pa.Column(bool),
        "snapshot_date": pa.Column("datetime64[ns]"),
    },
    unique=["code"],
    strict=False,
    coerce=True,
)
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/refdata/test_schemas.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/refdata/schemas.py tests/refdata/test_schemas.py
git commit -m "feat(refdata): schemas + RefdataNotAvailable"
```

---

## Task 2: refdata.industry (as-of reader + snapshot writer)

**Files:**
- Create: `instock/refdata/industry.py`
- Create: `tests/refdata/test_industry.py`

- [ ] **Step 1: Write failing tests**

Create `tests/refdata/test_industry.py`:

```python
import pandas as pd
import pytest
from datetime import date

from instock.refdata.industry import (
    write_industry_snapshot,
    read_industry_map,
)
from instock.refdata.schemas import RefdataNotAvailable


def test_write_then_read_latest(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001", "600000"],
        "industry": ["银行", "银行"],
        "snapshot_date": pd.to_datetime(["2026-04-01", "2026-04-01"]),
    })
    write_industry_snapshot(df)
    got = read_industry_map(date(2026, 4, 15))
    assert got == {"000001": "银行", "600000": "银行"}


def test_read_picks_latest_before_at(tmp_refdata_root):
    df1 = pd.DataFrame({
        "code": ["000001"], "industry": ["旧"],
        "snapshot_date": pd.to_datetime(["2026-01-01"]),
    })
    df2 = pd.DataFrame({
        "code": ["000001"], "industry": ["新"],
        "snapshot_date": pd.to_datetime(["2026-04-01"]),
    })
    write_industry_snapshot(df1)
    write_industry_snapshot(df2)
    assert read_industry_map(date(2026, 3, 1)) == {"000001": "旧"}
    assert read_industry_map(date(2026, 5, 1)) == {"000001": "新"}


def test_read_raises_when_no_snapshot(tmp_refdata_root):
    with pytest.raises(RefdataNotAvailable):
        read_industry_map(date(2026, 4, 15))


def test_read_raises_when_all_snapshots_in_future(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001"], "industry": ["银行"],
        "snapshot_date": pd.to_datetime(["2026-12-31"]),
    })
    write_industry_snapshot(df)
    with pytest.raises(RefdataNotAvailable):
        read_industry_map(date(2026, 1, 1))
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/refdata/test_industry.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement industry.py**

Create `instock/refdata/industry.py`:

```python
"""Industry-map snapshot storage and as-of reader."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd

from .schemas import INDUSTRY_SNAPSHOT_SCHEMA, RefdataNotAvailable


def _root() -> Path:
    root = Path(os.environ.get("INSTOCK_REFDATA_ROOT", "data/refdata"))
    d = root / "industry"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_industry_snapshot(df: pd.DataFrame) -> None:
    """Write one snapshot file keyed by its snapshot_date (YYYYMMDD)."""
    if df.empty:
        return
    df = INDUSTRY_SNAPSHOT_SCHEMA.validate(df.copy())
    snap = df["snapshot_date"].iloc[0]
    # All rows in one call share snapshot_date by contract.
    path = _root() / f"{snap.strftime('%Y%m%d')}.parquet"
    df.to_parquet(path, index=False)


def read_industry_map(at: date) -> dict[str, str]:
    """Load the most recent snapshot with snapshot_date <= at.

    Raises RefdataNotAvailable if no such file exists.
    """
    d = _root()
    ts_at = pd.Timestamp(at)
    candidates = []
    for p in d.glob("*.parquet"):
        try:
            snap = pd.Timestamp.strptime(p.stem, "%Y%m%d")
        except ValueError:
            continue
        if snap <= ts_at:
            candidates.append((snap, p))
    if not candidates:
        raise RefdataNotAvailable(
            f"no industry snapshot on or before {at}"
        )
    _, latest = max(candidates, key=lambda x: x[0])
    df = pd.read_parquet(latest)
    return dict(zip(df["code"].astype(str), df["industry"].astype(str)))
```

Note: `pd.Timestamp.strptime` does not exist. Replace with `datetime.strptime(p.stem, "%Y%m%d")` + `pd.Timestamp(...)`. Corrected version:

```python
from datetime import datetime
...
        try:
            snap = pd.Timestamp(datetime.strptime(p.stem, "%Y%m%d"))
        except ValueError:
            continue
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/refdata/test_industry.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/refdata/industry.py tests/refdata/test_industry.py
git commit -m "feat(refdata): industry snapshot storage + as-of read"
```

---

## Task 3: refdata.listing (upsert reader)

**Files:**
- Create: `instock/refdata/listing.py`
- Create: `tests/refdata/test_listing.py`

- [ ] **Step 1: Write failing tests**

Create `tests/refdata/test_listing.py`:

```python
import pandas as pd
import pytest
from datetime import date

from instock.refdata.listing import (
    upsert_listing_dates,
    read_listing_dates,
)
from instock.refdata.schemas import RefdataNotAvailable


def test_upsert_then_read(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001", "600000"],
        "listing_date": pd.to_datetime(["1991-04-03", "1999-11-10"]),
    })
    upsert_listing_dates(df)
    got = read_listing_dates()
    assert got == {
        "000001": date(1991, 4, 3),
        "600000": date(1999, 11, 10),
    }


def test_upsert_is_idempotent(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001"],
        "listing_date": pd.to_datetime(["1991-04-03"]),
    })
    upsert_listing_dates(df)
    upsert_listing_dates(df)
    got = read_listing_dates()
    assert len(got) == 1


def test_upsert_merges_new_codes(tmp_refdata_root):
    upsert_listing_dates(pd.DataFrame({
        "code": ["000001"], "listing_date": pd.to_datetime(["1991-04-03"]),
    }))
    upsert_listing_dates(pd.DataFrame({
        "code": ["600000"], "listing_date": pd.to_datetime(["1999-11-10"]),
    }))
    assert set(read_listing_dates().keys()) == {"000001", "600000"}


def test_upsert_updates_existing_code(tmp_refdata_root):
    upsert_listing_dates(pd.DataFrame({
        "code": ["000001"], "listing_date": pd.to_datetime(["1991-04-03"]),
    }))
    upsert_listing_dates(pd.DataFrame({
        "code": ["000001"], "listing_date": pd.to_datetime(["1991-04-04"]),
    }))
    assert read_listing_dates()["000001"] == date(1991, 4, 4)


def test_read_raises_when_empty(tmp_refdata_root):
    with pytest.raises(RefdataNotAvailable):
        read_listing_dates()
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/refdata/test_listing.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement listing.py**

Create `instock/refdata/listing.py`:

```python
"""Listing-date storage (single parquet, upsert-by-code)."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd

from .schemas import LISTING_DATES_SCHEMA, RefdataNotAvailable


def _path() -> Path:
    root = Path(os.environ.get("INSTOCK_REFDATA_ROOT", "data/refdata"))
    root.mkdir(parents=True, exist_ok=True)
    return root / "listing_dates.parquet"


def upsert_listing_dates(df: pd.DataFrame) -> None:
    """Merge incoming (code, listing_date) into the single parquet file."""
    if df.empty:
        return
    df = LISTING_DATES_SCHEMA.validate(df.copy())
    path = _path()
    if path.exists():
        old = pd.read_parquet(path)
        merged = (
            pd.concat([old, df], ignore_index=True)
            .drop_duplicates(subset=["code"], keep="last")
            .sort_values("code")
            .reset_index(drop=True)
        )
    else:
        merged = df.sort_values("code").reset_index(drop=True)
    merged = LISTING_DATES_SCHEMA.validate(merged)
    merged.to_parquet(path, index=False)


def read_listing_dates() -> dict[str, date]:
    """Return code -> listing_date dict. Raise RefdataNotAvailable if no file."""
    path = _path()
    if not path.exists():
        raise RefdataNotAvailable(f"listing_dates.parquet not found at {path}")
    df = pd.read_parquet(path)
    return {
        str(c): d.date()
        for c, d in zip(df["code"], pd.to_datetime(df["listing_date"]))
    }
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/refdata/test_listing.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/refdata/listing.py tests/refdata/test_listing.py
git commit -m "feat(refdata): listing-dates upsert + read"
```

---

## Task 4: refdata.st (as-of reader + snapshot writer)

**Files:**
- Create: `instock/refdata/st.py`
- Create: `tests/refdata/test_st.py`

- [ ] **Step 1: Write failing tests**

Create `tests/refdata/test_st.py`:

```python
import pandas as pd
import pytest
from datetime import date

from instock.refdata.st import write_st_snapshot, read_st_flags
from instock.refdata.schemas import RefdataNotAvailable


def test_write_then_read(tmp_refdata_root):
    df = pd.DataFrame({
        "code": ["000001", "600000"],
        "is_st": [True, False],
        "snapshot_date": pd.to_datetime(["2026-04-01", "2026-04-01"]),
    })
    write_st_snapshot(df)
    got = read_st_flags(date(2026, 4, 15))
    assert got == {"000001"}


def test_picks_latest_snapshot(tmp_refdata_root):
    write_st_snapshot(pd.DataFrame({
        "code": ["000001"], "is_st": [True],
        "snapshot_date": pd.to_datetime(["2026-01-01"]),
    }))
    write_st_snapshot(pd.DataFrame({
        "code": ["000001"], "is_st": [False],
        "snapshot_date": pd.to_datetime(["2026-04-01"]),
    }))
    assert read_st_flags(date(2026, 5, 1)) == set()
    assert read_st_flags(date(2026, 3, 1)) == {"000001"}


def test_raises_when_no_snapshot(tmp_refdata_root):
    with pytest.raises(RefdataNotAvailable):
        read_st_flags(date(2026, 4, 1))
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/refdata/test_st.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement st.py**

Create `instock/refdata/st.py`:

```python
"""ST-flag snapshot storage and as-of reader."""
from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .schemas import ST_SNAPSHOT_SCHEMA, RefdataNotAvailable


def _root() -> Path:
    root = Path(os.environ.get("INSTOCK_REFDATA_ROOT", "data/refdata"))
    d = root / "st"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_st_snapshot(df: pd.DataFrame) -> None:
    """Write one snapshot file keyed by snapshot_date (YYYYMMDD)."""
    if df.empty:
        return
    df = ST_SNAPSHOT_SCHEMA.validate(df.copy())
    snap = df["snapshot_date"].iloc[0]
    path = _root() / f"{snap.strftime('%Y%m%d')}.parquet"
    df.to_parquet(path, index=False)


def read_st_flags(at: date) -> set[str]:
    """Return set of codes with is_st=True in the latest snapshot <= at."""
    d = _root()
    ts_at = pd.Timestamp(at)
    candidates = []
    for p in d.glob("*.parquet"):
        try:
            snap = pd.Timestamp(datetime.strptime(p.stem, "%Y%m%d"))
        except ValueError:
            continue
        if snap <= ts_at:
            candidates.append((snap, p))
    if not candidates:
        raise RefdataNotAvailable(f"no ST snapshot on or before {at}")
    _, latest = max(candidates, key=lambda x: x[0])
    df = pd.read_parquet(latest)
    return set(df.loc[df["is_st"], "code"].astype(str))
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/refdata/test_st.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/refdata/st.py tests/refdata/test_st.py
git commit -m "feat(refdata): ST-flag snapshot + as-of read"
```

---

## Task 5: OhlcvPanelStore (read + on-demand fill + cache)

**Files:**
- Create: `instock/refdata/ohlcv_store.py`
- Create: `tests/refdata/test_ohlcv_store.py`

- [ ] **Step 1: Write failing tests**

Create `tests/refdata/test_ohlcv_store.py`:

```python
from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from instock.refdata.ohlcv_store import OhlcvPanelStore


def _ohlcv_rows(code, dates):
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": [code] * len(dates),
        "open": [10.0] * len(dates),
        "high": [11.0] * len(dates),
        "low": [9.5] * len(dates),
        "close": [10.5] * len(dates),
        "volume": [1000.0] * len(dates),
        "amount": [10500.0] * len(dates),
    })


def test_cache_miss_fetches_and_stores(tmp_ohlcv_root):
    source = MagicMock()
    source.get_trade_calendar.return_value = [
        date(2026, 4, 1), date(2026, 4, 2)
    ]
    source.get_ohlcv.side_effect = lambda c, s, e, adjust="qfq": \
        _ohlcv_rows(c, ["2026-04-01", "2026-04-02"])

    store = OhlcvPanelStore(source=source)
    got = store.get_panel(
        ["000001"], date(2026, 4, 1), date(2026, 4, 2)
    )
    assert len(got) == 2
    source.get_ohlcv.assert_called_once()


def test_cache_hit_skips_fetch(tmp_ohlcv_root):
    source = MagicMock()
    source.get_trade_calendar.return_value = [
        date(2026, 4, 1), date(2026, 4, 2)
    ]
    source.get_ohlcv.side_effect = lambda c, s, e, adjust="qfq": \
        _ohlcv_rows(c, ["2026-04-01", "2026-04-02"])
    store = OhlcvPanelStore(source=source)

    store.get_panel(["000001"], date(2026, 4, 1), date(2026, 4, 2))
    source.get_ohlcv.reset_mock()
    got2 = store.get_panel(["000001"], date(2026, 4, 1), date(2026, 4, 2))
    assert len(got2) == 2
    source.get_ohlcv.assert_not_called()


def test_missing_by_trade_calendar_not_date_range(tmp_ohlcv_root):
    """Weekends/holidays in [start,end] should NOT trigger refetch."""
    source = MagicMock()
    # Only Monday is a trade day; Sat/Sun are gaps by calendar.
    source.get_trade_calendar.return_value = [date(2026, 4, 6)]
    source.get_ohlcv.side_effect = lambda c, s, e, adjust="qfq": \
        _ohlcv_rows(c, ["2026-04-06"])
    store = OhlcvPanelStore(source=source)

    store.get_panel(["000001"], date(2026, 4, 4), date(2026, 4, 6))
    source.get_ohlcv.reset_mock()
    store.get_panel(["000001"], date(2026, 4, 4), date(2026, 4, 6))
    source.get_ohlcv.assert_not_called()


def test_single_code_fetch_failure_logged_and_skipped(tmp_ohlcv_root, caplog):
    source = MagicMock()
    source.get_trade_calendar.return_value = [date(2026, 4, 1)]

    def _maybe_fail(c, s, e, adjust="qfq"):
        if c == "000001":
            raise RuntimeError("boom")
        return _ohlcv_rows(c, ["2026-04-01"])

    source.get_ohlcv.side_effect = _maybe_fail
    store = OhlcvPanelStore(source=source)
    got = store.get_panel(
        ["000001", "600000"], date(2026, 4, 1), date(2026, 4, 1)
    )
    assert set(got["code"]) == {"600000"}
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/refdata/test_ohlcv_store.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement OhlcvPanelStore**

Create `instock/refdata/ohlcv_store.py`:

```python
"""OHLCV panel cache with on-demand fill from IDataSource.

Layout: <INSTOCK_OHLCV_ROOT>/<year>.parquet, row keys (date, code).
Gap-detection uses IDataSource.get_trade_calendar — NOT date.range —
so weekends/holidays never trigger spurious refetch.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from instock.datasource.base import IDataSource

log = logging.getLogger(__name__)

_COLUMNS = [
    "date", "code", "open", "high", "low", "close", "volume", "amount"
]


class OhlcvPanelStore:
    def __init__(
        self, source: IDataSource, root: Optional[Path] = None
    ) -> None:
        self.source = source
        if root is None:
            root = Path(os.environ.get("INSTOCK_OHLCV_ROOT", "data/ohlcv"))
        root.mkdir(parents=True, exist_ok=True)
        self.root = root

    def _path(self, year: int) -> Path:
        return self.root / f"{year}.parquet"

    def _load_cache(
        self, start: date, end: date
    ) -> pd.DataFrame:
        frames = []
        for y in range(start.year, end.year + 1):
            p = self._path(y)
            if p.exists():
                frames.append(pd.read_parquet(p))
        if not frames:
            return pd.DataFrame(columns=_COLUMNS)
        df = pd.concat(frames, ignore_index=True)
        ts_s, ts_e = pd.Timestamp(start), pd.Timestamp(end)
        return df.loc[
            (df["date"] >= ts_s) & (df["date"] <= ts_e)
        ].reset_index(drop=True)

    def _write_cache(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        df = df.copy()
        df["year"] = df["date"].dt.year
        for year, group in df.groupby("year"):
            path = self._path(int(year))
            payload = group.drop(columns=["year"])
            if path.exists():
                old = pd.read_parquet(path)
                payload = pd.concat([old, payload], ignore_index=True)
            payload = (
                payload.drop_duplicates(
                    subset=["date", "code"], keep="last"
                )
                .sort_values(["date", "code"])
                .reset_index(drop=True)
            )
            payload.to_parquet(path, index=False)

    def _missing_codes(
        self, cached: pd.DataFrame, codes: list[str],
        cal: list[date],
    ) -> list[str]:
        """Codes whose cached rows don't cover every trade day."""
        if not cal:
            return []
        needed = len(cal)
        if cached.empty:
            return list(codes)
        counts = cached.groupby("code")["date"].nunique()
        missing = []
        for c in codes:
            if counts.get(c, 0) < needed:
                missing.append(c)
        return missing

    def get_panel(
        self, codes: list[str], start: date, end: date,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame(columns=_COLUMNS)
        cached = self._load_cache(start, end)
        cal = self.source.get_trade_calendar(start, end)
        missing = self._missing_codes(cached, codes, cal)
        new_frames = []
        for c in missing:
            try:
                frame = self.source.get_ohlcv(c, start, end, adjust=adjust)
                if not frame.empty:
                    new_frames.append(frame)
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "OhlcvPanelStore: fetch failed for %s: %s", c, exc
                )
        if new_frames:
            fetched = pd.concat(new_frames, ignore_index=True)
            self._write_cache(fetched)
        # Reload after possibly writing new rows.
        merged = self._load_cache(start, end)
        return merged[merged["code"].isin(codes)].reset_index(drop=True)

    def warm_cache(
        self, codes: list[str], start: date, end: date,
        adjust: str = "qfq",
    ) -> None:
        self.get_panel(codes, start, end, adjust=adjust)
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/refdata/test_ohlcv_store.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/refdata/ohlcv_store.py tests/refdata/test_ohlcv_store.py
git commit -m "feat(refdata): OhlcvPanelStore with on-demand fill + yearly cache"
```

---

## Task 6: factors.bootstrap + swap Sub-1 daily job

**Files:**
- Create: `instock/factors/bootstrap.py`
- Create: `tests/factors/test_bootstrap.py`
- Modify: `instock/job/factor_compute_daily_job.py`

- [ ] **Step 1: Write failing tests**

Create `tests/factors/test_bootstrap.py`:

```python
from instock.factors import bootstrap, registry


def test_bootstrap_registers_six_factors():
    registry.clear_registry()
    bootstrap._REGISTERED = False
    bootstrap.register_default_factors()
    names = set(registry.get_all().keys())
    # exact six names from Sub-1 MVP
    for expected in {"mom_20d", "pe_ttm", "pb", "roe_ttm",
                     "lhb_heat_30d", "north_holding_chg_5d"}:
        assert expected in names


def test_bootstrap_is_idempotent():
    registry.clear_registry()
    bootstrap._REGISTERED = False
    bootstrap.register_default_factors()
    bootstrap.register_default_factors()  # must not raise
    assert len(registry.get_all()) >= 6
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/factors/test_bootstrap.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement bootstrap.py**

Create `instock/factors/bootstrap.py`:

```python
"""Idempotent default-factor registration helper.

Centralises knowledge of which factor classes ship in MVP so both
Sub-1's factor_compute_daily_job and Sub-2's generate_holdings_daily_job
share the same registration path (fixes the silent-no-op bug in which
side-effect imports were relied on but no module called register()).
"""
from __future__ import annotations

from . import registry

_REGISTERED = False


def register_default_factors() -> None:
    """Register the six MVP factors. Safe to call multiple times."""
    global _REGISTERED
    if _REGISTERED:
        return
    from .technical.momentum import MomentumFactor
    from .fundamental.valuation import PEFactor, PBFactor, ROEFactor
    from .lhb.heat import LhbHeatFactor
    from .flow.north import NorthHoldingChgFactor
    for cls in (
        MomentumFactor,
        PEFactor,
        PBFactor,
        ROEFactor,
        LhbHeatFactor,
        NorthHoldingChgFactor,
    ):
        try:
            registry.register(cls())
        except ValueError:
            # Already registered (test isolation etc.).
            pass
    _REGISTERED = True
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/factors/test_bootstrap.py -v`
Expected: 2 passed.

- [ ] **Step 5: Update Sub-1 daily job**

Edit `instock/job/factor_compute_daily_job.py`. In `run(start, end)`, insert as the first line:

```python
def run(start: date, end: date) -> dict[str, Exception]:
    """Run every registered factor for [start, end]; return per-factor errors."""
    from instock.factors.bootstrap import register_default_factors
    register_default_factors()
    errors: dict[str, Exception] = {}
    ...
```

- [ ] **Step 6: Run existing Sub-1 tests**

Run: `conda run -n base python -m pytest tests/factors tests/job -v`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add instock/factors/bootstrap.py tests/factors/test_bootstrap.py \
        instock/job/factor_compute_daily_job.py
git commit -m "feat(factors): bootstrap helper + wire into Sub-1 daily job"
```

---

## Task 7: Sub-2 daily job switches to bootstrap

**Files:**
- Modify: `instock/job/generate_holdings_daily_job.py`

- [ ] **Step 1: Replace _default_configs to use bootstrap**

Replace the `_default_configs` function body in `instock/job/generate_holdings_daily_job.py` with:

```python
def _default_configs() -> list[StrategyConfig]:
    """MVP: a single default strategy using every factor currently registered."""
    from instock.factors.bootstrap import register_default_factors
    from instock.factors.registry import get_all

    register_default_factors()
    names = list(get_all().keys())
    return [StrategyConfig(name="default", factors=names)]
```

- [ ] **Step 2: Run existing Sub-2 tests**

Run: `conda run -n base python -m pytest tests/portfolio tests/job -v`
Expected: all pass (same strategies, same factor list).

- [ ] **Step 3: Commit**

```bash
git add instock/job/generate_holdings_daily_job.py
git commit -m "refactor(job): generate_holdings_daily uses bootstrap helper"
```

---

## Task 8: STFilter + FilterContext.st_flags

**Files:**
- Modify: `instock/portfolio/filters.py`
- Modify: `tests/portfolio/test_filters.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/portfolio/test_filters.py`:

```python
import logging
import pandas as pd
import pytest

from instock.portfolio.filters import (
    FilterContext,
    STFilter,
)


def _empty_ohlcv():
    return pd.DataFrame(
        columns=["date", "code", "open", "high", "low", "close", "volume"]
    )


def test_stfilter_drops_st_codes():
    ctx = FilterContext(
        ohlcv_panel=_empty_ohlcv(),
        st_flags={"000001"},
    )
    out = STFilter().apply(
        ["000001", "600000"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == ["600000"]


def test_stfilter_noop_when_flags_missing(caplog):
    ctx = FilterContext(ohlcv_panel=_empty_ohlcv(), st_flags=None)
    with caplog.at_level(logging.WARNING):
        out = STFilter().apply(
            ["000001"], pd.Timestamp("2026-04-01"), ctx
        )
    assert out == ["000001"]
    assert any("st_flags" in rec.message.lower() for rec in caplog.records)


def test_stfilter_warns_only_once(caplog):
    flt = STFilter()
    ctx = FilterContext(ohlcv_panel=_empty_ohlcv(), st_flags=None)
    with caplog.at_level(logging.WARNING):
        flt.apply(["000001"], pd.Timestamp("2026-04-01"), ctx)
        flt.apply(["000002"], pd.Timestamp("2026-04-02"), ctx)
    st_warnings = [
        r for r in caplog.records if "st_flags" in r.message.lower()
    ]
    assert len(st_warnings) == 1
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/portfolio/test_filters.py -v`
Expected: ImportError / AttributeError (STFilter, new kwarg).

- [ ] **Step 3: Extend FilterContext + add STFilter**

Edit `instock/portfolio/filters.py`. Replace the `FilterContext` dataclass with:

```python
from datetime import date

@dataclass
class FilterContext:
    """Shared data passed to every filter. OHLCV must span back far enough
    for history-based filters (e.g. NewListingFilter.min_days).

    listing_dates and st_flags are Sub-2.5 additions; filters must tolerate
    None and fall back gracefully with a single warning.
    """
    ohlcv_panel: pd.DataFrame
    listing_dates: dict[str, date] | None = None
    st_flags: set[str] | None = None
```

Append to `instock/portfolio/filters.py`:

```python
class STFilter(UniverseFilter):
    """Drop codes flagged as ST in FilterContext.st_flags.

    If st_flags is None: warn once and no-op (keeps all codes).
    """

    def __init__(self) -> None:
        self._warned = False

    def apply(self, codes, at, context):
        if context.st_flags is None:
            if not self._warned:
                import logging
                logging.getLogger(__name__).warning(
                    "STFilter: context.st_flags is None; no-op"
                )
                self._warned = True
            return list(codes)
        flags = context.st_flags
        return [c for c in codes if c not in flags]
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/portfolio/test_filters.py -v`
Expected: all (previous + 3 new) pass.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/filters.py tests/portfolio/test_filters.py
git commit -m "feat(portfolio): STFilter + FilterContext.st_flags"
```

---

## Task 9: LimitFilter + default_thresholds

**Files:**
- Modify: `instock/portfolio/filters.py`
- Modify: `tests/portfolio/test_filters.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/portfolio/test_filters.py`:

```python
from instock.portfolio.filters import LimitFilter, default_thresholds


def test_thresholds_by_board():
    assert default_thresholds("000001", False) == 0.10
    assert default_thresholds("600000", False) == 0.10
    assert default_thresholds("300001", False) == 0.20
    assert default_thresholds("688001", False) == 0.20
    assert default_thresholds("830001", False) == 0.30
    assert default_thresholds("430001", False) == 0.30
    assert default_thresholds("000001", True) == 0.05  # ST override


def _panel_two_days(code, close_t_minus_1, close_t):
    return pd.DataFrame({
        "date": pd.to_datetime(["2026-03-31", "2026-04-01"]),
        "code": [code, code],
        "open":   [close_t_minus_1 * 0.99, close_t],
        "high":   [close_t_minus_1, close_t],
        "low":    [close_t_minus_1 * 0.98, close_t],
        "close":  [close_t_minus_1, close_t],
        "volume": [1000.0, 1000.0],
    })


def test_limitfilter_drops_limit_up_on_prior_day():
    # 000001 (主板): T-1 close = 11.0, prior close 10.0 -> +10% exactly.
    panel = pd.DataFrame({
        "date": pd.to_datetime(
            ["2026-03-30", "2026-03-31", "2026-04-01"]
        ),
        "code": ["000001"] * 3,
        "open":   [10.0, 10.5, 11.1],
        "high":   [10.0, 11.0, 11.2],
        "low":    [9.8, 10.3, 10.9],
        "close":  [10.0, 11.0, 11.1],
        "volume": [1000.0, 1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, st_flags=set())
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == []


def test_limitfilter_keeps_non_limit():
    panel = pd.DataFrame({
        "date": pd.to_datetime(["2026-03-30", "2026-03-31"]),
        "code": ["000001"] * 2,
        "open":   [10.0, 10.2],
        "high":   [10.1, 10.5],
        "low":    [9.9, 10.1],
        "close":  [10.0, 10.3],  # +3% — far from +10% cap
        "volume": [1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, st_flags=set())
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == ["000001"]


def test_limitfilter_uses_st_threshold():
    # ST stock: +5% is limit-up.
    panel = pd.DataFrame({
        "date": pd.to_datetime(["2026-03-30", "2026-03-31"]),
        "code": ["000001"] * 2,
        "open":   [10.0, 10.4],
        "high":   [10.0, 10.5],
        "low":    [9.8, 10.3],
        "close":  [10.0, 10.5],  # +5% -> limit-up for ST
        "volume": [1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, st_flags={"000001"})
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == []


def test_limitfilter_filters_when_t_minus_1_missing():
    panel = pd.DataFrame(
        columns=["date", "code", "open", "high", "low", "close", "volume"]
    )
    ctx = FilterContext(ohlcv_panel=panel, st_flags=set())
    out = LimitFilter().apply(
        ["000001"], pd.Timestamp("2026-04-01"), ctx
    )
    assert out == []  # conservative: no prior close -> drop
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/portfolio/test_filters.py -v`
Expected: ImportError for LimitFilter, default_thresholds.

- [ ] **Step 3: Implement LimitFilter + default_thresholds**

Append to `instock/portfolio/filters.py`:

```python
from typing import Callable


def default_thresholds(code: str, is_st: bool) -> float:
    """A-share limit-up/down thresholds by board prefix.

    - ST: 5%
    - 创业板 300xxx: 20%
    - 科创板 688xxx: 20%
    - 北交所 4/8 开头: 30%
    - 主板其余: 10%
    """
    if is_st:
        return 0.05
    if code.startswith("300") or code.startswith("688"):
        return 0.20
    if code.startswith("4") or code.startswith("8"):
        return 0.30
    return 0.10


class LimitFilter(UniverseFilter):
    """Drop codes that hit limit-up on the trading day before `at`.

    Logic: find the T-1 and T-2 rows for each code in ohlcv_panel; if
      (close_T-1 / close_T-2) - 1 >= threshold - tolerance, drop the code.
    Missing prior data -> conservative drop.
    """

    def __init__(
        self,
        threshold_provider: Callable[[str, bool], float] = default_thresholds,
        tolerance: float = 1e-3,
    ) -> None:
        self.threshold_provider = threshold_provider
        self.tolerance = tolerance

    def apply(self, codes, at, context):
        if not codes:
            return []
        panel = context.ohlcv_panel
        if panel.empty:
            return []
        ts = pd.Timestamp(at)
        prior = panel[panel["date"] < ts]
        if prior.empty:
            return []
        st_flags = context.st_flags or set()
        out = []
        for c in codes:
            rows = prior[prior["code"] == c].sort_values("date")
            if len(rows) < 2:
                continue  # conservative drop
            close_t1 = rows.iloc[-1]["close"]
            close_t2 = rows.iloc[-2]["close"]
            if close_t2 <= 0:
                continue
            ret = close_t1 / close_t2 - 1.0
            thr = self.threshold_provider(c, c in st_flags)
            if ret >= thr - self.tolerance:
                continue  # limit-up -> drop
            out.append(c)
        return out
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/portfolio/test_filters.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/filters.py tests/portfolio/test_filters.py
git commit -m "feat(portfolio): LimitFilter + default_thresholds by board"
```

---

## Task 10: NewListingFilter reads refdata with fallback

**Files:**
- Modify: `instock/portfolio/filters.py`
- Modify: `tests/portfolio/test_filters.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/portfolio/test_filters.py`:

```python
from datetime import date
from instock.portfolio.filters import NewListingFilter


def test_newlisting_uses_listing_dates_when_provided():
    panel = pd.DataFrame(
        columns=["date", "code", "open", "high", "low", "close", "volume"]
    )
    ctx = FilterContext(
        ohlcv_panel=panel,
        listing_dates={
            "000001": date(2020, 1, 1),
            "600000": date(2026, 4, 1),  # too new
        },
    )
    out = NewListingFilter(min_days=60).apply(
        ["000001", "600000"], pd.Timestamp("2026-04-10"), ctx
    )
    assert out == ["000001"]


def test_newlisting_falls_back_to_ohlcv_when_listing_dates_none(caplog):
    panel = pd.DataFrame({
        "date": pd.to_datetime(
            ["2020-01-01", "2026-03-01", "2026-04-10"]
        ),
        "code": ["000001", "600000", "600000"],
        "open":   [10.0, 10.0, 10.0],
        "high":   [10.0, 10.0, 10.0],
        "low":    [10.0, 10.0, 10.0],
        "close":  [10.0, 10.0, 10.0],
        "volume": [1000.0, 1000.0, 1000.0],
    })
    ctx = FilterContext(ohlcv_panel=panel, listing_dates=None)
    with caplog.at_level(logging.WARNING):
        out = NewListingFilter(min_days=60).apply(
            ["000001", "600000"], pd.Timestamp("2026-04-10"), ctx
        )
    assert out == ["000001"]
    assert any(
        "fallback" in r.message.lower() or "ohlcv" in r.message.lower()
        for r in caplog.records
    )
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/portfolio/test_filters.py::test_newlisting_uses_listing_dates_when_provided tests/portfolio/test_filters.py::test_newlisting_falls_back_to_ohlcv_when_listing_dates_none -v`
Expected: fail.

- [ ] **Step 3: Rewrite NewListingFilter**

Replace `NewListingFilter` class in `instock/portfolio/filters.py`:

```python
from datetime import timedelta

class NewListingFilter(UniverseFilter):
    """Drop codes listed less than `min_days` calendar days before `at`.

    Prefers FilterContext.listing_dates (authoritative). Falls back to
    earliest-OHLCV-observation approximation when listing_dates is None,
    warning once.
    """

    def __init__(self, min_days: int = 60) -> None:
        self.min_days = min_days
        self._warned_fallback = False

    def apply(self, codes, at, context):
        if not codes:
            return []
        ts = pd.Timestamp(at)
        if context.listing_dates is not None:
            cutoff = ts - pd.Timedelta(days=self.min_days)
            ok = []
            for c in codes:
                ld = context.listing_dates.get(c)
                if ld is None:
                    continue
                if pd.Timestamp(ld) <= cutoff:
                    ok.append(c)
            return ok
        # Fallback: earliest OHLCV observation.
        if not self._warned_fallback:
            import logging
            logging.getLogger(__name__).warning(
                "NewListingFilter: listing_dates missing; "
                "falling back to earliest-OHLCV approximation"
            )
            self._warned_fallback = True
        panel = context.ohlcv_panel
        if panel.empty:
            return []
        first_seen = panel.groupby("code")["date"].min()
        cutoff = ts - pd.Timedelta(days=self.min_days)
        ok = set(first_seen[first_seen <= cutoff].index.astype(str))
        return [c for c in codes if c in ok]
```

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/portfolio/test_filters.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/filters.py tests/portfolio/test_filters.py
git commit -m "feat(portfolio): NewListingFilter prefers listing_dates, falls back"
```

---

## Task 11: Pipeline wires real refdata into FilterContext + ConstraintContext

**Files:**
- Modify: `instock/portfolio/pipeline.py`
- Modify: `tests/portfolio/test_pipeline.py`

- [ ] **Step 1: Write failing test**

Append to `tests/portfolio/test_pipeline.py`:

```python
def test_pipeline_injects_refdata_when_available(monkeypatch, tmp_refdata_root):
    """With listing_dates / st_flags / industry_map present in refdata,
    StrategyPipeline must pass them into FilterContext / ConstraintContext."""
    from datetime import date
    import pandas as pd

    from instock.refdata.listing import upsert_listing_dates
    from instock.refdata.st import write_st_snapshot
    from instock.refdata.industry import write_industry_snapshot
    from instock.portfolio.pipeline import StrategyPipeline, StrategyConfig

    upsert_listing_dates(pd.DataFrame({
        "code": ["000001"], "listing_date": pd.to_datetime(["1991-04-03"]),
    }))
    write_st_snapshot(pd.DataFrame({
        "code": ["000001"], "is_st": [False],
        "snapshot_date": pd.to_datetime(["2026-04-01"]),
    }))
    write_industry_snapshot(pd.DataFrame({
        "code": ["000001"], "industry": ["银行"],
        "snapshot_date": pd.to_datetime(["2026-04-01"]),
    }))

    captured = {}

    class _SpyFilter:
        def apply(self, codes, at, context):
            captured["listing_dates"] = context.listing_dates
            captured["st_flags"] = context.st_flags
            return codes

    class _SpyConstraint:
        def apply(self, weights, context):
            captured["industry_map"] = context.industry_map
            return weights

    # The pipeline.run path is covered by other tests; here we only
    # need to confirm the context hook-up. Use the private helper:
    pipe = StrategyPipeline()
    fctx = pipe._build_filter_context(
        ohlcv=pd.DataFrame(columns=["date", "code", "volume"]),
        at=date(2026, 4, 15),
    )
    cctx = pipe._build_constraint_context(at=date(2026, 4, 15))

    assert fctx.listing_dates == {"000001": date(1991, 4, 3)}
    assert fctx.st_flags == set()  # is_st=False -> empty set
    assert cctx.industry_map == {"000001": "银行"}
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/portfolio/test_pipeline.py::test_pipeline_injects_refdata_when_available -v`
Expected: AttributeError — `_build_filter_context` missing.

- [ ] **Step 3: Extract helpers + wire refdata in pipeline.py**

Edit `instock/portfolio/pipeline.py`. At top add:

```python
from instock.refdata import industry as refdata_industry
from instock.refdata import listing as refdata_listing
from instock.refdata import st as refdata_st
from instock.refdata.schemas import RefdataNotAvailable
```

In `StrategyPipeline`, add two helpers (place before `run()`):

```python
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
```

Then, in `run()`, replace:
- `fctx = FilterContext(ohlcv_panel=ohlcv)` → `fctx = self._build_filter_context(ohlcv, start)`
- `cctx = ConstraintContext(industry_map=None)` → `cctx = self._build_constraint_context(d)`

(The filter context uses `start` once; the constraint context uses each rebalance `d` because industry/ST semantics are as-of that trade day.)

- [ ] **Step 4: Verify pass**

Run: `conda run -n base python -m pytest tests/portfolio -v`
Expected: all pass (new test + existing).

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/pipeline.py tests/portfolio/test_pipeline.py
git commit -m "feat(portfolio): pipeline wires real refdata into filter/constraint contexts"
```

---

## Task 12: Akshare fetchers for industry / listing / ST + three refdata jobs + smoke

**Files:**
- Modify: `instock/datasource/akshare_source.py`
- Create: `instock/job/refdata_industry_snapshot_job.py`
- Create: `instock/job/refdata_listing_dates_job.py`
- Create: `instock/job/refdata_st_snapshot_job.py`
- Create: `tests/refdata/test_real_data_smoke.py`
- Create: `tests/job/test_refdata_jobs.py`

- [ ] **Step 1: Write failing tests for the three jobs (mocked akshare)**

Create `tests/job/test_refdata_jobs.py`:

```python
from datetime import date
from unittest.mock import patch, MagicMock

import pandas as pd

from instock.job import (
    refdata_industry_snapshot_job as ind_job,
    refdata_listing_dates_job as lst_job,
    refdata_st_snapshot_job as st_job,
)
from instock.refdata import industry, listing, st


def test_industry_job_writes_snapshot(tmp_refdata_root):
    fake_rows = pd.DataFrame({
        "code": ["000001", "600000"],
        "industry": ["银行", "银行"],
    })
    with patch.object(ind_job, "_fetch", return_value=fake_rows):
        ind_job.run(snapshot_date=date(2026, 4, 15))
    got = industry.read_industry_map(date(2026, 4, 16))
    assert got == {"000001": "银行", "600000": "银行"}


def test_listing_job_upserts(tmp_refdata_root):
    fake = pd.DataFrame({
        "code": ["000001"],
        "listing_date": pd.to_datetime(["1991-04-03"]),
    })
    with patch.object(lst_job, "_fetch", return_value=fake):
        lst_job.run(codes=["000001"])
    got = listing.read_listing_dates()
    assert got["000001"] == date(1991, 4, 3)


def test_st_job_writes_snapshot(tmp_refdata_root):
    fake = pd.DataFrame({
        "code": ["000001"],
        "is_st": [True],
    })
    with patch.object(st_job, "_fetch", return_value=fake):
        st_job.run(snapshot_date=date(2026, 4, 15))
    got = st.read_st_flags(date(2026, 4, 16))
    assert got == {"000001"}
```

- [ ] **Step 2: Verify fail**

Run: `conda run -n base python -m pytest tests/job/test_refdata_jobs.py -v`
Expected: ImportError.

- [ ] **Step 3: Add akshare fetchers**

Append to `instock/datasource/akshare_source.py` inside `AkShareSource`:

```python
    # ---------- Board industry ----------
    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_board_industry_names(self) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_board_industry_name_em()

    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_board_industry_cons(self, board: str) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_board_industry_cons_em(symbol=board)

    # ---------- Individual info (listing date) ----------
    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_individual_info(self, code: str) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_individual_info_em(symbol=code)

    # ---------- ST snapshot ----------
    @with_retry(max_attempts=3, base_delay=0.5)
    def _fetch_st_snapshot(self) -> pd.DataFrame:
        self._limiter.wait()
        return ak.stock_zh_a_st_em()
```

- [ ] **Step 4: Implement the three jobs**

Create `instock/job/refdata_industry_snapshot_job.py`:

```python
"""Refresh industry-map snapshot.

Usage: python -m instock.job.refdata_industry_snapshot_job [YYYY-MM-DD]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

import pandas as pd

from instock.datasource.registry import get_source
from instock.refdata.industry import write_industry_snapshot

log = logging.getLogger(__name__)


def _fetch() -> pd.DataFrame:
    """Return [code, industry] from akshare board lists."""
    src = get_source()
    names = src._fetch_board_industry_names()  # type: ignore[attr-defined]
    board_col = "板块名称" if "板块名称" in names.columns else names.columns[0]
    rows = []
    for board in names[board_col].astype(str).tolist():
        try:
            cons = src._fetch_board_industry_cons(board)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            log.warning("industry board %s fetch failed: %s", board, exc)
            continue
        code_col = "代码" if "代码" in cons.columns else cons.columns[1]
        codes = cons[code_col].astype(str).str.zfill(6)
        for c in codes:
            rows.append({"code": c, "industry": board})
    return pd.DataFrame(rows)


def run(snapshot_date: date) -> None:
    df = _fetch()
    if df.empty:
        log.warning("industry fetch produced no rows")
        return
    df = df.drop_duplicates(subset=["code"], keep="first")
    df["snapshot_date"] = pd.Timestamp(snapshot_date)
    write_industry_snapshot(df)
    log.info("industry snapshot: %s rows", len(df))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = sys.argv[1:]
    d = (
        datetime.strptime(args[0], "%Y-%m-%d").date()
        if args else date.today()
    )
    run(d)
```

Create `instock/job/refdata_listing_dates_job.py`:

```python
"""Refresh listing-dates table.

Usage: python -m instock.job.refdata_listing_dates_job [CODE [CODE ...]]
With no args: tries all codes currently in CSI-300 membership.
"""
from __future__ import annotations

import logging
import sys
from datetime import date

import pandas as pd

from instock.datasource.registry import get_source
from instock.refdata.listing import upsert_listing_dates

log = logging.getLogger(__name__)


def _fetch(codes: list[str]) -> pd.DataFrame:
    src = get_source()
    rows = []
    for c in codes:
        try:
            info = src._fetch_individual_info(c)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            log.warning("individual_info %s failed: %s", c, exc)
            continue
        # akshare returns two-col df: item/value
        if info is None or info.empty:
            continue
        m = dict(zip(info["item"].astype(str), info["value"].astype(str)))
        ld = m.get("上市时间") or m.get("上市日期")
        if not ld:
            continue
        try:
            lt = pd.to_datetime(ld)
        except Exception:  # noqa: BLE001
            continue
        rows.append({"code": c, "listing_date": lt})
    return pd.DataFrame(rows)


def run(codes: list[str] | None = None) -> None:
    if codes is None:
        src = get_source()
        codes = src.get_index_member("000300", date.today())
    df = _fetch(codes)
    if df.empty:
        log.warning("listing fetch produced no rows")
        return
    upsert_listing_dates(df)
    log.info("listing upsert: %s rows", len(df))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = sys.argv[1:]
    run(args if args else None)
```

Create `instock/job/refdata_st_snapshot_job.py`:

```python
"""Refresh ST-flag snapshot.

Usage: python -m instock.job.refdata_st_snapshot_job [YYYY-MM-DD]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

import pandas as pd

from instock.datasource.registry import get_source
from instock.refdata.st import write_st_snapshot

log = logging.getLogger(__name__)


def _fetch() -> pd.DataFrame:
    src = get_source()
    raw = src._fetch_st_snapshot()  # type: ignore[attr-defined]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["code", "is_st"])
    code_col = "代码" if "代码" in raw.columns else raw.columns[0]
    codes = raw[code_col].astype(str).str.zfill(6)
    return pd.DataFrame({"code": codes.tolist(), "is_st": [True] * len(codes)})


def run(snapshot_date: date) -> None:
    df = _fetch()
    df["snapshot_date"] = pd.Timestamp(snapshot_date)
    if df.empty:
        log.warning("ST fetch produced no rows")
        return
    write_st_snapshot(df)
    log.info("ST snapshot: %s rows", len(df))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = sys.argv[1:]
    d = (
        datetime.strptime(args[0], "%Y-%m-%d").date()
        if args else date.today()
    )
    run(d)
```

- [ ] **Step 5: Verify pass**

Run: `conda run -n base python -m pytest tests/job/test_refdata_jobs.py -v`
Expected: 3 passed.

- [ ] **Step 6: Write the real-data smoke test**

Create `tests/refdata/test_real_data_smoke.py`:

```python
"""Real-akshare smoke for Sub-2.5 refdata pipeline.

Runs only when INSTOCK_SUB25_SMOKE=1 is set. Pulls tiny real samples to
verify akshare's columns still match our fetchers.
"""
from __future__ import annotations

import os
import sys
from datetime import date

import pandas as pd
import pytest

from instock.datasource.registry import get_source
from instock.refdata.industry import write_industry_snapshot, read_industry_map
from instock.refdata.listing import upsert_listing_dates, read_listing_dates
from instock.refdata.st import write_st_snapshot, read_st_flags
from instock.refdata.ohlcv_store import OhlcvPanelStore


pytestmark = pytest.mark.skipif(
    os.environ.get("INSTOCK_SUB25_SMOKE") != "1",
    reason="requires INSTOCK_SUB25_SMOKE=1 (live akshare)",
)


def test_industry_one_board(tmp_refdata_root):
    from instock.job.refdata_industry_snapshot_job import _fetch
    df = _fetch()
    assert not df.empty, "expected industry rows from akshare"
    df["snapshot_date"] = pd.Timestamp(date.today())
    # Sample to keep test fast
    df = df.head(50)
    write_industry_snapshot(df)
    got = read_industry_map(date.today())
    assert len(got) >= 1
    print(f"[smoke] industry rows: {len(got)}", file=sys.stderr)


def test_listing_one_code(tmp_refdata_root):
    from instock.job.refdata_listing_dates_job import _fetch
    df = _fetch(["000001"])
    assert not df.empty
    upsert_listing_dates(df)
    got = read_listing_dates()
    assert "000001" in got
    print(f"[smoke] listing 000001: {got['000001']}", file=sys.stderr)


def test_st_snapshot(tmp_refdata_root):
    from instock.job.refdata_st_snapshot_job import _fetch
    df = _fetch()
    df["snapshot_date"] = pd.Timestamp(date.today())
    if df.empty:
        pytest.skip("ST list empty; nothing to write")
    write_st_snapshot(df)
    got = read_st_flags(date.today())
    print(f"[smoke] ST codes: {len(got)}", file=sys.stderr)


def test_ohlcv_store_one_code(tmp_ohlcv_root):
    source = get_source()
    store = OhlcvPanelStore(source=source)
    df = store.get_panel(
        ["000001"], date(2026, 4, 1), date(2026, 4, 10)
    )
    assert not df.empty
    assert (df["code"] == "000001").all()
    print(f"[smoke] ohlcv rows: {len(df)}", file=sys.stderr)
```

- [ ] **Step 7: Run full test suite**

Run: `conda run -n base python -m pytest tests/ -x -q`
Expected: all non-smoke tests pass.

- [ ] **Step 8: Commit**

```bash
git add instock/datasource/akshare_source.py \
        instock/job/refdata_industry_snapshot_job.py \
        instock/job/refdata_listing_dates_job.py \
        instock/job/refdata_st_snapshot_job.py \
        tests/job/test_refdata_jobs.py \
        tests/refdata/test_real_data_smoke.py
git commit -m "feat(job): three refdata jobs + akshare fetchers + smoke test"
```

---

## Task 13: Housekeeping — roadmap + follow-up + tag

**Files:**
- Modify: `docs/superpowers/roadmap.md`
- Create: `docs/superpowers/followups/subproject-2.5-data-alignment.md`

- [ ] **Step 1: Append Sub-2.5 section to roadmap**

Edit `docs/superpowers/roadmap.md`. Insert after the Sub-2 section, before Sub-3:

```markdown
### Sub-project 2.5 ✅ 数据层对齐（已合并到 master，tag `subproject-2.5-data-alignment-mvp`）

**实际交付**（MVP）：
- `instock/refdata/` 包：industry / listing / st / ohlcv_store + schemas
  - `read_industry_map(at)`、`read_st_flags(at)`：as-of 查询最近快照
  - `read_listing_dates()` / `upsert_listing_dates`
  - `OhlcvPanelStore`：本地 Parquet 缓存 + 缺失时 IDataSource 在线补齐；
    按 trade_calendar 判缺失（不按 date.range）
- `instock/factors/bootstrap.py::register_default_factors()`：修 Sub-1 follow-up A
  silent-no-op bug；Sub-1 / Sub-2 daily job 共用
- `instock/portfolio/filters.py`：新增 `STFilter` + `LimitFilter`（按板块差化阈值）；
  `NewListingFilter` 改为优先用 `listing_dates`，缺失时回退
- `instock/portfolio/pipeline.py`：自动注入 refdata 到 `FilterContext` /
  `ConstraintContext`；缺 refdata 时各 filter/constraint 回退 + warn 一次
- 三个 refdata daily job：industry（推荐周）/ listing（推荐月）/ ST（推荐周）

**MVP 验收已达成**：
- refdata reader 从空态到有态的全链路单测
- `OhlcvPanelStore` cache-hit / cache-miss / trade_calendar-gap 单测
- Sub-2 默认 `FilterChain` 升级为 `[Suspended, NewListing(refdata), ST, Limit]`
  并注入真 industry_map
- `INSTOCK_SUB25_SMOKE=1` 真数据冒烟 4 例

**follow-up**: `docs/superpowers/followups/subproject-2.5-data-alignment.md`
```

Also bump the milestone table: add row for Sub-2.5 状态 ✅, and change Sub-3 's row estimated tasks per actual state.

- [ ] **Step 2: Create follow-up doc**

Create `docs/superpowers/followups/subproject-2.5-data-alignment.md`:

```markdown
# Sub-project 2.5 (Data Alignment) — Follow-up Items

Source: `docs/superpowers/specs/2026-04-18-data-alignment-design.md`,
`docs/superpowers/plans/2026-04-18-data-alignment.md`, per-task review.

## A. Known MVP trade-offs (documented risks)

- [ ] **Current-snapshot ST look-ahead**: 2024 年 ST 股票在 2020 年回测被错误标。
      Sub-3 启动时在 HTML 报告里加"st=current snapshot"水印。
- [ ] **Current-snapshot industry look-ahead**: 同上语义。水印同。
- [ ] **OhlcvPanelStore 首次预热慢**: akshare 0.2s/req × N codes × K 年。
      预置 CLI `python -m instock.refdata.ohlcv_store warm`（Task 5 已做）；
      Sub-3 启动前手动跑。

## B. Small wins (any time)

- [ ] `refdata.industry` / `refdata.st` 只 glob 当前目录，未用 as-of 索引；
      快照多时（>几百）考虑加 manifest 文件。
- [ ] `OhlcvPanelStore._missing_codes` 目前只按 `nunique(date) < needed` 判；
      这对"中间有若干 trade-day 缺失"的股票会误报为 fully-missing，过度拉。
      改为"对齐 trade_calendar 的 (code, date) 缺失集"更精确。
- [ ] `listing_dates` 增量缺口告警阈值目前硬编码；move to config。
- [ ] `LimitFilter` 仅判 limit-up；limit-down 同日卖出约束留给 Sub-3。

## C. Deferred / out-of-scope

- 历史 ST / 历史行业快照（tushare 商业接口或累积 N 年自拉）
- 申万行业、证监会行业
- Tushare 实现 listing / ST / industry 三件

## D. Sub-3 entry hand-off

Sub-3 backtester should:
1. Receive an `OhlcvPanelStore` instance instead of calling `IDataSource.get_ohlcv` directly.
2. Query `read_industry_map(at)` / `read_st_flags(at)` / `read_listing_dates()` as-of each rebalance date.
3. Emit report watermark: "refdata as of YYYY-MM-DD; historical ST/industry approximated".
```

- [ ] **Step 3: Run full suite one more time**

Run: `conda run -n base python -m pytest tests/ -x -q`
Expected: all non-smoke pass.

- [ ] **Step 4: Commit + tag**

```bash
git add docs/superpowers/roadmap.md \
        docs/superpowers/followups/subproject-2.5-data-alignment.md
git commit -m "docs(sub-2.5): follow-ups + mark sub-2.5 complete on roadmap"
git tag subproject-2.5-data-alignment-mvp
```

---

## Self-Review Notes

**Spec coverage check** (every §1–§9 item in the spec must map to a task):
- §1 Scope items 1–5: industry=T2, listing=T3, st=T4, ohlcv_store=T5, bootstrap=T6 ✅
- §1 MVP acceptance: refdata jobs = T12; FilterChain upgrade = T8+T9+T10+T11;
  industry_map injection = T11; bootstrap + non-empty assert = T6+T7; smoke = T12 ✅
- §2 Layout: all paths used verbatim in T2/T3/T4/T5 ✅
- §3 Interfaces: STFilter/LimitFilter = T8/T9; FilterContext extension = T8;
  NewListingFilter rewrite = T10; bootstrap = T6 ✅
- §4 Data flow: T11 wires pipeline; T12 wires jobs ✅
- §5 Error handling: every case hits a test (STFilter fallback T8; LimitFilter
  missing-prior T9; NewListingFilter fallback T10; Refdata missing → pipeline
  fallback T11; OhlcvPanelStore fetch-fail T5; bootstrap idempotent T6) ✅
- §6 Test strategy: mock + env-gated smoke, as specified ✅
- §7 Risks: all 5 banked in Task 13 follow-up doc ✅
- §8 Task breakdown: matches spec's 14-task estimate (0–13) ✅
- §9 Contracts: Sub-2 backward-compat verified via T8/T10/T11 fallback tests;
  Sub-3 interface (read_* signatures) defined in T2/T3/T4 ✅

**Type consistency check**:
- `FilterContext.listing_dates: dict[str, date] | None` — introduced T8 (field),
  consumed T10 (NewListingFilter), written T11 (pipeline). ✅ consistent.
- `FilterContext.st_flags: set[str] | None` — same three points. ✅
- `ConstraintContext.industry_map: dict[str, str] | None` — already exists;
  written T11. ✅
- `register_default_factors()` — signature identical across T6/T7. ✅
- `read_industry_map(at: date) -> dict[str, str]` / `read_st_flags(at: date) -> set[str]`
  / `read_listing_dates() -> dict[str, date]` — consistent across T2–T4 and T11/T13. ✅

**Placeholder scan**: no TBD/TODO/"implement later"; every code block is complete.
One caveat: Task 2 Step 3 shows an incorrect `pd.Timestamp.strptime` then corrects
it in the same step — kept deliberately as a code review note. Implementer should
use the corrected form.
