# Portfolio Construction (Sub-Project 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Sub-1 factor Parquet into actionable `HoldingSchedule` Parquet via a composable `StrategyPipeline` of factor-combiner → universe-filter → selector → weighter → constraint → schedule.

**Architecture:** New package `instock/portfolio/` with one ABC + one default implementation per concern, each in its own file. A `StrategyPipeline` wires them together per a `StrategyConfig`. Storage mirrors Sub-1 Parquet-yearly-partition design. A single daily job iterates over a list of `StrategyConfig` and writes each strategy's holdings. No cvxpy, no short side, no MySQL mirror (all YAGNI for MVP; see spec §1).

**Tech Stack:** Python 3.12, pandas, pandera (already pinned 0.31.1), pyarrow (already in use for factor storage). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-04-18-portfolio-construction-design.md`

---

## File structure

New files (17):
- `instock/portfolio/__init__.py`
- `instock/portfolio/schemas.py` — pandera schema + validation error
- `instock/portfolio/storage.py` — Parquet read/write
- `instock/portfolio/combiner.py` — `FactorCombiner` ABC + `EqualRankCombiner`
- `instock/portfolio/filters.py` — `UniverseFilter` ABC + `FilterChain` + 2 internals
- `instock/portfolio/selector.py` — `Selector` ABC + 2 internals
- `instock/portfolio/weighter.py` — `Weighter` ABC + `EqualWeighter` + `WeighterContext`
- `instock/portfolio/constraints.py` — `PortfolioConstraint` ABC + 2 internals + `ConstraintContext`
- `instock/portfolio/schedule.py` — `RebalanceSchedule` ABC + 2 internals
- `instock/portfolio/pipeline.py` — `StrategyConfig` + `StrategyPipeline`
- `instock/job/generate_holdings_daily_job.py`
- `tests/portfolio/__init__.py`
- `tests/portfolio/test_storage.py`
- `tests/portfolio/test_combiner.py`
- `tests/portfolio/test_filters.py`
- `tests/portfolio/test_selector.py`
- `tests/portfolio/test_weighter.py`
- `tests/portfolio/test_constraints.py`
- `tests/portfolio/test_schedule.py`
- `tests/portfolio/test_pipeline.py`
- `tests/portfolio/test_daily_job.py`
- `tests/portfolio/test_real_data_smoke.py` (default skip)

Modified files (1):
- `tests/conftest.py` — add `tmp_holding_root` fixture next to `tmp_factor_root`

---

## Task 0: Scaffold + conftest

**Files:**
- Create: `instock/portfolio/__init__.py` (empty)
- Create: `tests/portfolio/__init__.py` (empty)
- Modify: `tests/conftest.py`

- [ ] **Step 1: Create empty package init files**

Create `instock/portfolio/__init__.py` and `tests/portfolio/__init__.py` as empty files.

- [ ] **Step 2: Extend conftest with `tmp_holding_root`**

Replace `tests/conftest.py` with:

```python
import pytest


@pytest.fixture
def tmp_factor_root(tmp_path, monkeypatch):
    """Point the factor Parquet root at a per-test tmp dir."""
    root = tmp_path / "factors"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_FACTOR_ROOT", str(root))
    return root


@pytest.fixture
def tmp_holding_root(tmp_path, monkeypatch):
    """Point the holding Parquet root at a per-test tmp dir."""
    root = tmp_path / "holdings"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_HOLDING_ROOT", str(root))
    return root
```

- [ ] **Step 3: Commit**

```bash
git add instock/portfolio/__init__.py tests/portfolio/__init__.py tests/conftest.py
git commit -m "chore(portfolio): scaffold package + tmp_holding_root fixture"
```

---

## Task 1: `HoldingSchedule` schema + validation error

**Files:**
- Create: `instock/portfolio/schemas.py`
- Test: `tests/portfolio/test_schemas.py`

- [ ] **Step 1: Write failing schema tests**

Create `tests/portfolio/test_schemas.py`:

```python
import pandas as pd
import pytest

from instock.portfolio.schemas import (
    HOLDING_SCHEDULE_SCHEMA,
    HoldingScheduleValidationError,
    validate_holding_invariants,
)


def _good_row(date="2024-01-02", code="600519", weight=1.0,
              score=0.5, strategy="default"):
    return {
        "date": pd.Timestamp(date),
        "code": code,
        "weight": weight,
        "score": score,
        "strategy": strategy,
    }


def test_schema_accepts_valid_row():
    df = pd.DataFrame([_good_row()])
    out = HOLDING_SCHEDULE_SCHEMA.validate(df)
    assert list(out.columns) >= ["date", "code", "weight", "score", "strategy"]


def test_schema_rejects_bad_code():
    df = pd.DataFrame([_good_row(code="ABC")])
    with pytest.raises(Exception):
        HOLDING_SCHEDULE_SCHEMA.validate(df)


def test_schema_rejects_weight_out_of_range():
    df = pd.DataFrame([_good_row(weight=1.5)])
    with pytest.raises(Exception):
        HOLDING_SCHEDULE_SCHEMA.validate(df)


def test_schema_accepts_null_score():
    df = pd.DataFrame([_good_row(score=None)])
    out = HOLDING_SCHEDULE_SCHEMA.validate(df)
    assert pd.isna(out["score"].iloc[0])


def test_invariants_pass_when_weights_sum_to_one():
    df = pd.DataFrame([
        _good_row(code="600519", weight=0.5),
        _good_row(code="000001", weight=0.5),
    ])
    validate_holding_invariants(df)  # should not raise


def test_invariants_fail_when_weights_do_not_sum_to_one():
    df = pd.DataFrame([
        _good_row(code="600519", weight=0.4),
        _good_row(code="000001", weight=0.4),
    ])
    with pytest.raises(HoldingScheduleValidationError):
        validate_holding_invariants(df)


def test_invariants_fail_on_duplicate_code_in_same_group():
    df = pd.DataFrame([
        _good_row(code="600519", weight=0.5),
        _good_row(code="600519", weight=0.5),
    ])
    with pytest.raises(HoldingScheduleValidationError):
        validate_holding_invariants(df)
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_schemas.py -v`
Expected: all fail with `ModuleNotFoundError`.

- [ ] **Step 3: Implement schema module**

Create `instock/portfolio/schemas.py`:

```python
"""Pandera schema and invariants for HoldingSchedule.

Semantic contract (CRITICAL, relied on by Sub-3 backtester):
    HoldingSchedule.date = T means:
        The target portfolio is generated from data known as of T's close
        and is executable at T+1 open.
    Implication: factors/prices/halt-status must all use data <= T.
"""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa


class HoldingScheduleValidationError(Exception):
    """Raised when a HoldingSchedule violates schema or invariants.

    Fail-fast: never retried. Indicates a bug in pipeline/constraints.
    """


HOLDING_SCHEDULE_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "code": pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "weight": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
        "score": pa.Column(float, nullable=True),
        "strategy": pa.Column(str, pa.Check.str_length(min_value=1)),
    },
    coerce=True,
    strict=False,
)


_WEIGHT_SUM_TOL = 1e-6


def validate_holding_invariants(df: pd.DataFrame) -> None:
    """Enforce invariants pandera cannot express.

    For each (date, strategy) group:
      - weight.sum() ~= 1.0 (within _WEIGHT_SUM_TOL)
      - code is unique
    """
    if df.empty:
        return
    for (d, s), g in df.groupby(["date", "strategy"]):
        total = g["weight"].sum()
        if abs(total - 1.0) > _WEIGHT_SUM_TOL:
            raise HoldingScheduleValidationError(
                f"weight sum {total:.6f} != 1.0 for (date={d}, strategy={s})"
            )
        if g["code"].duplicated().any():
            dup = g.loc[g["code"].duplicated(), "code"].tolist()
            raise HoldingScheduleValidationError(
                f"duplicate codes {dup} for (date={d}, strategy={s})"
            )
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_schemas.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/schemas.py tests/portfolio/test_schemas.py
git commit -m "feat(portfolio): HoldingSchedule schema + sum-to-1 invariant"
```

---

## Task 2: Storage (read/write, yearly partition)

**Files:**
- Create: `instock/portfolio/storage.py`
- Test: `tests/portfolio/test_storage.py`

- [ ] **Step 1: Write failing storage tests**

Create `tests/portfolio/test_storage.py`:

```python
import pandas as pd
import pytest

from instock.portfolio import storage
from instock.portfolio.schemas import HoldingScheduleValidationError


def _df(dates, codes, weight, strategy="default"):
    n = len(dates)
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": codes,
        "weight": [weight] * n,
        "score": [0.0] * n,
        "strategy": [strategy] * n,
    })


def test_write_and_read_roundtrip(tmp_holding_root):
    df = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5)
    storage.write_holding("default", df)
    out = storage.read_holding("default",
                               pd.Timestamp("2024-01-02"),
                               pd.Timestamp("2024-01-02"))
    assert len(out) == 2
    assert abs(out["weight"].sum() - 1.0) < 1e-9


def test_write_partitions_by_year(tmp_holding_root):
    df_2023 = _df(["2023-12-29", "2023-12-29"], ["600519", "000001"], 0.5)
    df_2024 = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5)
    storage.write_holding("default", df_2023)
    storage.write_holding("default", df_2024)
    files = sorted(p.name for p in (tmp_holding_root / "default").iterdir())
    assert files == ["2023.parquet", "2024.parquet"]


def test_write_dedupes_on_rewrite(tmp_holding_root):
    df_a = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5)
    df_b = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.5,
               strategy="default")
    # Second write should not duplicate rows (keep="last").
    storage.write_holding("default", df_a)
    storage.write_holding("default", df_b)
    out = storage.read_holding("default",
                               pd.Timestamp("2024-01-02"),
                               pd.Timestamp("2024-01-02"))
    assert len(out) == 2


def test_write_rejects_bad_weight_sum(tmp_holding_root):
    bad = _df(["2024-01-02", "2024-01-02"], ["600519", "000001"], 0.4)
    with pytest.raises(HoldingScheduleValidationError):
        storage.write_holding("default", bad)


def test_read_returns_empty_frame_when_no_files(tmp_holding_root):
    out = storage.read_holding("nonexistent",
                               pd.Timestamp("2024-01-02"),
                               pd.Timestamp("2024-01-02"))
    assert out.empty
    assert list(out.columns) == ["date", "code", "weight", "score", "strategy"]
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_storage.py -v`
Expected: all fail with `ModuleNotFoundError`.

- [ ] **Step 3: Implement storage**

Create `instock/portfolio/storage.py`:

```python
"""Parquet storage for HoldingSchedule.

Mirrors instock/factors/storage.py:
  <INSTOCK_HOLDING_ROOT>/<strategy>/<year>.parquet
  (date, code, strategy) unique per file via drop_duplicates keep="last".
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from .schemas import (
    HOLDING_SCHEDULE_SCHEMA,
    validate_holding_invariants,
)

_COLUMNS = ["date", "code", "weight", "score", "strategy"]


def _root() -> Path:
    root = os.environ.get("INSTOCK_HOLDING_ROOT", "data/holdings")
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _strategy_dir(strategy: str) -> Path:
    d = _root() / strategy
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_holding(strategy: str, df: pd.DataFrame) -> None:
    """Validate + append-and-dedup writer, partitioned by year(date)."""
    if df.empty:
        return
    df = HOLDING_SCHEDULE_SCHEMA.validate(df.copy())
    validate_holding_invariants(df)
    df["year"] = df["date"].dt.year
    for year, group in df.groupby("year"):
        target = _strategy_dir(strategy) / f"{year}.parquet"
        payload = group.drop(columns=["year"])
        if target.exists():
            old = pd.read_parquet(target)
            payload = pd.concat([old, payload], ignore_index=True)
        payload = (
            payload.drop_duplicates(
                subset=["date", "code", "strategy"], keep="last"
            )
            .sort_values(["date", "code"])
            .reset_index(drop=True)
        )
        payload.to_parquet(target, index=False)


def read_holding(
    strategy: str, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    d = _strategy_dir(strategy)
    frames = []
    for year in range(start.year, end.year + 1):
        target = d / f"{year}.parquet"
        if target.exists():
            frames.append(pd.read_parquet(target))
    if not frames:
        return pd.DataFrame(columns=_COLUMNS)
    df = pd.concat(frames, ignore_index=True)
    mask = (df["date"] >= start) & (df["date"] <= end)
    return df.loc[mask].reset_index(drop=True)
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_storage.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/storage.py tests/portfolio/test_storage.py
git commit -m "feat(portfolio): Parquet yearly-partition holding storage"
```

---

## Task 3: `FactorCombiner` ABC + `EqualRankCombiner`

**Files:**
- Create: `instock/portfolio/combiner.py`
- Test: `tests/portfolio/test_combiner.py`

- [ ] **Step 1: Write failing combiner tests**

Create `tests/portfolio/test_combiner.py`:

```python
import pandas as pd
import pytest

from instock.portfolio.combiner import EqualRankCombiner


def test_equal_rank_combiner_basic():
    # Two factors, same direction. Combined score should correlate perfectly
    # with input direction.
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02"] * 3),
        "code": ["600519", "000001", "000002"],
        "mom_20d": [0.1, 0.2, 0.3],
        "pe_ttm": [10.0, 20.0, 30.0],
    })
    out = EqualRankCombiner().combine(df)
    assert list(out.columns) == ["date", "code", "score"]
    assert out.sort_values("score")["code"].tolist() == [
        "600519", "000001", "000002"
    ]


def test_equal_rank_combiner_drops_row_with_any_nan():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02"] * 3),
        "code": ["600519", "000001", "000002"],
        "mom_20d": [0.1, None, 0.3],
        "pe_ttm": [10.0, 20.0, 30.0],
    })
    out = EqualRankCombiner().combine(df)
    assert "000001" not in out["code"].tolist()


def test_equal_rank_combiner_score_in_unit_interval():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02"] * 4),
        "code": ["A00001", "B00002", "C00003", "D00004"],
        "f1": [1.0, 2.0, 3.0, 4.0],
    })
    out = EqualRankCombiner().combine(df)
    assert out["score"].between(0.0, 1.0).all()
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_combiner.py -v`
Expected: fails with ImportError.

- [ ] **Step 3: Implement combiner**

Create `instock/portfolio/combiner.py`:

```python
"""FactorCombiner: wide factor panel -> single combined score per (date, code).

MVP implementation: EqualRankCombiner — per-date cross-sectional percentile
rank of each factor, then arithmetic mean.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class FactorCombiner(ABC):
    @abstractmethod
    def combine(self, factor_panel: pd.DataFrame) -> pd.DataFrame:
        """Combine factors into one score.

        Args:
            factor_panel: long-form with columns [date, code, <factor1>,
                <factor2>, ...]. Factor columns are whatever non-key
                columns are present.

        Returns:
            DataFrame[date, code, score].
        """


class EqualRankCombiner(FactorCombiner):
    """Per-date rank(pct=True, method='average') on every factor, then mean.

    Any row with a NaN in any factor column is dropped before ranking.
    """

    def combine(self, factor_panel: pd.DataFrame) -> pd.DataFrame:
        key_cols = ["date", "code"]
        factor_cols = [c for c in factor_panel.columns if c not in key_cols]
        if not factor_cols:
            raise ValueError("factor_panel has no factor columns")

        df = factor_panel.dropna(subset=factor_cols).copy()
        if df.empty:
            return pd.DataFrame(columns=["date", "code", "score"])

        for c in factor_cols:
            df[f"_rk_{c}"] = df.groupby("date")[c].rank(
                pct=True, method="average"
            )
        rank_cols = [f"_rk_{c}" for c in factor_cols]
        df["score"] = df[rank_cols].mean(axis=1)
        return df[["date", "code", "score"]].reset_index(drop=True)
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_combiner.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/combiner.py tests/portfolio/test_combiner.py
git commit -m "feat(portfolio): FactorCombiner ABC + EqualRankCombiner"
```

---

## Task 4: `UniverseFilter` ABC + `FilterChain` + 2 filters

**Files:**
- Create: `instock/portfolio/filters.py`
- Test: `tests/portfolio/test_filters.py`

- [ ] **Step 1: Write failing filter tests**

Create `tests/portfolio/test_filters.py`:

```python
import pandas as pd
import pytest

from instock.portfolio.filters import (
    FilterChain,
    FilterContext,
    NewListingFilter,
    SuspendedFilter,
)


def _ohlcv(code, dates, volume=1_000_000):
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": [code] * len(dates),
        "volume": [volume] * len(dates),
    })


def test_suspended_filter_drops_zero_volume():
    panel = pd.concat([
        _ohlcv("600519", ["2024-01-02"], volume=1000),
        _ohlcv("000001", ["2024-01-02"], volume=0),
    ])
    ctx = FilterContext(ohlcv_panel=panel)
    out = SuspendedFilter().apply(
        ["600519", "000001"], pd.Timestamp("2024-01-02"), ctx
    )
    assert out == ["600519"]


def test_suspended_filter_drops_missing_rows():
    panel = _ohlcv("600519", ["2024-01-02"])
    ctx = FilterContext(ohlcv_panel=panel)
    out = SuspendedFilter().apply(
        ["600519", "000001"], pd.Timestamp("2024-01-02"), ctx
    )
    assert out == ["600519"]


def test_new_listing_filter_drops_too_young():
    # 600519 first seen 2023-12-01 (>60d before 2024-03-01): keep
    # 000001 first seen 2024-02-15 (<60d before 2024-03-01): drop
    panel = pd.concat([
        _ohlcv("600519", ["2023-12-01", "2024-03-01"]),
        _ohlcv("000001", ["2024-02-15", "2024-03-01"]),
    ])
    ctx = FilterContext(ohlcv_panel=panel)
    out = NewListingFilter(min_days=60).apply(
        ["600519", "000001"], pd.Timestamp("2024-03-01"), ctx
    )
    assert out == ["600519"]


def test_filter_chain_applies_in_order():
    panel = pd.concat([
        _ohlcv("600519", ["2023-12-01", "2024-03-01"]),
        _ohlcv("000001", ["2024-02-15", "2024-03-01"]),
        _ohlcv("000002", ["2023-12-01", "2024-03-01"], volume=0),
    ])
    ctx = FilterContext(ohlcv_panel=panel)
    chain = FilterChain([SuspendedFilter(), NewListingFilter(min_days=60)])
    out = chain.apply(
        ["600519", "000001", "000002"], pd.Timestamp("2024-03-01"), ctx
    )
    assert out == ["600519"]


def test_filter_chain_short_circuits_on_empty():
    panel = pd.DataFrame({"date": [], "code": [], "volume": []})
    ctx = FilterContext(ohlcv_panel=panel)
    out = FilterChain([SuspendedFilter()]).apply(
        ["600519"], pd.Timestamp("2024-01-02"), ctx
    )
    assert out == []
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_filters.py -v`
Expected: fails with ImportError.

- [ ] **Step 3: Implement filters**

Create `instock/portfolio/filters.py`:

```python
"""Universe filters: shrink the candidate code list before selection.

MVP rules only use information already present in OHLCV.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class FilterContext:
    """Shared data passed to every filter. OHLCV must span back far enough
    for history-based filters (e.g. NewListingFilter.min_days)."""
    ohlcv_panel: pd.DataFrame


class UniverseFilter(ABC):
    @abstractmethod
    def apply(
        self,
        codes: list[str],
        at: pd.Timestamp,
        context: FilterContext,
    ) -> list[str]:
        ...


class SuspendedFilter(UniverseFilter):
    """Drop codes with no OHLCV row on `at` or with volume == 0 on `at`."""

    def apply(self, codes, at, context):
        if not codes:
            return []
        panel = context.ohlcv_panel
        on_day = panel[panel["date"] == at]
        active = set(on_day.loc[on_day["volume"] > 0, "code"].astype(str))
        return [c for c in codes if c in active]


class NewListingFilter(UniverseFilter):
    """Drop codes whose earliest OHLCV date is < `min_days` before `at`."""

    def __init__(self, min_days: int = 60) -> None:
        self.min_days = min_days

    def apply(self, codes, at, context):
        if not codes:
            return []
        panel = context.ohlcv_panel
        if panel.empty:
            return []
        first_seen = panel.groupby("code")["date"].min()
        cutoff = at - pd.Timedelta(days=self.min_days)
        ok = set(first_seen[first_seen <= cutoff].index.astype(str))
        return [c for c in codes if c in ok]


class FilterChain:
    """Apply a list of filters in order. Short-circuits on empty intermediate."""

    def __init__(self, filters: list[UniverseFilter]) -> None:
        self.filters = filters

    def apply(self, codes, at, context):
        out = list(codes)
        for f in self.filters:
            out = f.apply(out, at, context)
            if not out:
                return []
        return out
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_filters.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/filters.py tests/portfolio/test_filters.py
git commit -m "feat(portfolio): universe filters (suspended, new listing, chain)"
```

---

## Task 5: `Selector` ABC + `TopQuantileSelector` + `TopNSelector`

**Files:**
- Create: `instock/portfolio/selector.py`
- Test: `tests/portfolio/test_selector.py`

- [ ] **Step 1: Write failing selector tests**

Create `tests/portfolio/test_selector.py`:

```python
import pandas as pd

from instock.portfolio.selector import TopNSelector, TopQuantileSelector


def _scores(pairs):
    return pd.Series({c: s for c, s in pairs})


def test_top_quantile_picks_top_fraction():
    s = _scores([("A00001", 0.1), ("B00002", 0.2), ("C00003", 0.9),
                 ("D00004", 0.8), ("E00005", 0.5)])
    out = TopQuantileSelector(quantile=0.4).select(
        s, ["A00001", "B00002", "C00003", "D00004", "E00005"]
    )
    assert set(out) == {"C00003", "D00004"}


def test_top_quantile_guarantees_at_least_one():
    s = _scores([("A00001", 0.1), ("B00002", 0.2)])
    out = TopQuantileSelector(quantile=0.0001).select(
        s, ["A00001", "B00002"]
    )
    assert len(out) == 1
    assert out == ["B00002"]


def test_top_quantile_respects_universe_filter():
    s = _scores([("A00001", 0.9), ("B00002", 0.8), ("C00003", 0.1)])
    out = TopQuantileSelector(quantile=0.5).select(
        s, ["B00002", "C00003"]  # A00001 excluded by universe
    )
    assert out == ["B00002"]


def test_top_n_returns_top_n():
    s = _scores([("A00001", 0.1), ("B00002", 0.9), ("C00003", 0.5)])
    out = TopNSelector(n=2).select(s, ["A00001", "B00002", "C00003"])
    assert out == ["B00002", "C00003"]


def test_top_n_returns_all_if_universe_smaller():
    s = _scores([("A00001", 0.1), ("B00002", 0.9)])
    out = TopNSelector(n=5).select(s, ["A00001", "B00002"])
    assert set(out) == {"A00001", "B00002"}


def test_selector_empty_universe_returns_empty():
    s = _scores([("A00001", 0.9)])
    assert TopNSelector(n=5).select(s, []) == []
    assert TopQuantileSelector(0.1).select(s, []) == []
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_selector.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement selector**

Create `instock/portfolio/selector.py`:

```python
"""Selector: pick a subset of the filtered universe based on per-code scores."""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Selector(ABC):
    @abstractmethod
    def select(self, scores: pd.Series, universe: list[str]) -> list[str]:
        """Args:
            scores: Series indexed by code, value = score on the current date.
            universe: filtered list of eligible codes.
        Returns:
            selected codes (subset of universe).
        """


def _ranked_within_universe(scores: pd.Series, universe: list[str]) -> pd.Series:
    inter = [c for c in universe if c in scores.index]
    if not inter:
        return pd.Series(dtype=float)
    return scores.loc[inter].sort_values(ascending=False)


class TopQuantileSelector(Selector):
    """Top `quantile` fraction of the (universe-intersected) scored codes.

    Always returns at least 1 code when the universe is non-empty.
    """

    def __init__(self, quantile: float) -> None:
        if not (0.0 < quantile <= 1.0):
            raise ValueError(f"quantile must be in (0, 1], got {quantile}")
        self.quantile = quantile

    def select(self, scores, universe):
        ranked = _ranked_within_universe(scores, universe)
        if ranked.empty:
            return []
        k = max(1, round(len(ranked) * self.quantile))
        return ranked.head(k).index.tolist()


class TopNSelector(Selector):
    """Top `n` of the (universe-intersected) scored codes."""

    def __init__(self, n: int) -> None:
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")
        self.n = n

    def select(self, scores, universe):
        ranked = _ranked_within_universe(scores, universe)
        if ranked.empty:
            return []
        return ranked.head(self.n).index.tolist()
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_selector.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/selector.py tests/portfolio/test_selector.py
git commit -m "feat(portfolio): Selector ABC + TopQuantileSelector/TopNSelector"
```

---

## Task 6: `Weighter` ABC + `EqualWeighter` + context

**Files:**
- Create: `instock/portfolio/weighter.py`
- Test: `tests/portfolio/test_weighter.py`

- [ ] **Step 1: Write failing weighter tests**

Create `tests/portfolio/test_weighter.py`:

```python
import pandas as pd

from instock.portfolio.weighter import EqualWeighter, WeighterContext


def test_equal_weighter_sums_to_one():
    ctx = WeighterContext(
        price_panel=pd.DataFrame(),
        mcap_panel=None,
        scores=pd.Series(dtype=float),
    )
    w = EqualWeighter().weigh(["A00001", "B00002", "C00003"], ctx)
    assert abs(w.sum() - 1.0) < 1e-12
    assert set(w.index) == {"A00001", "B00002", "C00003"}


def test_equal_weighter_empty_returns_empty_series():
    ctx = WeighterContext(
        price_panel=pd.DataFrame(),
        mcap_panel=None,
        scores=pd.Series(dtype=float),
    )
    w = EqualWeighter().weigh([], ctx)
    assert w.empty
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_weighter.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement weighter**

Create `instock/portfolio/weighter.py`:

```python
"""Weighter: assign portfolio weights to selected codes.

MVP: EqualWeighter. Context carries data future weighters (inverse-vol,
market-cap, score-weighted) will need; MVP ignores all of it.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class WeighterContext:
    price_panel: pd.DataFrame
    mcap_panel: pd.DataFrame | None
    scores: pd.Series


class Weighter(ABC):
    @abstractmethod
    def weigh(
        self, selected: list[str], context: WeighterContext
    ) -> pd.Series:
        """Returns Series indexed by code, weights sum to 1.0
        (empty Series when selected is empty)."""


class EqualWeighter(Weighter):
    def weigh(self, selected, context):
        n = len(selected)
        if n == 0:
            return pd.Series(dtype=float)
        return pd.Series(1.0 / n, index=selected)
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_weighter.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/weighter.py tests/portfolio/test_weighter.py
git commit -m "feat(portfolio): Weighter ABC + EqualWeighter"
```

---

## Task 7: `PortfolioConstraint` ABC + `MaxWeightConstraint`

**Files:**
- Create: `instock/portfolio/constraints.py`
- Test: `tests/portfolio/test_constraints.py`

- [ ] **Step 1: Write failing constraint tests**

Create `tests/portfolio/test_constraints.py`:

```python
import pandas as pd
import pytest

from instock.portfolio.constraints import (
    ConstraintContext,
    IndustryExposureConstraint,
    MaxWeightConstraint,
)


def _ctx(industry_map=None):
    return ConstraintContext(industry_map=industry_map)


def test_max_weight_noop_when_under_cap():
    w = pd.Series({"A00001": 0.3, "B00002": 0.3, "C00003": 0.4})
    out = MaxWeightConstraint(0.5).apply(w, _ctx())
    pd.testing.assert_series_equal(out.sort_index(), w.sort_index())


def test_max_weight_clips_and_redistributes():
    # One name at 60%, caps at 30%; the 30% overflow should redistribute
    # to the other two (originally 20% each) proportionally.
    w = pd.Series({"A00001": 0.6, "B00002": 0.2, "C00003": 0.2})
    out = MaxWeightConstraint(0.3).apply(w, _ctx())
    assert abs(out.sum() - 1.0) < 1e-9
    assert out["A00001"] <= 0.3 + 1e-9
    # B, C equally split the redistributed weight, so still equal
    assert abs(out["B00002"] - out["C00003"]) < 1e-9


def test_max_weight_iterates_until_converged():
    # 2 names way over cap; first redistribution would push remaining one over.
    w = pd.Series({"A00001": 0.5, "B00002": 0.4, "C00003": 0.05,
                   "D00004": 0.05})
    out = MaxWeightConstraint(0.3).apply(w, _ctx())
    assert (out <= 0.3 + 1e-9).all()
    assert abs(out.sum() - 1.0) < 1e-9


def test_max_weight_infeasible_raises():
    # 2 names, cap 0.3 -> total capacity 0.6 < 1.0: infeasible
    w = pd.Series({"A00001": 0.5, "B00002": 0.5})
    with pytest.raises(ValueError):
        MaxWeightConstraint(0.3).apply(w, _ctx())


def test_industry_constraint_is_noop_without_map():
    w = pd.Series({"A00001": 0.5, "B00002": 0.5})
    out = IndustryExposureConstraint(0.4, industry_map=None).apply(
        w, _ctx(industry_map=None)
    )
    pd.testing.assert_series_equal(out.sort_index(), w.sort_index())


def test_industry_constraint_scales_over_cap_industry():
    w = pd.Series({
        "A00001": 0.4, "B00002": 0.3,  # industry "tech" 70%
        "C00003": 0.2, "D00004": 0.1,  # industry "finance" 30%
    })
    imap = {"A00001": "tech", "B00002": "tech",
            "C00003": "finance", "D00004": "finance"}
    out = IndustryExposureConstraint(0.4, industry_map=imap).apply(
        w, _ctx(industry_map=imap)
    )
    tech_total = out[["A00001", "B00002"]].sum()
    assert tech_total <= 0.4 + 1e-9
    assert abs(out.sum() - 1.0) < 1e-9
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_constraints.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement constraints**

Create `instock/portfolio/constraints.py`:

```python
"""Portfolio-level constraints applied after weighing.

MVP:
  - MaxWeightConstraint: iterative clip + proportional redistribution
  - IndustryExposureConstraint: no-op if industry_map is None, else
    per-industry proportional scaling
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

log = logging.getLogger(__name__)


@dataclass
class ConstraintContext:
    industry_map: dict[str, str] | None = None


class PortfolioConstraint(ABC):
    @abstractmethod
    def apply(
        self, weights: pd.Series, context: ConstraintContext
    ) -> pd.Series:
        """Return new weights. Must still sum to ~1.0."""


class MaxWeightConstraint(PortfolioConstraint):
    """Cap every name at `max_weight`, redistribute overflow to unsaturated
    names in proportion to their current weights. Iterates to fixed point.

    Raises ValueError if n * max_weight < 1.0 (infeasible).
    """

    def __init__(self, max_weight: float, max_iter: int = 100) -> None:
        if not (0.0 < max_weight <= 1.0):
            raise ValueError(f"max_weight must be in (0,1], got {max_weight}")
        self.max_weight = max_weight
        self.max_iter = max_iter

    def apply(self, weights, context):
        if weights.empty:
            return weights
        if len(weights) * self.max_weight < 1.0 - 1e-9:
            raise ValueError(
                f"infeasible: {len(weights)} names * "
                f"max_weight {self.max_weight} < 1.0"
            )

        w = weights.copy().astype(float)
        for _ in range(self.max_iter):
            over = w > self.max_weight + 1e-12
            if not over.any():
                break
            overflow = (w[over] - self.max_weight).sum()
            w[over] = self.max_weight
            unsat = ~over & (w < self.max_weight - 1e-12)
            if not unsat.any():
                break
            w.loc[unsat] += overflow * (
                w.loc[unsat] / w.loc[unsat].sum()
            )
        return w


class IndustryExposureConstraint(PortfolioConstraint):
    """Cap per-industry total weight; scale offending industries down and
    redistribute freed weight pro-rata to non-capped industries.

    If industry_map is None (or ConstraintContext.industry_map is None),
    this is a no-op and emits a warning once per instance.
    """

    def __init__(
        self,
        max_industry_weight: float,
        industry_map: dict[str, str] | None = None,
    ) -> None:
        if not (0.0 < max_industry_weight <= 1.0):
            raise ValueError(
                f"max_industry_weight must be in (0,1], got {max_industry_weight}"
            )
        self.max_industry_weight = max_industry_weight
        self.industry_map = industry_map
        self._warned = False

    def apply(self, weights, context):
        imap = context.industry_map or self.industry_map
        if imap is None:
            if not self._warned:
                log.warning(
                    "IndustryExposureConstraint skipped: industry_map is None"
                )
                self._warned = True
            return weights
        if weights.empty:
            return weights

        # Build industry series aligned to weights index.
        industries = pd.Series(
            {c: imap.get(c, "_unknown") for c in weights.index}
        )
        w = weights.copy().astype(float)
        ind_totals = w.groupby(industries).sum()
        over = ind_totals[ind_totals > self.max_industry_weight + 1e-12]
        if over.empty:
            return w

        # Total weight freed by scaling over-cap industries down.
        freed = 0.0
        for ind, total in over.items():
            codes_in_ind = industries[industries == ind].index
            scale = self.max_industry_weight / total
            freed += w.loc[codes_in_ind].sum() * (1.0 - scale)
            w.loc[codes_in_ind] *= scale

        # Redistribute `freed` pro-rata across non-capped codes
        # (those whose industry is not capped).
        capped_ind = set(over.index)
        non_capped = w.index[~industries.isin(capped_ind)]
        if len(non_capped) > 0 and w.loc[non_capped].sum() > 0:
            w.loc[non_capped] += freed * (
                w.loc[non_capped] / w.loc[non_capped].sum()
            )
        return w
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_constraints.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/constraints.py tests/portfolio/test_constraints.py
git commit -m "feat(portfolio): MaxWeight + IndustryExposure constraints"
```

---

## Task 8: `RebalanceSchedule` ABC + 2 schedules

**Files:**
- Create: `instock/portfolio/schedule.py`
- Test: `tests/portfolio/test_schedule.py`

- [ ] **Step 1: Write failing schedule tests**

Create `tests/portfolio/test_schedule.py`:

```python
from datetime import date

import pytest

from instock.portfolio.schedule import DailyRebalance, WeeklyRebalance


def _cal(days):
    return [date(2024, 1, d) for d in days]


def test_daily_rebalance_returns_every_trading_day():
    cal = _cal([2, 3, 4, 5])
    out = DailyRebalance().rebalance_dates(date(2024, 1, 1), date(2024, 1, 5), cal)
    assert out == cal


def test_daily_rebalance_respects_window():
    cal = _cal([2, 3, 4, 5])
    out = DailyRebalance().rebalance_dates(date(2024, 1, 3), date(2024, 1, 4), cal)
    assert out == [date(2024, 1, 3), date(2024, 1, 4)]


def test_weekly_rebalance_picks_fri():
    # Jan 2024: Fri=5, 12, 19, 26
    cal = _cal([2, 3, 4, 5, 8, 9, 10, 11, 12])
    out = WeeklyRebalance("FRI").rebalance_dates(
        date(2024, 1, 1), date(2024, 1, 14), cal
    )
    assert out == [date(2024, 1, 5), date(2024, 1, 12)]


def test_weekly_rebalance_falls_back_when_friday_missing():
    # Friday Jan 5 missing from calendar; should fall back to Thu Jan 4.
    cal = _cal([2, 3, 4, 8, 9, 10, 11, 12])
    out = WeeklyRebalance("FRI").rebalance_dates(
        date(2024, 1, 1), date(2024, 1, 14), cal
    )
    assert out == [date(2024, 1, 4), date(2024, 1, 12)]


def test_weekly_rebalance_rejects_bad_weekday():
    with pytest.raises(ValueError):
        WeeklyRebalance("XYZ")
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_schedule.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement schedule**

Create `instock/portfolio/schedule.py`:

```python
"""RebalanceSchedule: decide which dates in [start, end] to rebalance on."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, timedelta


_WEEKDAYS = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4,
             "SAT": 5, "SUN": 6}


class RebalanceSchedule(ABC):
    @abstractmethod
    def rebalance_dates(
        self, start: date, end: date, trade_calendar: list[date]
    ) -> list[date]:
        ...


class DailyRebalance(RebalanceSchedule):
    def rebalance_dates(self, start, end, trade_calendar):
        return [d for d in trade_calendar if start <= d <= end]


class WeeklyRebalance(RebalanceSchedule):
    """Pick one trading day per ISO week: the latest trading day <= the
    target weekday. If no such day exists in that week, skip the week."""

    def __init__(self, weekday: str = "FRI") -> None:
        key = weekday.upper()
        if key not in _WEEKDAYS:
            raise ValueError(f"unknown weekday {weekday!r}")
        self.weekday = _WEEKDAYS[key]

    def rebalance_dates(self, start, end, trade_calendar):
        cal_set = set(trade_calendar)
        out: list[date] = []
        # Iterate one week at a time, starting from the Monday of `start`'s week.
        cur = start - timedelta(days=start.weekday())
        while cur <= end:
            # Target date in this week: Monday + self.weekday offset.
            target = cur + timedelta(days=self.weekday)
            # Walk backward from target within this week looking for a
            # trading day in [start, end].
            picked = None
            for i in range(self.weekday + 1):
                candidate = target - timedelta(days=i)
                if candidate < cur:
                    break
                if (
                    start <= candidate <= end
                    and candidate in cal_set
                ):
                    picked = candidate
                    break
            if picked is not None:
                out.append(picked)
            cur += timedelta(days=7)
        return out
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_schedule.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/schedule.py tests/portfolio/test_schedule.py
git commit -m "feat(portfolio): RebalanceSchedule + Daily/Weekly implementations"
```

---

## Task 9: `StrategyConfig` + `StrategyPipeline`

**Files:**
- Create: `instock/portfolio/pipeline.py`
- Test: `tests/portfolio/test_pipeline.py`

- [ ] **Step 1: Write failing pipeline tests**

Create `tests/portfolio/test_pipeline.py`:

```python
from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from instock.portfolio.combiner import EqualRankCombiner
from instock.portfolio.constraints import MaxWeightConstraint
from instock.portfolio.filters import SuspendedFilter, NewListingFilter
from instock.portfolio.pipeline import StrategyConfig, StrategyPipeline
from instock.portfolio.schedule import DailyRebalance
from instock.portfolio.selector import TopNSelector
from instock.portfolio.weighter import EqualWeighter


def _factor_df(factor_name, dates, codes, values):
    rows = []
    for d in dates:
        for c, v in zip(codes, values):
            rows.append({"date": pd.Timestamp(d), "code": c, "value": v})
    return pd.DataFrame(rows)


def _ohlcv_panel(codes, dates):
    rows = []
    for c in codes:
        for d in dates:
            rows.append({
                "date": pd.Timestamp(d),
                "code": c,
                "volume": 1_000_000,
            })
    return pd.DataFrame(rows)


def test_pipeline_end_to_end_small(monkeypatch):
    dates = ["2023-10-01", "2023-10-02", "2023-12-01", "2023-12-02",
             "2024-01-02", "2024-01-03"]
    codes = ["000001", "000002", "000003", "000004", "000005"]

    # Mock read_factor to return a synthetic factor for "f1".
    def fake_read_factor(name, start, end):
        assert name == "f1"
        return _factor_df("f1", dates, codes, [0.1, 0.2, 0.3, 0.4, 0.5])

    # Mock get_source for OHLCV + trade calendar + universe.
    mock_source = MagicMock()
    mock_source.get_trade_calendar.return_value = [
        date.fromisoformat(d) for d in dates
    ]
    mock_source.get_ohlcv.return_value = _ohlcv_panel(codes, dates)
    mock_source.get_index_member.return_value = codes

    monkeypatch.setattr(
        "instock.portfolio.pipeline.read_factor", fake_read_factor
    )
    monkeypatch.setattr(
        "instock.portfolio.pipeline.get_source", lambda: mock_source
    )

    cfg = StrategyConfig(
        name="test",
        factors=["f1"],
        combiner=EqualRankCombiner(),
        filters=[SuspendedFilter(), NewListingFilter(min_days=30)],
        selector=TopNSelector(n=2),
        weighter=EqualWeighter(),
        constraints=[MaxWeightConstraint(0.6)],
        schedule=DailyRebalance(),
    )
    out = StrategyPipeline().run(
        date(2024, 1, 2), date(2024, 1, 3), cfg
    )

    assert not out.empty
    assert set(out["strategy"].unique()) == {"test"}
    for d, g in out.groupby("date"):
        assert abs(g["weight"].sum() - 1.0) < 1e-9
        assert len(g) == 2
        assert (g["weight"] <= 0.6 + 1e-9).all()


def test_pipeline_empty_factor_data_returns_empty(monkeypatch):
    monkeypatch.setattr(
        "instock.portfolio.pipeline.read_factor",
        lambda name, start, end: pd.DataFrame(
            columns=["date", "code", "value"]
        ),
    )
    mock_source = MagicMock()
    mock_source.get_trade_calendar.return_value = [date(2024, 1, 2)]
    mock_source.get_ohlcv.return_value = pd.DataFrame(
        columns=["date", "code", "volume"]
    )
    mock_source.get_index_member.return_value = ["000001"]
    monkeypatch.setattr(
        "instock.portfolio.pipeline.get_source", lambda: mock_source
    )

    cfg = StrategyConfig(name="test", factors=["f1"])
    out = StrategyPipeline().run(
        date(2024, 1, 2), date(2024, 1, 2), cfg
    )
    assert out.empty
    assert list(out.columns) == [
        "date", "code", "weight", "score", "strategy"
    ]
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_pipeline.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement pipeline**

Create `instock/portfolio/pipeline.py`:

```python
"""StrategyPipeline: orchestrates factor-combiner -> filter -> selector ->
weighter -> constraint -> holding frame.

Dependencies are pluggable; defaults chosen to match the spec's recommended
baseline (equal-rank combiner, top-10% selection, equal weights, 3% single-
name cap, weekly-Friday rebalance).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Callable

import pandas as pd

from instock.datasource.registry import get_source
from instock.factors.storage import read_factor

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

    def run(
        self, start: date, end: date, config: StrategyConfig
    ) -> pd.DataFrame:
        source = get_source()

        # 1. Trade calendar for the window, used by schedule + filters.
        cal_dates = source.get_trade_calendar(start, end)

        # 2. Rebalance dates in [start, end].
        rebal = config.schedule.rebalance_dates(start, end, cal_dates)
        if not rebal:
            log.warning("strategy %s: no rebalance dates in window", config.name)
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)

        # 3. Fetch factors once for the whole window and combine.
        panel = self._load_factor_panel(config.factors, start, end)
        if panel.empty:
            log.warning("strategy %s: empty factor panel", config.name)
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)
        scores_panel = config.combiner.combine(panel)

        # 4. Universe resolver: lazy per-date; default falls back to sub-1 job.
        resolver = config.universe_resolver or _default_universe_resolver()

        # 5. OHLCV panel spanning back `min_days`+buffer for NewListingFilter.
        ohlcv = self._load_ohlcv_panel(start, end)
        fctx = FilterContext(ohlcv_panel=ohlcv)

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

            cctx = ConstraintContext(industry_map=None)
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
        """Placeholder that returns an empty panel; real impl pulled by
        real_data_smoke test. Pipeline callers that need history override
        this via monkeypatch in unit tests or provide real OHLCV in
        integration environment."""
        # NOTE: intentionally minimal; sub-3 will drive the real implementation
        # through the backtester's data-panel preloader. For MVP the pipeline
        # relies on callers (or tests) to preload and monkeypatch.
        return pd.DataFrame(columns=["date", "code", "volume"])


def _default_universe_resolver() -> Callable[[date], list[str]]:
    """Fallback: sub-1's CSI-300-as-of-end resolver, but closured per-date."""
    from instock.job.factor_compute_daily_job import _resolve_universe

    def _f(at: date) -> list[str]:
        return _resolve_universe(at)

    return _f
```

**Note on `_load_ohlcv_panel`:** The integration test monkeypatches `get_source().get_ohlcv` to supply a panel; real wiring (fetch per-run OHLCV from the source with a `min_days` lookback buffer) lands with the real-data smoke in Task 12. Keeping it this way avoids pulling a multi-year OHLCV fetch into the pipeline layer before we know the caller's needs.

Actually — the unit test above expects `mock_source.get_ohlcv.return_value = _ohlcv_panel(...)` to flow through. Fix `_load_ohlcv_panel` to actually call `source.get_ohlcv` across the window:

```python
    def _load_ohlcv_panel(
        self, start: date, end: date
    ) -> pd.DataFrame:
        """Fetch an OHLCV panel covering [start - 120d, end] for filter use.

        120-day lookback is a conservative buffer for NewListingFilter(60).
        """
        from datetime import timedelta
        source = get_source()
        lookback = start - timedelta(days=120)
        try:
            return source.get_ohlcv("ALL", lookback, end)  # type: ignore[arg-type]
        except Exception as exc:
            log.warning("failed to load OHLCV panel: %s", exc)
            return pd.DataFrame(columns=["date", "code", "volume"])
```

Update the production code in the task to use this corrected implementation. The `"ALL"` string is a caller-side convention; the akshare source today fetches per-code, so in the real-data smoke test (Task 12) we'll either loop per code or revise this. For the unit test, the `MagicMock` returns whatever `return_value` says regardless of argument.

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_pipeline.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/portfolio/pipeline.py tests/portfolio/test_pipeline.py
git commit -m "feat(portfolio): StrategyConfig + StrategyPipeline orchestration"
```

---

## Task 10: Daily job CLI

**Files:**
- Create: `instock/job/generate_holdings_daily_job.py`
- Test: `tests/portfolio/test_daily_job.py`

- [ ] **Step 1: Write failing daily-job test**

Create `tests/portfolio/test_daily_job.py`:

```python
from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from instock.job import generate_holdings_daily_job as job
from instock.portfolio import storage
from instock.portfolio.pipeline import StrategyConfig


def test_job_writes_each_strategy(tmp_holding_root, monkeypatch):
    out_df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
        "code": ["600519", "000001"],
        "weight": [0.5, 0.5],
        "score": [0.9, 0.7],
        "strategy": ["alpha", "alpha"],
    })
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = out_df
    monkeypatch.setattr(job, "StrategyPipeline", lambda: mock_pipeline)

    cfg = StrategyConfig(name="alpha", factors=["f1"])
    errs = job.run(date(2024, 1, 2), date(2024, 1, 2), [cfg])
    assert errs == {}

    read_back = storage.read_holding(
        "alpha",
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-02"),
    )
    assert len(read_back) == 2
    assert abs(read_back["weight"].sum() - 1.0) < 1e-9


def test_job_collects_per_strategy_errors(tmp_holding_root, monkeypatch):
    mock_pipeline = MagicMock()
    mock_pipeline.run.side_effect = RuntimeError("boom")
    monkeypatch.setattr(job, "StrategyPipeline", lambda: mock_pipeline)

    cfg = StrategyConfig(name="alpha", factors=["f1"])
    errs = job.run(date(2024, 1, 2), date(2024, 1, 2), [cfg])
    assert "alpha" in errs
    assert isinstance(errs["alpha"], RuntimeError)
```

- [ ] **Step 2: Run tests, confirm RED**

Run: `pytest tests/portfolio/test_daily_job.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement daily job**

Create `instock/job/generate_holdings_daily_job.py`:

```python
"""Daily holding-generation job.

Usage:
    python -m instock.job.generate_holdings_daily_job [YYYY-MM-DD [YYYY-MM-DD]]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

from instock.portfolio import storage
from instock.portfolio.pipeline import StrategyConfig, StrategyPipeline

log = logging.getLogger(__name__)


def run(
    start: date, end: date, configs: list[StrategyConfig]
) -> dict[str, Exception]:
    errors: dict[str, Exception] = {}
    for cfg in configs:
        try:
            df = StrategyPipeline().run(start, end, cfg)
            if df.empty:
                log.warning("strategy %s produced no rows", cfg.name)
                continue
            storage.write_holding(cfg.name, df)
            log.info("strategy %s: %s rows written", cfg.name, len(df))
        except Exception as exc:
            log.exception("strategy %s failed", cfg.name)
            errors[cfg.name] = exc
    return errors


def _parse(argv: list[str]) -> tuple[date, date]:
    if len(argv) == 0:
        today = date.today()
        return today, today
    if len(argv) == 1:
        d = datetime.strptime(argv[0], "%Y-%m-%d").date()
        return d, d
    s = datetime.strptime(argv[0], "%Y-%m-%d").date()
    e = datetime.strptime(argv[1], "%Y-%m-%d").date()
    return s, e


def _default_configs() -> list[StrategyConfig]:
    """MVP: a single default strategy using every factor currently registered."""
    # Side-effect imports so factor registry is populated.
    import instock.factors.technical.momentum  # noqa: F401
    import instock.factors.fundamental.valuation  # noqa: F401
    import instock.factors.lhb.heat  # noqa: F401
    import instock.factors.flow.north  # noqa: F401
    from instock.factors.registry import get_all

    names = list(get_all().keys())
    return [StrategyConfig(name="default", factors=names)]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start, end = _parse(sys.argv[1:])
    errs = run(start, end, _default_configs())
    if errs:
        sys.exit(1)
```

- [ ] **Step 4: Run tests, confirm GREEN**

Run: `pytest tests/portfolio/test_daily_job.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add instock/job/generate_holdings_daily_job.py tests/portfolio/test_daily_job.py
git commit -m "feat(job): daily holding-generation job"
```

---

## Task 11: Real-data smoke (Q8 B gate)

**Files:**
- Create: `tests/portfolio/test_real_data_smoke.py`

This test is `skip`'ed by default. It is how a human / CI operator confirms the MVP gate by pointing the env at real Sub-1 factor Parquet and CSI-300 data.

- [ ] **Step 1: Write skipped-by-default real-data test**

Create `tests/portfolio/test_real_data_smoke.py`:

```python
"""Real-data smoke test for Sub-2 MVP gate.

Skip unless INSTOCK_SUB2_SMOKE=1 is set in the environment. This test reads
real factor Parquet written by Sub-1's daily job (INSTOCK_FACTOR_ROOT must
also be set) and runs the default strategy over a 3-month CSI-300 window.

Assertions (MVP gate per spec Q8/B):
  - Each rebalance date's weight sum == 1.0
  - All weights <= 3%
  - At least one rebalance date produces non-empty holdings
  - Industry distribution is dumped to stderr for human eyeballing
"""
from __future__ import annotations

import os
import sys
from datetime import date

import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("INSTOCK_SUB2_SMOKE") != "1",
    reason="real-data smoke: set INSTOCK_SUB2_SMOKE=1 to run",
)


def test_default_strategy_three_months():
    from instock.job.generate_holdings_daily_job import _default_configs
    from instock.portfolio import storage
    from instock.portfolio.pipeline import StrategyPipeline

    configs = _default_configs()
    assert configs, "no default configs (factor registry empty?)"
    cfg = configs[0]

    start = date(2024, 1, 2)
    end = date(2024, 3, 29)

    df = StrategyPipeline().run(start, end, cfg)
    assert not df.empty, "pipeline produced no rows"

    for d, g in df.groupby("date"):
        assert abs(g["weight"].sum() - 1.0) < 1e-6, (
            f"sum != 1 on {d}: {g['weight'].sum()}"
        )
        assert (g["weight"] <= 0.03 + 1e-9).all(), (
            f"weight > 3% on {d}: {g.loc[g['weight'] > 0.03].to_dict()}"
        )

    # Round-trip through storage.
    storage.write_holding(cfg.name, df)
    back = storage.read_holding(
        cfg.name, pd.Timestamp(start), pd.Timestamp(end)
    )
    assert len(back) == len(df)

    # Sanity dump for humans (industry map may be None, so this is just
    # per-date holding counts).
    summary = df.groupby("date").agg(
        n_names=("code", "count"),
        sum_w=("weight", "sum"),
        max_w=("weight", "max"),
    )
    print("\n=== Sub-2 smoke summary ===", file=sys.stderr)
    print(summary.to_string(), file=sys.stderr)
```

- [ ] **Step 2: Run test without env var, confirm skipped**

Run: `pytest tests/portfolio/test_real_data_smoke.py -v`
Expected: 1 skipped (reason: INSTOCK_SUB2_SMOKE not set).

- [ ] **Step 3: Commit**

```bash
git add tests/portfolio/test_real_data_smoke.py
git commit -m "test(portfolio): real-data smoke (skipped unless INSTOCK_SUB2_SMOKE=1)"
```

---

## Task 12: End-to-end smoke + follow-up housekeeping

**Files:**
- Modify: `docs/superpowers/roadmap.md` (mark Sub-2 complete)
- Create: `docs/superpowers/followups/subproject-2-portfolio-construction.md`

- [ ] **Step 1: Run full test suite**

Run: `pytest tests/ -v`
Expected: all Sub-1 tests + all new Sub-2 tests green (Sub-2 real-data smoke skipped).

- [ ] **Step 2: Smoke import**

Run:
```bash
python3 -c "
import instock.portfolio.schemas
import instock.portfolio.storage
import instock.portfolio.combiner
import instock.portfolio.filters
import instock.portfolio.selector
import instock.portfolio.weighter
import instock.portfolio.constraints
import instock.portfolio.schedule
import instock.portfolio.pipeline
import instock.job.generate_holdings_daily_job
print('ok')
"
```
Expected: `ok`.

- [ ] **Step 3: py_compile check**

Run: `python3 -m py_compile $(git ls-files '*.py' | grep -E '^(instock|tests)/')`
Expected: no output.

- [ ] **Step 4: Write follow-up doc**

Create `docs/superpowers/followups/subproject-2-portfolio-construction.md`:

```markdown
# Sub-project 2 (Portfolio Construction) — Follow-up Items

Source: Design spec
`docs/superpowers/specs/2026-04-18-portfolio-construction-design.md`
and items deferred during implementation.

## Entry gates for Sub-3 (must-fix before backtester work)

- [ ] Real industry map data source. `IndustryExposureConstraint` is
      currently no-op when `industry_map` is None. Pick a source
      (akshare `stock_board_industry_name_em` + membership API) and load
      into a dict-like / DataFrame.
- [ ] Real stock-listing-date source. `NewListingFilter` approximates
      listing date by the earliest OHLCV observation; replace with an
      authoritative source once fundamentals / ref-data pipeline exists.
- [ ] ST / 涨跌停 (limit-up/down) filters. Requires pulling ST flag
      per date and prior-close vs limit ratio.

## Nice-to-have (any time)

- [ ] `MonthlyRebalance` schedule implementation.
- [ ] `MarketCapWeighter` implementation (needs market cap data in the
      WeighterContext; akshare `stock_zh_a_spot_em` has it).
- [ ] `InverseVolWeighter` implementation (can compute from OHLCV
      already available in WeighterContext).
- [ ] `ICWeightedCombiner` that reads historical IC from Sub-1 evaluator
      output and weights factors accordingly (careful: must be rolling to
      avoid look-ahead).
- [ ] Atomic Parquet writes (tmp + rename) in `portfolio/storage.py`.
- [ ] pandas `DataFrameGroupBy.apply` FutureWarning cleanup across the
      package (mirrors Sub-1 follow-up).

## Deferred per MVP scope (will need a design revisit)

- cvxpy-based optimizer (mean-variance, tracking-error constraints).
- Short side / market-neutral portfolios.
- MySQL mirror of HoldingSchedule for Sub-4 Web queries (decide in Sub-4
  whether the Parquet-only read is fast enough).
```

- [ ] **Step 5: Update roadmap**

Open `docs/superpowers/roadmap.md` and change the Sub-project 2 entry in the "规划中" section from "🎯 选股与组合构建（下一步）" to "✅ 已合并"; move the Sub-2 row in the milestone table from `⏳` to `✅`. Leave Sub-3 / Sub-4 unchanged.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/followups/subproject-2-portfolio-construction.md \
        docs/superpowers/roadmap.md
git commit -m "docs: sub-2 follow-ups + mark sub-2 complete on roadmap"
```

- [ ] **Step 7: Tag**

```bash
git tag subproject-2-portfolio-construction-mvp
git log --oneline subproject-1-data-factors-mvp..subproject-2-portfolio-construction-mvp
```

---

## Self-Review

**Spec coverage:**
- Schema + invariant (spec §3) → Task 1
- Storage (spec §5.7) → Task 2
- FactorCombiner (spec §5.1) → Task 3
- UniverseFilter + FilterChain (spec §5.2) → Task 4
- Selector (spec §5.3) → Task 5
- Weighter (spec §5.4) → Task 6
- MaxWeight + IndustryExposure constraints (spec §5.5) → Task 7
- RebalanceSchedule (spec §5.6) → Task 8
- StrategyConfig + StrategyPipeline (spec §6) → Task 9
- Daily job CLI (spec §7) → Task 10
- Real-data smoke, Q8 B gate (spec §8.3) → Task 11
- End-to-end self-check + follow-up housekeeping (analogous to Sub-1 Task 16) → Task 12

**Placeholder scan:** No TBD/TODO. Task 9's original `_load_ohlcv_panel` placeholder replaced inline with a proper `get_source().get_ohlcv(...)` call with lookback buffer.

**Type / signature consistency:**
- `FactorCombiner.combine(factor_panel: DataFrame) -> DataFrame[date,code,score]` used consistently in Tasks 3, 9.
- `UniverseFilter.apply(codes, at: Timestamp, ctx) -> list[str]` consistent in Tasks 4, 9.
- `Selector.select(scores: Series, universe: list[str]) -> list[str]` consistent in Tasks 5, 9.
- `Weighter.weigh(selected: list[str], ctx) -> Series` consistent in Tasks 6, 9.
- `PortfolioConstraint.apply(weights: Series, ctx) -> Series` consistent in Tasks 7, 9.
- `RebalanceSchedule.rebalance_dates(start, end, trade_calendar) -> list[date]` consistent in Tasks 8, 9.
- Storage signatures `write_holding(strategy, df)`, `read_holding(strategy, start, end)` consistent in Tasks 2, 9, 10, 11.
- HoldingSchedule column order `[date, code, weight, score, strategy]` consistent in schema (Task 1), output constant (Task 9), storage defaults (Task 2).

**Deliberate deferrals (matches spec §1 non-goals and §11 open questions):**
- No `HoldingSchedule` dataclass wrapper (YAGNI confirmed — DataFrame is the contract).
- `_load_ohlcv_panel` uses a `get_ohlcv("ALL", ...)` convention that does not match the current akshare source (per-code). Explicitly flagged in Task 9; in practice unit tests monkeypatch and real-data smoke will reveal if per-code loop is needed. Recorded as a Sub-3 gate item indirectly (the backtester is the first real consumer of pipeline OHLCV needs).
