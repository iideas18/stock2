# Sub-project 3 — Backtest Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an event-driven daily backtester that consumes Sub-2 `HoldingSchedule` Parquet, executes T+1 trades against A-share OHLCV with realistic costs/constraints, and produces reproducible NAV/trades/metrics/HTML reports — with walk-forward IS/OOS diagnostics.

**Architecture:** A Portfolio state machine (cash + positions) is driven forward day-by-day by a BacktestEngine that (a) marks-to-market on close, (b) emits orders at each rebalance date from `HoldingSchedule`, (c) executes pending orders at next-day open through a FeeModel + SlippageModel + TradeConstraint chain, and (d) writes trades/positions/NAV to Parquet. Walk-forward wraps the engine to produce IS/OOS-spliced NAV. Reproducibility is enforced by a fingerprint SHA over all inputs + config + RNG seed.

**Tech Stack:** Python 3.12 (conda env `base`), pandas, pandera (`pandera.pandas as pa`), pyarrow/Parquet, Jinja2 (report), matplotlib (chart rendering to base64 PNG, reused from Sub-1 evaluator pattern), pytest.

**Running tests:** Always `conda run -n base python -m pytest <path> -v`. Assume `conda run` prefix throughout.

**Spec reference:** `docs/superpowers/specs/2026-04-18-backtest-engine-design.md` (15 sections, read it before starting).

---

## File Structure

All files under `instock/backtest/` unless otherwise noted. Tests mirror at `tests/backtest/`.

| File | Responsibility |
|------|---------------|
| `instock/backtest/__init__.py` | Package export surface: `BacktestEngine`, `BacktestConfig`, `run_backtest`. |
| `instock/backtest/schemas.py` | Pandera schemas: `TRADE_SCHEMA`, `POSITION_SCHEMA`, `NAV_SCHEMA`, `METRICS_SCHEMA`. Enum-like literal columns. |
| `instock/backtest/config.py` | `BacktestConfig` dataclass: fee/slippage params, benchmarks, lot_size, rng_seed. |
| `instock/backtest/costs.py` | `FeeModel` ABC + `StandardFeeModel` (0.025% / ¥5 min, 0.05% stamp sell-only, 0.001% transfer). `SlippageModel` ABC + `BpsSlippage` + `ZeroSlippage`. |
| `instock/backtest/constraints.py` | `TradeConstraint` ABC + `SuspendedConstraint`, `LimitUpConstraint`, `LimitDownConstraint`, `DelistConstraint`, `ConstraintChain`. |
| `instock/backtest/portfolio_state.py` | `PortfolioState`: cash, positions (code -> shares, avg_cost), `mark_to_market`, `apply_trade`, `snapshot`. |
| `instock/backtest/execution.py` | `Executor.execute(order, row, fee, slip)` → produces `TradeRecord` dict; handles lot-size rounding and cash residual. |
| `instock/backtest/engine.py` | `BacktestEngine.run(holding_schedule, ohlcv_store, config, start, end)` → event loop, returns dict of DataFrames. |
| `instock/backtest/metrics.py` | `compute_metrics(nav, trades, benchmarks)` → `MetricsSummary` dict. |
| `instock/backtest/benchmarks.py` | `load_benchmarks(source, codes, start, end)` — returns DataFrame indexed by date with one column per benchmark. |
| `instock/backtest/walkforward.py` | `WalkForwardConfig` + `WalkForwardRunner.run(...)` → splices IS/OOS NAV. |
| `instock/backtest/storage.py` | Parquet writers/readers: `write_run(run_id, trades, positions, nav)`, `read_run(run_id)`, `append_metrics(row)`. |
| `instock/backtest/fingerprint.py` | `compute_fingerprint(holding_schedule_path, ohlcv_files, refdata_files, config_dict, rng_seed) -> str`. |
| `instock/backtest/report.py` | `render_report(run_id, nav, trades, metrics, benchmarks, refdata_as_of) -> str (HTML)`. |
| `instock/backtest/template.html` | Jinja2 template (clone Sub-1 style). |
| `instock/job/backtest_run_job.py` | CLI entry: argparse → BacktestEngine → write → report. |
| `tests/backtest/*` | Mirror one test file per module; `test_real_data_smoke.py` gated by `INSTOCK_SUB3_SMOKE=1`. |

---

## Task Index

| # | Task | Deps |
|---|------|------|
| 1 | Package skeleton + schemas | — |
| 2 | `BacktestConfig` dataclass | 1 |
| 3 | FeeModel (standard + zero) | 1, 2 |
| 4 | SlippageModel (bps + zero) | 1, 2 |
| 5 | TradeConstraint chain | 1, 2 |
| 6 | PortfolioState | 1 |
| 7 | Executor (lot-size + cash residual) | 3, 4, 5, 6 |
| 8 | BacktestEngine event loop | 1–7 |
| 9 | Storage (trades/positions/nav Parquet) | 1 |
| 10 | Benchmarks loader | 1 |
| 11 | Metrics (sharpe, dd, calmar, turnover, alpha/beta/IR) | 9, 10 |
| 12 | Fingerprint + config SHA | 2, 9 |
| 13 | WalkForward runner | 8, 9, 11 |
| 14 | HTML report (template + render) | 9, 10, 11 |
| 15 | CLI job + default_configs hookup | 8–14 |
| 16 | Integration: replace Sub-2 `_load_ohlcv_panel("ALL", ...)` | 8 |
| 17 | Real-data smoke test | 15 |
| 18 | Roadmap + followups doc | 17 |

---
### Task 1: Package skeleton + schemas

**Files:**
- Create: `instock/backtest/__init__.py`
- Create: `instock/backtest/schemas.py`
- Create: `tests/backtest/__init__.py`
- Test: `tests/backtest/test_schemas.py`

- [ ] **Step 1: Write failing tests for schemas**

File `tests/backtest/test_schemas.py`:

```python
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
import pytest

from instock.backtest.schemas import (
    TRADE_SCHEMA,
    POSITION_SCHEMA,
    NAV_SCHEMA,
    METRICS_SCHEMA,
    BacktestValidationError,
)


def _valid_trade_row() -> dict:
    return {
        "date": pd.Timestamp("2023-01-03"),
        "code": "600000",
        "side": "BUY",
        "target_w": 0.01,
        "filled_shares": 100,
        "fill_price": 10.0,
        "fill_value": 1000.0,
        "commission": 5.0,
        "stamp_tax": 0.0,
        "transfer_fee": 0.01,
        "slippage_value": 0.5,
        "gross_value": 1000.5,
        "net_cash_change": -1005.51,
        "reason": "REBALANCE",
    }


def test_trade_schema_valid():
    df = pd.DataFrame([_valid_trade_row()])
    TRADE_SCHEMA.validate(df)


def test_trade_schema_rejects_bad_side():
    row = _valid_trade_row()
    row["side"] = "HOLD"
    df = pd.DataFrame([row])
    with pytest.raises(pa.errors.SchemaError):
        TRADE_SCHEMA.validate(df)


def test_trade_schema_rejects_bad_code():
    row = _valid_trade_row()
    row["code"] = "60000"  # 5 digits
    df = pd.DataFrame([row])
    with pytest.raises(pa.errors.SchemaError):
        TRADE_SCHEMA.validate(df)


def test_position_schema_valid():
    df = pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"),
        "code": "600000",
        "shares": 100,
        "avg_cost": 10.0,
        "market_value": 1010.0,
        "unrealized_pnl": 10.0,
        "weight": 0.01,
    }])
    POSITION_SCHEMA.validate(df)


def test_nav_schema_valid():
    df = pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"),
        "nav": 1.0,
        "cash": 100000.0,
        "position_value": 0.0,
        "total_value": 100000.0,
        "ret_daily": 0.0,
        "ret_cum": 0.0,
        "turnover_daily": 0.0,
        "n_holdings": 0,
    }])
    NAV_SCHEMA.validate(df)


def test_metrics_schema_valid():
    df = pd.DataFrame([{
        "run_id": "x_2022-01-04_2023-12-29_abcd1234",
        "strategy": "x",
        "start": pd.Timestamp("2022-01-04"),
        "end": pd.Timestamp("2023-12-29"),
        "ret_annual": 0.1,
        "ret_total": 0.2,
        "vol_annual": 0.15,
        "sharpe": 0.7,
        "sortino": 0.9,
        "max_drawdown": -0.1,
        "max_dd_duration_days": 30,
        "calmar": 1.0,
        "win_rate_daily": 0.52,
        "win_rate_monthly": 0.6,
        "turnover_annual": 2.0,
        "total_cost_bps": 30.0,
        "lot_drag_bps": 2.0,
        "fingerprint_sha": "abcd" * 16,
        "refdata_as_of": "2026-04-18",
    }])
    METRICS_SCHEMA.validate(df)
```

- [ ] **Step 2: Run test (expect FAIL: module missing)**

```
conda run -n base python -m pytest tests/backtest/test_schemas.py -v
```
Expected: `ModuleNotFoundError: No module named 'instock.backtest'`.

- [ ] **Step 3: Create package + schemas**

File `tests/backtest/__init__.py`: empty.

File `instock/backtest/__init__.py`:

```python
"""Backtest engine for Sub-2 HoldingSchedule.

Public entry points:
    BacktestEngine, BacktestConfig, run_backtest
"""
from __future__ import annotations
```

File `instock/backtest/schemas.py`:

```python
"""Pandera schemas for backtest outputs.

All DataFrames use pandas dtypes; dates are datetime64[ns]; codes are
6-digit strings; enum columns enforced with str_matches.
"""
from __future__ import annotations

import pandera.pandas as pa


class BacktestValidationError(Exception):
    """Raised when a backtest DataFrame violates its schema contract."""


_CODE_RE = r"^\d{6}$"
_SIDE_RE = r"^(BUY|SELL)$"
_REASON_RE = r"^(REBALANCE|ST_FORCE_OUT|DELIST_FORCE_OUT|NO_OP)$"


TRADE_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "code": pa.Column(str, pa.Check.str_matches(_CODE_RE)),
        "side": pa.Column(str, pa.Check.str_matches(_SIDE_RE)),
        "target_w": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
        "filled_shares": pa.Column(int),
        "fill_price": pa.Column(float, pa.Check.ge(0.0)),
        "fill_value": pa.Column(float, pa.Check.ge(0.0)),
        "commission": pa.Column(float, pa.Check.ge(0.0)),
        "stamp_tax": pa.Column(float, pa.Check.ge(0.0)),
        "transfer_fee": pa.Column(float, pa.Check.ge(0.0)),
        "slippage_value": pa.Column(float),
        "gross_value": pa.Column(float),
        "net_cash_change": pa.Column(float),
        "reason": pa.Column(str, pa.Check.str_matches(_REASON_RE)),
    },
    coerce=True,
    strict=False,
)


POSITION_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "code": pa.Column(str, pa.Check.str_matches(_CODE_RE)),
        "shares": pa.Column(int, pa.Check.ge(0)),
        "avg_cost": pa.Column(float, pa.Check.ge(0.0)),
        "market_value": pa.Column(float, pa.Check.ge(0.0)),
        "unrealized_pnl": pa.Column(float),
        "weight": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
    },
    coerce=True,
    strict=False,
)


NAV_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "nav": pa.Column(float, pa.Check.gt(0.0)),
        "cash": pa.Column(float),
        "position_value": pa.Column(float, pa.Check.ge(0.0)),
        "total_value": pa.Column(float, pa.Check.gt(0.0)),
        "ret_daily": pa.Column(float),
        "ret_cum": pa.Column(float),
        "turnover_daily": pa.Column(float, pa.Check.ge(0.0)),
        "n_holdings": pa.Column(int, pa.Check.ge(0)),
    },
    coerce=True,
    strict=False,
)


METRICS_SCHEMA = pa.DataFrameSchema(
    {
        "run_id": pa.Column(str, pa.Check.str_length(min_value=1)),
        "strategy": pa.Column(str, pa.Check.str_length(min_value=1)),
        "start": pa.Column("datetime64[ns]"),
        "end": pa.Column("datetime64[ns]"),
        "ret_annual": pa.Column(float),
        "ret_total": pa.Column(float),
        "vol_annual": pa.Column(float, pa.Check.ge(0.0)),
        "sharpe": pa.Column(float, nullable=True),
        "sortino": pa.Column(float, nullable=True),
        "max_drawdown": pa.Column(float, pa.Check.le(0.0)),
        "max_dd_duration_days": pa.Column(int, pa.Check.ge(0)),
        "calmar": pa.Column(float, nullable=True),
        "win_rate_daily": pa.Column(float, pa.Check.in_range(0.0, 1.0)),
        "win_rate_monthly": pa.Column(
            float, pa.Check.in_range(0.0, 1.0), nullable=True
        ),
        "turnover_annual": pa.Column(float, pa.Check.ge(0.0)),
        "total_cost_bps": pa.Column(float, pa.Check.ge(0.0)),
        "lot_drag_bps": pa.Column(float),
        "fingerprint_sha": pa.Column(str, pa.Check.str_length(min_value=8)),
        "refdata_as_of": pa.Column(str, nullable=True),
    },
    coerce=True,
    strict=False,
)
```

- [ ] **Step 4: Run tests (expect PASS)**

```
conda run -n base python -m pytest tests/backtest/test_schemas.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```
git add instock/backtest/__init__.py instock/backtest/schemas.py tests/backtest/__init__.py tests/backtest/test_schemas.py
git commit -m "feat(sub-3): add backtest package skeleton + output schemas"
```

---

### Task 2: BacktestConfig dataclass

**Files:**
- Create: `instock/backtest/config.py`
- Test: `tests/backtest/test_config.py`

- [ ] **Step 1: Write failing test**

File `tests/backtest/test_config.py`:

```python
from __future__ import annotations

import json
from datetime import date

from instock.backtest.config import BacktestConfig


def test_default_config():
    c = BacktestConfig(strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29))
    assert c.initial_capital == 1_000_000.0
    assert c.lot_size == 100
    assert c.slippage_bps == 5.0
    assert c.rng_seed == 42
    assert c.benchmarks == ("000300.SH", "000905.SH", "000852.SH")
    assert c.price_adjust == "qfq"
    assert c.enable_fees is True
    assert c.enable_slippage is True


def test_config_to_dict_deterministic():
    c = BacktestConfig(strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29))
    d = c.to_dict()
    s1 = json.dumps(d, sort_keys=True)
    s2 = json.dumps(c.to_dict(), sort_keys=True)
    assert s1 == s2
    assert d["strategy"] == "x"
    assert d["start"] == "2023-01-04"


def test_config_override_fees_off():
    c = BacktestConfig(
        strategy="x", start=date(2023, 1, 4), end=date(2023, 12, 29),
        enable_fees=False, enable_slippage=False,
    )
    assert c.enable_fees is False
    assert c.enable_slippage is False
```

- [ ] **Step 2: Run (expect FAIL: module missing)**

```
conda run -n base python -m pytest tests/backtest/test_config.py -v
```

- [ ] **Step 3: Implement**

File `instock/backtest/config.py`:

```python
"""BacktestConfig: single source of truth for run parameters.

Serialized into fingerprint input so config changes propagate to run_id.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Tuple


@dataclass(frozen=True)
class BacktestConfig:
    strategy: str
    start: date
    end: date

    initial_capital: float = 1_000_000.0
    lot_size: int = 100
    price_adjust: str = "qfq"

    # Fees
    enable_fees: bool = True
    commission_rate: float = 0.00025
    commission_min: float = 5.0
    stamp_tax_rate: float = 0.0005          # SELL only
    transfer_fee_rate: float = 0.00001

    # Slippage
    enable_slippage: bool = True
    slippage_bps: float = 5.0

    # Benchmarks (order preserved for report)
    benchmarks: Tuple[str, ...] = (
        "000300.SH", "000905.SH", "000852.SH"
    )

    # Reproducibility
    rng_seed: int = 42

    # Constraint tuning
    limit_tolerance: float = 1e-3
    max_suspended_defer_days: int = 5

    def to_dict(self) -> dict:
        d = asdict(self)
        d["start"] = self.start.isoformat()
        d["end"] = self.end.isoformat()
        d["benchmarks"] = list(self.benchmarks)
        return d
```

- [ ] **Step 4: Run (expect PASS)**

```
conda run -n base python -m pytest tests/backtest/test_config.py -v
```

- [ ] **Step 5: Commit**

```
git add instock/backtest/config.py tests/backtest/test_config.py
git commit -m "feat(sub-3): add BacktestConfig dataclass"
```

---

### Task 3: FeeModel

**Files:**
- Create: `instock/backtest/costs.py` (FeeModel half)
- Test: `tests/backtest/test_costs.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_costs.py`:

```python
from __future__ import annotations

import pytest

from instock.backtest.costs import (
    FeeModel, StandardFeeModel, ZeroFeeModel,
)


def test_standard_fee_buy_min_commission():
    fm = StandardFeeModel()
    # Small buy: commission floored at 5 yuan
    fees = fm.compute(value=1000.0, side="BUY")
    assert fees["commission"] == 5.0
    assert fees["stamp_tax"] == 0.0
    assert fees["transfer_fee"] == pytest.approx(1000.0 * 0.00001)


def test_standard_fee_buy_pct_dominates():
    fm = StandardFeeModel()
    fees = fm.compute(value=100_000.0, side="BUY")
    assert fees["commission"] == pytest.approx(100_000.0 * 0.00025)
    assert fees["stamp_tax"] == 0.0


def test_standard_fee_sell_stamp():
    fm = StandardFeeModel()
    fees = fm.compute(value=100_000.0, side="SELL")
    assert fees["stamp_tax"] == pytest.approx(100_000.0 * 0.0005)
    assert fees["commission"] == pytest.approx(100_000.0 * 0.00025)


def test_standard_fee_zero_value():
    fm = StandardFeeModel()
    fees = fm.compute(value=0.0, side="BUY")
    assert fees == {"commission": 5.0, "stamp_tax": 0.0, "transfer_fee": 0.0}


def test_zero_fee_returns_zero():
    fm = ZeroFeeModel()
    for side in ("BUY", "SELL"):
        fees = fm.compute(value=100_000.0, side=side)
        assert fees == {"commission": 0.0, "stamp_tax": 0.0, "transfer_fee": 0.0}


def test_fee_model_is_abc():
    assert issubclass(StandardFeeModel, FeeModel)
```

- [ ] **Step 2: Run (expect FAIL)**

```
conda run -n base python -m pytest tests/backtest/test_costs.py -v
```

- [ ] **Step 3: Implement**

File `instock/backtest/costs.py`:

```python
"""FeeModel + SlippageModel.

FeeModel: commission (min floored), A-share stamp tax (sell only),
transfer fee (simplified: applied all markets).

SlippageModel: bps proportional drift on fill price; side-signed.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class FeeModel(ABC):
    @abstractmethod
    def compute(self, value: float, side: str) -> dict:
        """Return {commission, stamp_tax, transfer_fee}, all >= 0."""


class StandardFeeModel(FeeModel):
    def __init__(
        self,
        commission_rate: float = 0.00025,
        commission_min: float = 5.0,
        stamp_tax_rate: float = 0.0005,
        transfer_fee_rate: float = 0.00001,
    ) -> None:
        self.commission_rate = commission_rate
        self.commission_min = commission_min
        self.stamp_tax_rate = stamp_tax_rate
        self.transfer_fee_rate = transfer_fee_rate

    def compute(self, value: float, side: str) -> dict:
        commission = max(value * self.commission_rate, self.commission_min)
        stamp_tax = value * self.stamp_tax_rate if side == "SELL" else 0.0
        transfer_fee = value * self.transfer_fee_rate
        return {
            "commission": float(commission),
            "stamp_tax": float(stamp_tax),
            "transfer_fee": float(transfer_fee),
        }


class ZeroFeeModel(FeeModel):
    def compute(self, value: float, side: str) -> dict:
        return {"commission": 0.0, "stamp_tax": 0.0, "transfer_fee": 0.0}
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/costs.py tests/backtest/test_costs.py
git commit -m "feat(sub-3): FeeModel (standard + zero)"
```

---

### Task 4: SlippageModel

**Files:**
- Modify: `instock/backtest/costs.py` (append)
- Modify: `tests/backtest/test_costs.py` (append)

- [ ] **Step 1: Append failing tests**

Append to `tests/backtest/test_costs.py`:

```python
from instock.backtest.costs import SlippageModel, BpsSlippage, ZeroSlippage


def test_bps_slippage_buy_positive():
    s = BpsSlippage(bps=5.0)
    fill = s.fill_price(open_price=10.0, side="BUY")
    assert fill == pytest.approx(10.0 * (1 + 5 * 1e-4))


def test_bps_slippage_sell_negative():
    s = BpsSlippage(bps=5.0)
    fill = s.fill_price(open_price=10.0, side="SELL")
    assert fill == pytest.approx(10.0 * (1 - 5 * 1e-4))


def test_bps_slippage_symmetric():
    s = BpsSlippage(bps=10.0)
    buy = s.fill_price(100.0, "BUY") - 100.0
    sell = 100.0 - s.fill_price(100.0, "SELL")
    assert buy == pytest.approx(sell)


def test_zero_slippage_returns_open():
    s = ZeroSlippage()
    assert s.fill_price(10.0, "BUY") == 10.0
    assert s.fill_price(10.0, "SELL") == 10.0


def test_slippage_model_is_abc():
    assert issubclass(BpsSlippage, SlippageModel)
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Append implementation**

Append to `instock/backtest/costs.py`:

```python
class SlippageModel(ABC):
    @abstractmethod
    def fill_price(self, open_price: float, side: str) -> float:
        """Return realized fill price given T+1 open price."""


class BpsSlippage(SlippageModel):
    def __init__(self, bps: float = 5.0) -> None:
        self.bps = bps

    def fill_price(self, open_price: float, side: str) -> float:
        sign = 1.0 if side == "BUY" else -1.0
        return float(open_price * (1.0 + sign * self.bps * 1e-4))


class ZeroSlippage(SlippageModel):
    def fill_price(self, open_price: float, side: str) -> float:
        return float(open_price)
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/costs.py tests/backtest/test_costs.py
git commit -m "feat(sub-3): SlippageModel (bps + zero)"
```

---

### Task 5: TradeConstraint chain

**Files:**
- Create: `instock/backtest/constraints.py`
- Test: `tests/backtest/test_constraints.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_constraints.py`:

```python
from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.constraints import (
    ConstraintContext,
    SuspendedConstraint,
    LimitUpConstraint,
    LimitDownConstraint,
    DelistConstraint,
    ConstraintChain,
)


def _ctx(row_today, row_yesterday, listing=None, delisted=False):
    return ConstraintContext(
        row_today=row_today,
        row_yesterday=row_yesterday,
        listing_dates=listing,
        delisted=delisted,
    )


def test_suspended_blocks_buy_when_volume_zero():
    today = {"open": 10.0, "close": 10.0, "volume": 0}
    ok, reason = SuspendedConstraint().check(
        side="BUY", row_today=today, row_yesterday={"close": 10.0},
    )
    assert ok is False
    assert reason == "SUSPENDED"


def test_suspended_blocks_sell_when_volume_zero():
    today = {"open": 10.0, "close": 10.0, "volume": 0}
    ok, reason = SuspendedConstraint().check(
        side="SELL", row_today=today, row_yesterday={"close": 10.0},
    )
    assert ok is False


def test_suspended_allows_when_volume_nonzero():
    today = {"open": 10.0, "close": 10.0, "volume": 100_000}
    ok, _ = SuspendedConstraint().check(
        side="BUY", row_today=today, row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_limit_up_blocks_buy_mainboard():
    # Prev close 10.0, today open 11.0 = +10% -> limit up
    ok, reason = LimitUpConstraint(tolerance=1e-3).check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 11.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is False
    assert reason == "LIMIT_UP"


def test_limit_up_allows_sell():
    ok, _ = LimitUpConstraint().check(
        code="600000",
        is_st=False,
        side="SELL",
        row_today={"open": 11.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_limit_up_chinext_20pct():
    # 300xxx has 20% cap; +10% is NOT blocked
    ok, _ = LimitUpConstraint().check(
        code="300001",
        is_st=False,
        side="BUY",
        row_today={"open": 11.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_limit_down_blocks_sell():
    ok, reason = LimitDownConstraint(tolerance=1e-3).check(
        code="600000",
        is_st=False,
        side="SELL",
        row_today={"open": 9.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is False
    assert reason == "LIMIT_DOWN"


def test_limit_down_allows_buy():
    ok, _ = LimitDownConstraint().check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 9.0, "volume": 100},
        row_yesterday={"close": 10.0},
    )
    assert ok is True


def test_delist_forces_sell_when_flag_set():
    ok, reason = DelistConstraint().check(
        side="SELL", delisted=True,
    )
    assert ok is True  # SELL is forced, not blocked
    assert reason == "DELIST_FORCE_OUT"


def test_delist_blocks_buy():
    ok, reason = DelistConstraint().check(
        side="BUY", delisted=True,
    )
    assert ok is False


def test_chain_short_circuits_on_first_block():
    chain = ConstraintChain([
        SuspendedConstraint(),
        LimitUpConstraint(),
    ])
    ok, reason = chain.check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 11.0, "volume": 0},
        row_yesterday={"close": 10.0},
        delisted=False,
    )
    assert ok is False
    assert reason == "SUSPENDED"  # first one wins


def test_chain_allows_when_all_pass():
    chain = ConstraintChain([SuspendedConstraint(), LimitUpConstraint()])
    ok, _ = chain.check(
        code="600000",
        is_st=False,
        side="BUY",
        row_today={"open": 10.5, "volume": 100_000},
        row_yesterday={"close": 10.0},
        delisted=False,
    )
    assert ok is True
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/constraints.py`:

```python
"""Trade constraints: gate each order before execution.

Rule: first constraint to block wins. Delist is "force sell"; treated as
allowed-for-SELL but with reason=DELIST_FORCE_OUT so downstream records it.
"""
from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional

from instock.portfolio.filters import default_thresholds


@dataclass
class ConstraintContext:
    """Per-order snapshot passed into each constraint."""
    row_today: dict
    row_yesterday: dict
    listing_dates: Optional[dict] = None
    delisted: bool = False


class TradeConstraint(ABC):
    pass


class SuspendedConstraint(TradeConstraint):
    """Block both sides if today's volume <= 0."""

    def check(self, side: str, row_today: dict, row_yesterday: dict):
        if row_today.get("volume", 0) <= 0:
            return False, "SUSPENDED"
        return True, None


class LimitUpConstraint(TradeConstraint):
    """Block BUYs if today's open >= yesterday_close * (1 + threshold - tol)."""

    def __init__(self, tolerance: float = 1e-3) -> None:
        self.tolerance = tolerance

    def check(
        self, code: str, is_st: bool, side: str,
        row_today: dict, row_yesterday: dict,
    ):
        if side != "BUY":
            return True, None
        yc = row_yesterday.get("close", 0.0)
        if yc <= 0:
            return True, None
        thr = default_thresholds(code, is_st)
        if row_today["open"] / yc - 1.0 >= thr - self.tolerance:
            return False, "LIMIT_UP"
        return True, None


class LimitDownConstraint(TradeConstraint):
    """Block SELLs if today's open <= yesterday_close * (1 - threshold + tol)."""

    def __init__(self, tolerance: float = 1e-3) -> None:
        self.tolerance = tolerance

    def check(
        self, code: str, is_st: bool, side: str,
        row_today: dict, row_yesterday: dict,
    ):
        if side != "SELL":
            return True, None
        yc = row_yesterday.get("close", 0.0)
        if yc <= 0:
            return True, None
        thr = default_thresholds(code, is_st)
        if row_today["open"] / yc - 1.0 <= -(thr - self.tolerance):
            return False, "LIMIT_DOWN"
        return True, None


class DelistConstraint(TradeConstraint):
    """If delisted: force SELL (reason=DELIST_FORCE_OUT), block BUY."""

    def check(self, side: str, delisted: bool):
        if not delisted:
            return True, None
        if side == "SELL":
            return True, "DELIST_FORCE_OUT"
        return False, "DELISTED_NO_BUY"


class ConstraintChain:
    """Apply constraints in order; first block wins."""

    def __init__(self, constraints: list) -> None:
        self.constraints = constraints

    def check(
        self, code: str, is_st: bool, side: str,
        row_today: dict, row_yesterday: dict,
        delisted: bool = False,
    ):
        for c in self.constraints:
            if isinstance(c, SuspendedConstraint):
                ok, reason = c.check(side, row_today, row_yesterday)
            elif isinstance(c, (LimitUpConstraint, LimitDownConstraint)):
                ok, reason = c.check(
                    code, is_st, side, row_today, row_yesterday
                )
            elif isinstance(c, DelistConstraint):
                ok, reason = c.check(side, delisted)
            else:
                raise TypeError(f"Unknown constraint {type(c)}")
            if not ok:
                return False, reason
            # DELIST_FORCE_OUT is "pass but record reason" — surface it
            if reason == "DELIST_FORCE_OUT":
                return True, reason
        return True, None
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/constraints.py tests/backtest/test_constraints.py
git commit -m "feat(sub-3): TradeConstraint chain (suspended/limit/delist)"
```

---

### Task 6: PortfolioState

**Files:**
- Create: `instock/backtest/portfolio_state.py`
- Test: `tests/backtest/test_portfolio_state.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_portfolio_state.py`:

```python
from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.portfolio_state import PortfolioState


def test_initial_state_all_cash():
    p = PortfolioState(initial_cash=1_000_000.0)
    assert p.cash == 1_000_000.0
    assert p.positions == {}
    snap = p.snapshot(pd.Timestamp("2023-01-03"), prices={})
    assert snap["total_value"] == 1_000_000.0
    assert snap["position_value"] == 0.0


def test_apply_trade_buy_updates_cash_and_position():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade(
        code="600000", side="BUY", shares=100, fill_price=10.0,
        total_fees=5.01, slippage_value=0.5,
    )
    # cash -= 100*10 + 5.01 + 0.5
    assert p.cash == pytest.approx(1_000_000.0 - 1000.0 - 5.01 - 0.5)
    assert p.positions["600000"].shares == 100
    # avg_cost includes fees+slippage
    assert p.positions["600000"].avg_cost == pytest.approx(
        (1000.0 + 5.01 + 0.5) / 100
    )


def test_apply_trade_sell_reduces_position():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 200, 10.0, 5.0, 1.0)
    p.apply_trade("600000", "SELL", 100, 11.0, 6.05, 0.55)
    pos = p.positions["600000"]
    assert pos.shares == 100
    # cash = 1_000_000 - (200*10 + 5 + 1) + (100*11 - 6.05 - 0.55)
    assert p.cash == pytest.approx(
        1_000_000.0 - 2006.0 + (1100.0 - 6.05 - 0.55)
    )


def test_sell_all_removes_position():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 100, 10.0, 5.0, 0.5)
    p.apply_trade("600000", "SELL", 100, 11.0, 5.5, 0.55)
    assert "600000" not in p.positions


def test_mark_to_market_updates_snapshot():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 100, 10.0, 5.0, 0.5)
    prices = {"600000": 12.0}
    snap = p.snapshot(pd.Timestamp("2023-01-04"), prices)
    assert snap["position_value"] == pytest.approx(100 * 12.0)
    assert snap["total_value"] == pytest.approx(p.cash + 100 * 12.0)


def test_snapshot_missing_price_uses_avg_cost():
    p = PortfolioState(initial_cash=1_000_000.0)
    p.apply_trade("600000", "BUY", 100, 10.0, 5.0, 0.5)
    # Price dict empty -> use avg_cost as fallback (suspended day)
    snap = p.snapshot(pd.Timestamp("2023-01-04"), prices={})
    avg = p.positions["600000"].avg_cost
    assert snap["position_value"] == pytest.approx(100 * avg)
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/portfolio_state.py`:

```python
"""PortfolioState: the mutable core of the backtest loop.

Tracks cash and shares per code; avg_cost is cost-basis of remaining
shares (simple weighted mean including fees+slippage).

SELL uses FIFO: no separate lot accounting — avg_cost stays constant on
SELL (cost basis of remaining shares unchanged), matching common practice.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

import pandas as pd


@dataclass
class Position:
    code: str
    shares: int
    avg_cost: float   # per-share


@dataclass
class PortfolioState:
    initial_cash: float
    cash: float = field(init=False)
    positions: Dict[str, Position] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.cash = float(self.initial_cash)

    def apply_trade(
        self, code: str, side: str, shares: int,
        fill_price: float, total_fees: float, slippage_value: float,
    ) -> None:
        gross = shares * fill_price
        if side == "BUY":
            total_out = gross + total_fees + slippage_value
            self.cash -= total_out
            pos = self.positions.get(code)
            if pos is None:
                self.positions[code] = Position(
                    code=code, shares=shares,
                    avg_cost=total_out / shares if shares > 0 else 0.0,
                )
            else:
                new_shares = pos.shares + shares
                new_cost_basis = pos.avg_cost * pos.shares + total_out
                pos.shares = new_shares
                pos.avg_cost = (
                    new_cost_basis / new_shares if new_shares > 0 else 0.0
                )
        elif side == "SELL":
            proceeds = gross - total_fees - slippage_value
            self.cash += proceeds
            pos = self.positions.get(code)
            if pos is None:
                raise ValueError(f"SELL on empty position {code}")
            pos.shares -= shares
            if pos.shares <= 0:
                del self.positions[code]
            # avg_cost unchanged on sell (cost basis of remaining shares)
        else:
            raise ValueError(f"unknown side {side}")

    def snapshot(
        self, at: pd.Timestamp, prices: Dict[str, float],
    ) -> dict:
        position_value = 0.0
        rows = []
        for code, pos in self.positions.items():
            price = prices.get(code, pos.avg_cost)
            mv = pos.shares * price
            position_value += mv
            rows.append({
                "date": at, "code": code,
                "shares": int(pos.shares),
                "avg_cost": float(pos.avg_cost),
                "market_value": float(mv),
                "unrealized_pnl": float(
                    (price - pos.avg_cost) * pos.shares
                ),
            })
        total_value = self.cash + position_value
        for r in rows:
            r["weight"] = (
                r["market_value"] / total_value if total_value > 0 else 0.0
            )
        return {
            "date": at,
            "cash": float(self.cash),
            "position_value": float(position_value),
            "total_value": float(total_value),
            "positions": rows,
            "n_holdings": len(rows),
        }
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/portfolio_state.py tests/backtest/test_portfolio_state.py
git commit -m "feat(sub-3): PortfolioState with cash/positions/mark-to-market"
```

---

### Task 7: Executor

**Files:**
- Create: `instock/backtest/execution.py`
- Test: `tests/backtest/test_execution.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_execution.py`:

```python
from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.costs import StandardFeeModel, BpsSlippage
from instock.backtest.execution import Executor, Order
from instock.backtest.portfolio_state import PortfolioState


def test_execute_buy_lot_size_floors():
    # target_shares = 10_000 / 10 = 1000; lot=100 → 1000 (already multiple)
    # target_shares = 999 / 10 = 99.9 → floor to lot: 0
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    ex = Executor(
        fee_model=StandardFeeModel(),
        slippage_model=BpsSlippage(bps=0.0),
        lot_size=100,
    )
    order = Order(
        code="600000", side="BUY",
        target_value=999.0, target_w=0.001,
    )
    row_today = {"open": 10.0, "close": 10.0, "volume": 100}
    trade = ex.execute(
        order=order, row_today=row_today,
        portfolio=portfolio, at=pd.Timestamp("2023-01-03"),
        reason="REBALANCE",
    )
    # 99.9 shares -> floor(99.9/100)*100 = 0 -> no-op trade
    assert trade["filled_shares"] == 0
    assert trade["reason"] == "NO_OP"


def test_execute_buy_normal_flow():
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    ex = Executor(
        fee_model=StandardFeeModel(),
        slippage_model=BpsSlippage(bps=5.0),
        lot_size=100,
    )
    order = Order(code="600000", side="BUY", target_value=10_000.0, target_w=0.01)
    row = {"open": 10.0, "close": 10.5, "volume": 100_000}
    trade = ex.execute(order, row, portfolio, pd.Timestamp("2023-01-03"), "REBALANCE")
    # fill_price = 10 * 1.0005 = 10.005
    # shares = floor(10000 / 10.005 / 100) * 100 = 900 (9.995 lots -> 9 lots)
    assert trade["fill_price"] == pytest.approx(10.005)
    assert trade["filled_shares"] == 900
    assert trade["side"] == "BUY"
    assert trade["commission"] >= 5.0
    assert portfolio.positions["600000"].shares == 900


def test_execute_sell_normal_flow():
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    # seed a position
    portfolio.apply_trade("600000", "BUY", 500, 10.0, 5.0, 0.0)
    ex = Executor(StandardFeeModel(), BpsSlippage(5.0), lot_size=100)
    order = Order(code="600000", side="SELL", target_value=2000.0, target_w=0.0)
    row = {"open": 11.0, "close": 11.0, "volume": 100_000}
    trade = ex.execute(order, row, portfolio, pd.Timestamp("2023-01-04"), "REBALANCE")
    # fill_price ≈ 10.9945
    # sell shares = min(held=500, floor(2000/11)) ; but target_value=2000 means
    # we sell DOWN to 2000 value → need to keep 2000/11=181 -> lot 100 -> sell 400
    # Executor contract: SELL target_value is the value to *keep*; delta handled upstream
    # Simpler MVP contract: SELL target_value = proceeds to raise (value to SELL)
    # We design: order.target_value is trade value, not residual. So:
    # desired_shares_to_sell = floor(2000/11.0/100)*100 = floor(18.18/100)*100 = 0? no:
    # desired = floor(2000/fill_price) then floor to lot
    # fill_price=10.9945, shares = floor(2000/10.9945) = 181 -> floor to 100 = 100
    assert trade["side"] == "SELL"
    assert trade["filled_shares"] == 100
    assert portfolio.positions["600000"].shares == 400


def test_execute_sell_capped_by_held_shares():
    portfolio = PortfolioState(initial_cash=1_000_000.0)
    portfolio.apply_trade("600000", "BUY", 200, 10.0, 5.0, 0.0)
    ex = Executor(StandardFeeModel(), BpsSlippage(0.0), lot_size=100)
    # Try to sell value of 100_000 (would be 10_000 shares at 10)
    order = Order(code="600000", side="SELL", target_value=100_000.0, target_w=0.0)
    row = {"open": 10.0, "close": 10.0, "volume": 100_000}
    trade = ex.execute(order, row, portfolio, pd.Timestamp("2023-01-04"), "REBALANCE")
    assert trade["filled_shares"] == 200   # capped at held
    assert "600000" not in portfolio.positions
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/execution.py`:

```python
"""Executor: turn Order objects into TradeRecord dicts + mutate portfolio.

Order contract:
  BUY:  target_value = cash to spend (upper bound); shares = floor(value / fill_price)
         then floor to lot_size. Residual cash stays in portfolio (lot drag).
  SELL: target_value = value to sell (gross); shares = floor(value / fill_price)
         then floor to lot_size, capped by held shares.

Executor does NOT check constraints — that is the engine's job before it
even creates the Order.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from .costs import FeeModel, SlippageModel
from .portfolio_state import PortfolioState


@dataclass
class Order:
    code: str
    side: str           # "BUY" or "SELL"
    target_value: float
    target_w: float     # for trade record audit


class Executor:
    def __init__(
        self,
        fee_model: FeeModel,
        slippage_model: SlippageModel,
        lot_size: int = 100,
    ) -> None:
        self.fee_model = fee_model
        self.slippage_model = slippage_model
        self.lot_size = lot_size

    def execute(
        self,
        order: Order,
        row_today: dict,
        portfolio: PortfolioState,
        at: pd.Timestamp,
        reason: str,
    ) -> dict:
        open_price = float(row_today["open"])
        fill_price = self.slippage_model.fill_price(open_price, order.side)

        if fill_price <= 0:
            return self._noop_trade(order, at, fill_price, reason)

        # Target shares from value / fill_price, floored to lot size
        raw_shares = int(order.target_value // fill_price)
        lots = raw_shares // self.lot_size
        shares = lots * self.lot_size

        if order.side == "SELL":
            held = portfolio.positions.get(order.code)
            if held is not None:
                shares = min(shares, held.shares)
                # Force full exit if remainder < 1 lot
                if held.shares - shares < self.lot_size:
                    shares = held.shares
            else:
                shares = 0

        if shares <= 0:
            return self._noop_trade(order, at, fill_price, reason)

        gross = shares * fill_price
        fees = self.fee_model.compute(value=gross, side=order.side)
        total_fees = fees["commission"] + fees["stamp_tax"] + fees["transfer_fee"]
        slippage_value = abs(fill_price - open_price) * shares

        portfolio.apply_trade(
            code=order.code, side=order.side, shares=shares,
            fill_price=fill_price, total_fees=total_fees,
            slippage_value=slippage_value,
        )

        net = gross + total_fees + slippage_value
        net_cash_change = -net if order.side == "BUY" else (gross - total_fees - slippage_value)
        return {
            "date": at, "code": order.code, "side": order.side,
            "target_w": float(order.target_w),
            "filled_shares": int(shares),
            "fill_price": float(fill_price),
            "fill_value": float(gross),
            "commission": float(fees["commission"]),
            "stamp_tax": float(fees["stamp_tax"]),
            "transfer_fee": float(fees["transfer_fee"]),
            "slippage_value": float(slippage_value),
            "gross_value": float(net),
            "net_cash_change": float(net_cash_change),
            "reason": reason,
        }

    @staticmethod
    def _noop_trade(order: Order, at: pd.Timestamp,
                    fill_price: float, reason: str) -> dict:
        return {
            "date": at, "code": order.code, "side": order.side,
            "target_w": float(order.target_w),
            "filled_shares": 0,
            "fill_price": float(max(fill_price, 0.0)),
            "fill_value": 0.0, "commission": 0.0,
            "stamp_tax": 0.0, "transfer_fee": 0.0,
            "slippage_value": 0.0, "gross_value": 0.0,
            "net_cash_change": 0.0, "reason": "NO_OP",
        }
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/execution.py tests/backtest/test_execution.py
git commit -m "feat(sub-3): Executor with lot-size rounding + fees/slippage"
```

---

### Task 8: BacktestEngine event loop

**Files:**
- Create: `instock/backtest/engine.py`
- Test: `tests/backtest/test_engine.py`

- [ ] **Step 1: Write failing test (minimal round-trip)**

File `tests/backtest/test_engine.py`:

```python
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from instock.backtest.config import BacktestConfig
from instock.backtest.engine import BacktestEngine


def _stub_ohlcv(codes, start, end):
    """Make a flat OHLCV panel, one row per (code, date)."""
    dates = pd.bdate_range(start, end, freq="B")  # 5-day week approx
    rows = []
    for c in codes:
        base = 10.0
        for i, d in enumerate(dates):
            price = base + 0.1 * i  # gently rising
            rows.append({
                "date": d, "code": c,
                "open": price, "high": price * 1.01,
                "low": price * 0.99, "close": price,
                "volume": 100_000, "amount": price * 100_000,
            })
    return pd.DataFrame(rows)


def _stub_holding(dates, codes):
    """2-code equal-weight holding schedule."""
    rows = []
    for d in dates:
        for c in codes:
            rows.append({
                "date": d, "code": c,
                "weight": 1.0 / len(codes),
                "score": 1.0,
                "strategy": "stub",
            })
    return pd.DataFrame(rows)


def test_engine_end_to_end_minimal(tmp_path):
    codes = ["600000", "600519"]
    start = date(2023, 1, 3)
    end = date(2023, 1, 20)

    ohlcv = _stub_ohlcv(codes, start, end)
    # One rebalance on 2023-01-04
    holding = _stub_holding([pd.Timestamp("2023-01-04")], codes)

    cal = [d.date() for d in pd.bdate_range(start, end)]
    source = MagicMock()
    source.get_trade_calendar.return_value = cal

    # Mock OhlcvPanelStore: return our stubbed panel
    store = MagicMock()
    store.get_panel.return_value = ohlcv

    cfg = BacktestConfig(
        strategy="stub", start=start, end=end,
        initial_capital=1_000_000.0, enable_slippage=False,
    )
    engine = BacktestEngine(source=source, ohlcv_store=store)
    result = engine.run(holding_schedule=holding, config=cfg)

    assert "trades" in result and "positions" in result and "nav" in result
    nav = result["nav"]
    assert len(nav) == len(cal)
    # After buying at 2023-01-05 open, NAV should fluctuate with prices
    assert nav["nav"].iloc[-1] > 0
    trades = result["trades"]
    # Two BUY orders for two codes on first rebalance
    assert (trades["side"] == "BUY").sum() == 2


def test_engine_no_rebalance_dates_returns_flat_nav():
    codes = ["600000"]
    start = date(2023, 1, 3)
    end = date(2023, 1, 10)
    source = MagicMock()
    cal = [d.date() for d in pd.bdate_range(start, end)]
    source.get_trade_calendar.return_value = cal
    store = MagicMock()
    store.get_panel.return_value = _stub_ohlcv(codes, start, end)

    cfg = BacktestConfig(strategy="s", start=start, end=end)
    engine = BacktestEngine(source=source, ohlcv_store=store)
    res = engine.run(holding_schedule=pd.DataFrame(columns=[
        "date", "code", "weight", "score", "strategy"]), config=cfg)
    assert (res["nav"]["total_value"] == cfg.initial_capital).all()
    assert len(res["trades"]) == 0


def test_engine_skips_suspended_and_defers(tmp_path):
    codes = ["600000"]
    start = date(2023, 1, 3)
    end = date(2023, 1, 13)
    ohlcv = _stub_ohlcv(codes, start, end)
    # suspend on the T+1 after the rebalance
    rebal = pd.Timestamp("2023-01-04")
    next_day = pd.Timestamp("2023-01-05")
    ohlcv.loc[
        (ohlcv["code"] == "600000") & (ohlcv["date"] == next_day), "volume"
    ] = 0
    holding = _stub_holding([rebal], codes)

    cal = [d.date() for d in pd.bdate_range(start, end)]
    source = MagicMock()
    source.get_trade_calendar.return_value = cal
    store = MagicMock()
    store.get_panel.return_value = ohlcv

    cfg = BacktestConfig(strategy="s", start=start, end=end,
                         enable_slippage=False)
    engine = BacktestEngine(source=source, ohlcv_store=store)
    res = engine.run(holding_schedule=holding, config=cfg)

    trades = res["trades"]
    # BUY succeeds on next trade day (2023-01-06), not on 01-05
    buys = trades[trades["side"] == "BUY"]
    assert len(buys) == 1
    assert buys["date"].iloc[0] == pd.Timestamp("2023-01-06")
```

- [ ] **Step 2: Run (expect FAIL: module missing)**

- [ ] **Step 3: Implement**

File `instock/backtest/engine.py`:

```python
"""BacktestEngine: event-driven daily loop over trade_calendar.

Loop per day d:
  1. Apply pending orders scheduled for d (from d-1 rebalance):
     - Constraint check: suspended/limit/delist
     - On block -> defer to next trading day unless max_defer exceeded
     - On pass -> Executor.execute mutates portfolio
  2. Mark-to-market on close, emit NAV row.
  3. If d appears in holding_schedule: compute orders (deltas), schedule
     them for next trade day.

T+1 semantics: holding[d] produces orders that execute at d+1 open.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from typing import Optional

import pandas as pd

from instock.datasource.base import IDataSource
from instock.refdata.ohlcv_store import OhlcvPanelStore
from instock.refdata import st as refdata_st
from instock.refdata import listing as refdata_listing
from instock.refdata.schemas import RefdataNotAvailable

from .config import BacktestConfig
from .constraints import (
    ConstraintChain, DelistConstraint, LimitDownConstraint,
    LimitUpConstraint, SuspendedConstraint,
)
from .costs import (
    BpsSlippage, StandardFeeModel, ZeroFeeModel, ZeroSlippage,
)
from .execution import Executor, Order
from .portfolio_state import PortfolioState

log = logging.getLogger(__name__)


class BacktestEngine:
    def __init__(
        self,
        source: IDataSource,
        ohlcv_store: OhlcvPanelStore,
    ) -> None:
        self.source = source
        self.ohlcv_store = ohlcv_store

    def run(
        self, holding_schedule: pd.DataFrame, config: BacktestConfig,
    ) -> dict:
        cal_dates = [
            pd.Timestamp(d) for d in self.source.get_trade_calendar(
                config.start, config.end
            )
        ]
        if not cal_dates:
            return self._empty_result(config)

        # 1. Determine universe: union of codes in holding_schedule
        codes = (
            sorted(holding_schedule["code"].unique().tolist())
            if not holding_schedule.empty else []
        )

        # 2. Load OHLCV for that universe, full window
        panel = (
            self.ohlcv_store.get_panel(
                codes, config.start, config.end,
                adjust=config.price_adjust,
            ) if codes else pd.DataFrame(columns=[
                "date", "code", "open", "high", "low", "close",
                "volume", "amount"])
        )
        # Index (date, code) -> row for fast lookups
        panel_by_day = {
            ts: grp.set_index("code").to_dict("index")
            for ts, grp in panel.groupby("date")
        }

        # 3. Try to load listing_dates + st_flags (Sub-2.5)
        try:
            listing_dates = refdata_listing.read_listing_dates()
        except RefdataNotAvailable:
            listing_dates = None
        try:
            st_flags = refdata_st.read_st_flags(config.end)
        except RefdataNotAvailable:
            st_flags = set()

        # 4. Build portfolio + executor + constraint chain
        portfolio = PortfolioState(initial_cash=config.initial_capital)
        fee_model = (
            StandardFeeModel(
                commission_rate=config.commission_rate,
                commission_min=config.commission_min,
                stamp_tax_rate=config.stamp_tax_rate,
                transfer_fee_rate=config.transfer_fee_rate,
            ) if config.enable_fees else ZeroFeeModel()
        )
        slip_model = (
            BpsSlippage(bps=config.slippage_bps)
            if config.enable_slippage else ZeroSlippage()
        )
        executor = Executor(
            fee_model=fee_model, slippage_model=slip_model,
            lot_size=config.lot_size,
        )
        chain = ConstraintChain([
            SuspendedConstraint(),
            LimitUpConstraint(tolerance=config.limit_tolerance),
            LimitDownConstraint(tolerance=config.limit_tolerance),
            DelistConstraint(),
        ])

        # 5. Index holding_schedule by date for lookup
        holding_dates = set()
        holding_by_day: dict = {}
        if not holding_schedule.empty:
            for ts, grp in holding_schedule.groupby("date"):
                holding_dates.add(pd.Timestamp(ts))
                holding_by_day[pd.Timestamp(ts)] = grp.set_index("code")[
                    "weight"
                ].to_dict()

        # 6. Event loop
        pending: dict = defaultdict(list)       # date -> list[(order, defer_count)]
        trades: list = []
        positions: list = []
        nav_rows: list = []
        prev_total_value = config.initial_capital

        for i, d in enumerate(cal_dates):
            # 6a. Apply pending orders for d
            day_orders = pending.pop(d, [])
            panel_today = panel_by_day.get(d, {})
            panel_prev = panel_by_day.get(
                cal_dates[i - 1], {}
            ) if i > 0 else {}
            for order, defer_count in day_orders:
                row_today = panel_today.get(order.code)
                row_prev = panel_prev.get(order.code, {"close": 0.0})
                delisted = (
                    row_today is None or
                    (listing_dates is not None
                     and order.code not in listing_dates)
                )
                if row_today is None:
                    # No row today -> treat as suspended + defer
                    defer_count += 1
                    if defer_count > config.max_suspended_defer_days:
                        trades.append(executor._noop_trade(
                            order, d, 0.0, "NO_OP"
                        ))
                        continue
                    next_day = cal_dates[i + 1] if i + 1 < len(cal_dates) else None
                    if next_day is not None:
                        pending[next_day].append((order, defer_count))
                    continue
                is_st = order.code in (st_flags or set())
                ok, reason = chain.check(
                    code=order.code, is_st=is_st, side=order.side,
                    row_today=row_today, row_yesterday=row_prev,
                    delisted=delisted,
                )
                if not ok:
                    defer_count += 1
                    if defer_count > config.max_suspended_defer_days:
                        trades.append(executor._noop_trade(
                            order, d, row_today.get("open", 0.0),
                            reason or "NO_OP",
                        ))
                        continue
                    next_day = cal_dates[i + 1] if i + 1 < len(cal_dates) else None
                    if next_day is not None:
                        pending[next_day].append((order, defer_count))
                    continue
                trade = executor.execute(
                    order=order, row_today=row_today,
                    portfolio=portfolio, at=d,
                    reason=reason or "REBALANCE",
                )
                trades.append(trade)

            # 6b. Mark-to-market on close
            close_prices = {
                c: r["close"] for c, r in panel_today.items()
            }
            snap = portfolio.snapshot(d, close_prices)
            total = snap["total_value"]
            ret_daily = (
                total / prev_total_value - 1.0 if prev_total_value > 0 else 0.0
            )
            nav_rows.append({
                "date": d,
                "nav": total / config.initial_capital,
                "cash": snap["cash"],
                "position_value": snap["position_value"],
                "total_value": total,
                "ret_daily": ret_daily,
                "ret_cum": total / config.initial_capital - 1.0,
                "turnover_daily": sum(
                    abs(t["net_cash_change"]) for t in trades
                    if t["date"] == d
                ) / max(total, 1.0),
                "n_holdings": snap["n_holdings"],
            })
            positions.extend(snap["positions"])
            prev_total_value = total

            # 6c. If d is a rebalance date, schedule orders for d+1
            if d in holding_dates:
                target_weights = holding_by_day[d]
                orders = self._compute_orders(
                    portfolio=portfolio,
                    target_weights=target_weights,
                    close_prices=close_prices,
                    total_value=total,
                )
                next_day = cal_dates[i + 1] if i + 1 < len(cal_dates) else None
                if next_day is not None:
                    for o in orders:
                        pending[next_day].append((o, 0))

        return {
            "trades": pd.DataFrame(trades),
            "positions": pd.DataFrame(positions),
            "nav": pd.DataFrame(nav_rows),
        }

    @staticmethod
    def _compute_orders(
        portfolio: PortfolioState,
        target_weights: dict,
        close_prices: dict,
        total_value: float,
    ) -> list:
        """Produce Orders to move current -> target weights.

        SELL first (to raise cash), then BUY. target_value for BUY is
        the delta value; for SELL it is the value to liquidate.
        """
        current = {
            c: pos.shares * close_prices.get(c, pos.avg_cost)
            for c, pos in portfolio.positions.items()
        }
        codes = set(current) | set(target_weights)
        orders_sell = []
        orders_buy = []
        for c in sorted(codes):
            tgt_val = target_weights.get(c, 0.0) * total_value
            cur_val = current.get(c, 0.0)
            delta = tgt_val - cur_val
            if delta < 0:
                orders_sell.append(Order(
                    code=c, side="SELL",
                    target_value=abs(delta),
                    target_w=target_weights.get(c, 0.0),
                ))
            elif delta > 0:
                orders_buy.append(Order(
                    code=c, side="BUY",
                    target_value=delta,
                    target_w=target_weights.get(c, 0.0),
                ))
        return orders_sell + orders_buy

    @staticmethod
    def _empty_result(config: BacktestConfig) -> dict:
        return {
            "trades": pd.DataFrame(),
            "positions": pd.DataFrame(),
            "nav": pd.DataFrame(),
        }
```

- [ ] **Step 4: Run (expect PASS)**

```
conda run -n base python -m pytest tests/backtest/test_engine.py -v
```

- [ ] **Step 5: Commit**

```
git add instock/backtest/engine.py tests/backtest/test_engine.py
git commit -m "feat(sub-3): BacktestEngine event loop w/ T+1 + defer-on-block"
```

---

### Task 9: Storage (Parquet writers/readers)

**Files:**
- Create: `instock/backtest/storage.py`
- Test: `tests/backtest/test_storage.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_storage.py`:

```python
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from instock.backtest import storage as bt_storage


@pytest.fixture
def tmp_root(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_BACKTEST_ROOT", str(tmp_path))
    return tmp_path


def _trade_df():
    return pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"), "code": "600000", "side": "BUY",
        "target_w": 0.01, "filled_shares": 100, "fill_price": 10.0,
        "fill_value": 1000.0, "commission": 5.0, "stamp_tax": 0.0,
        "transfer_fee": 0.01, "slippage_value": 0.5, "gross_value": 1005.51,
        "net_cash_change": -1005.51, "reason": "REBALANCE",
    }])


def _nav_df():
    return pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"), "nav": 1.0,
        "cash": 100000.0, "position_value": 0.0, "total_value": 100000.0,
        "ret_daily": 0.0, "ret_cum": 0.0, "turnover_daily": 0.0,
        "n_holdings": 0,
    }])


def _positions_df():
    return pd.DataFrame([{
        "date": pd.Timestamp("2023-01-03"), "code": "600000",
        "shares": 100, "avg_cost": 10.05, "market_value": 1000.0,
        "unrealized_pnl": -5.0, "weight": 0.01,
    }])


def test_write_read_round_trip(tmp_root):
    bt_storage.write_run(
        run_id="test_run_abcd1234",
        trades=_trade_df(), positions=_positions_df(), nav=_nav_df(),
    )
    r = bt_storage.read_run("test_run_abcd1234")
    assert len(r["trades"]) == 1
    assert len(r["nav"]) == 1
    assert len(r["positions"]) == 1


def test_append_metrics_merges_by_run_id(tmp_root):
    row1 = {
        "run_id": "x_2023_abcd1234", "strategy": "x",
        "start": pd.Timestamp("2023-01-04"), "end": pd.Timestamp("2023-12-29"),
        "ret_annual": 0.1, "ret_total": 0.12, "vol_annual": 0.15,
        "sharpe": 0.7, "sortino": 0.9, "max_drawdown": -0.1,
        "max_dd_duration_days": 30, "calmar": 1.0,
        "win_rate_daily": 0.52, "win_rate_monthly": 0.6,
        "turnover_annual": 2.0, "total_cost_bps": 30.0, "lot_drag_bps": 2.0,
        "fingerprint_sha": "a" * 64, "refdata_as_of": "2026-04-18",
    }
    bt_storage.append_metrics(row1)
    # Re-append same run_id with updated sharpe -> replaces
    row1["sharpe"] = 0.8
    bt_storage.append_metrics(row1)
    all_m = bt_storage.read_metrics()
    assert len(all_m) == 1
    assert all_m["sharpe"].iloc[0] == 0.8


def test_read_run_missing_returns_empty(tmp_root):
    r = bt_storage.read_run("does_not_exist")
    assert r["trades"].empty
    assert r["nav"].empty
    assert r["positions"].empty
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/storage.py`:

```python
"""Parquet IO for backtest artifacts.

Layout:
  <INSTOCK_BACKTEST_ROOT>/<run_id>/{trades,positions,nav}.parquet
  <INSTOCK_BACKTEST_ROOT>/_metrics.parquet   (append; dedup by run_id)
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from .schemas import (
    METRICS_SCHEMA, NAV_SCHEMA, POSITION_SCHEMA, TRADE_SCHEMA,
)


def _root() -> Path:
    r = Path(os.environ.get("INSTOCK_BACKTEST_ROOT", "data/backtest"))
    r.mkdir(parents=True, exist_ok=True)
    return r


def _run_dir(run_id: str) -> Path:
    d = _root() / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_run(
    run_id: str,
    trades: pd.DataFrame,
    positions: pd.DataFrame,
    nav: pd.DataFrame,
) -> None:
    if not trades.empty:
        TRADE_SCHEMA.validate(trades.copy())
    if not positions.empty:
        POSITION_SCHEMA.validate(positions.copy())
    if not nav.empty:
        NAV_SCHEMA.validate(nav.copy())
    d = _run_dir(run_id)
    trades.to_parquet(d / "trades.parquet", index=False)
    positions.to_parquet(d / "positions.parquet", index=False)
    nav.to_parquet(d / "nav.parquet", index=False)


def read_run(run_id: str) -> dict:
    d = _run_dir(run_id)
    def _read(name: str) -> pd.DataFrame:
        p = d / f"{name}.parquet"
        return pd.read_parquet(p) if p.exists() else pd.DataFrame()
    return {
        "trades": _read("trades"),
        "positions": _read("positions"),
        "nav": _read("nav"),
    }


def append_metrics(row: dict) -> None:
    df = pd.DataFrame([row])
    METRICS_SCHEMA.validate(df.copy())
    path = _root() / "_metrics.parquet"
    if path.exists():
        old = pd.read_parquet(path)
        merged = pd.concat([old, df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["run_id"], keep="last")
    else:
        merged = df
    merged.to_parquet(path, index=False)


def read_metrics() -> pd.DataFrame:
    path = _root() / "_metrics.parquet"
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/storage.py tests/backtest/test_storage.py
git commit -m "feat(sub-3): Parquet storage for trades/positions/nav/metrics"
```

---

### Task 10: Benchmarks loader

**Files:**
- Create: `instock/backtest/benchmarks.py`
- Test: `tests/backtest/test_benchmarks.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_benchmarks.py`:

```python
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from instock.backtest.benchmarks import load_benchmarks


def _ohlcv_series(start, end, code, base=3000.0):
    dates = pd.bdate_range(start, end)
    return pd.DataFrame({
        "date": dates, "code": [code] * len(dates),
        "open": base, "high": base, "low": base,
        "close": [base + i * 0.1 for i in range(len(dates))],
        "volume": 1, "amount": 1,
    })


def test_load_benchmarks_returns_wide_frame():
    src = MagicMock()
    def _gt(code, start, end, adjust="qfq"):
        return _ohlcv_series(start, end, code)
    src.get_ohlcv.side_effect = _gt

    out = load_benchmarks(
        src, ["000300.SH", "000905.SH"],
        date(2023, 1, 4), date(2023, 1, 20),
    )
    assert "000300.SH" in out.columns
    assert "000905.SH" in out.columns
    assert isinstance(out.index, pd.DatetimeIndex)


def test_load_benchmarks_skips_missing():
    src = MagicMock()
    def _gt(code, start, end, adjust="qfq"):
        if code == "000300.SH":
            return _ohlcv_series(start, end, code)
        raise RuntimeError("not found")
    src.get_ohlcv.side_effect = _gt

    out = load_benchmarks(
        src, ["000300.SH", "000999.SH"],
        date(2023, 1, 4), date(2023, 1, 20),
    )
    assert "000300.SH" in out.columns
    assert "000999.SH" not in out.columns


def test_load_benchmarks_empty_list():
    out = load_benchmarks(
        MagicMock(), [], date(2023, 1, 4), date(2023, 1, 20),
    )
    assert out.empty
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/benchmarks.py`:

```python
"""Benchmark OHLCV loader.

Returns wide DataFrame:
    index = date (DatetimeIndex)
    columns = one per successfully-loaded benchmark code
    values = close price
Missing/failed benchmarks are skipped with a warning.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Sequence

import pandas as pd

from instock.datasource.base import IDataSource

log = logging.getLogger(__name__)


def load_benchmarks(
    source: IDataSource,
    codes: Sequence[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    series_list = []
    for c in codes:
        try:
            df = source.get_ohlcv(c, start, end, adjust="qfq")
            if df.empty:
                log.warning("benchmark %s: empty", c)
                continue
            s = (
                df.set_index("date")["close"]
                .rename(c)
                .sort_index()
            )
            series_list.append(s)
        except Exception as exc:  # noqa: BLE001
            log.warning("benchmark %s: failed (%s)", c, exc)
    if not series_list:
        return pd.DataFrame()
    return pd.concat(series_list, axis=1).sort_index()
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/benchmarks.py tests/backtest/test_benchmarks.py
git commit -m "feat(sub-3): benchmarks loader (wide-frame, skip-on-fail)"
```

---

### Task 11: Metrics

**Files:**
- Create: `instock/backtest/metrics.py`
- Test: `tests/backtest/test_metrics.py`

- [ ] **Step 1: Write failing tests (known-value sanity checks)**

File `tests/backtest/test_metrics.py`:

```python
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from instock.backtest.metrics import (
    annualize_return, annualize_volatility,
    sharpe_ratio, max_drawdown, compute_metrics,
)


def test_annualize_constant_daily_return():
    # 1% daily for 252 days
    rets = pd.Series([0.01] * 252)
    ann = annualize_return(rets)
    assert ann == pytest.approx((1.01) ** 252 - 1, rel=1e-6)


def test_annualize_vol_scales_sqrt():
    rets = pd.Series(np.random.default_rng(0).normal(0, 0.01, 252))
    vol_daily = rets.std(ddof=1)
    ann = annualize_volatility(rets)
    assert ann == pytest.approx(vol_daily * np.sqrt(252), rel=1e-6)


def test_sharpe_positive_drift():
    rets = pd.Series([0.001] * 252)  # positive, no vol -> sharpe undefined
    sr = sharpe_ratio(rets)
    # zero vol -> nan
    assert pd.isna(sr)


def test_max_drawdown_known():
    # nav: 1, 1.2, 1.0, 1.3 -> peak 1.2, drop to 1.0 = -1/6 ≈ -0.1667
    nav = pd.Series([1.0, 1.2, 1.0, 1.3])
    dd, dur = max_drawdown(nav)
    assert dd == pytest.approx(-1 / 6, rel=1e-4)
    assert dur == 1


def test_max_drawdown_no_drawdown():
    nav = pd.Series([1.0, 1.1, 1.2])
    dd, dur = max_drawdown(nav)
    assert dd == 0.0
    assert dur == 0


def test_compute_metrics_smoke():
    dates = pd.bdate_range("2023-01-04", periods=252)
    nav_values = np.cumprod(1 + 0.0005 * np.ones(252))
    nav_df = pd.DataFrame({
        "date": dates, "nav": nav_values,
        "cash": 0.0, "position_value": nav_values * 1_000_000,
        "total_value": nav_values * 1_000_000,
        "ret_daily": [0.0005] * 252,
        "ret_cum": nav_values - 1,
        "turnover_daily": [0.0] * 252, "n_holdings": [10] * 252,
    })
    trades = pd.DataFrame([])
    benchmarks = pd.DataFrame()
    m = compute_metrics(nav_df, trades, benchmarks)
    assert m["ret_annual"] > 0
    assert m["vol_annual"] == 0.0
    assert pd.isna(m["sharpe"])
    assert m["max_drawdown"] == 0.0
    assert m["total_cost_bps"] == 0.0
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/metrics.py`:

```python
"""Performance metrics.

Standard quant formulas; 252 trading days/year. All return dicts are
flat (not nested) so they can go straight into METRICS_SCHEMA.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

_TD_PER_YEAR = 252


def annualize_return(rets: pd.Series) -> float:
    if rets.empty:
        return 0.0
    total = (1 + rets).prod()
    n = len(rets)
    return float(total ** (_TD_PER_YEAR / n) - 1)


def annualize_volatility(rets: pd.Series) -> float:
    if len(rets) < 2:
        return 0.0
    return float(rets.std(ddof=1) * np.sqrt(_TD_PER_YEAR))


def sharpe_ratio(rets: pd.Series, rf: float = 0.0) -> float:
    vol = annualize_volatility(rets)
    if vol == 0.0:
        return float("nan")
    excess = rets - rf / _TD_PER_YEAR
    return float(excess.mean() / rets.std(ddof=1) * np.sqrt(_TD_PER_YEAR))


def sortino_ratio(rets: pd.Series, rf: float = 0.0) -> float:
    downside = rets[rets < 0]
    if len(downside) < 2:
        return float("nan")
    dd_std = downside.std(ddof=1) * np.sqrt(_TD_PER_YEAR)
    if dd_std == 0.0:
        return float("nan")
    return float((rets.mean() - rf / _TD_PER_YEAR) / dd_std * _TD_PER_YEAR)


def max_drawdown(nav: pd.Series) -> tuple[float, int]:
    """Return (max_dd, duration_days). max_dd <= 0."""
    if nav.empty:
        return 0.0, 0
    peaks = nav.cummax()
    dd = nav / peaks - 1.0
    mdd = float(dd.min())
    if mdd == 0.0:
        return 0.0, 0
    end = int(dd.idxmin())
    # duration: distance from previous peak to trough
    peak_idx = int(nav.iloc[:end + 1].idxmax())
    return mdd, end - peak_idx


def _alpha_beta_ir(rets: pd.Series, bench: pd.Series):
    df = pd.concat([rets, bench], axis=1).dropna()
    if len(df) < 2:
        return float("nan"), float("nan"), float("nan")
    r, b = df.iloc[:, 0], df.iloc[:, 1]
    cov = np.cov(r, b, ddof=1)
    beta = cov[0, 1] / cov[1, 1] if cov[1, 1] > 0 else float("nan")
    alpha_daily = r.mean() - beta * b.mean()
    alpha = alpha_daily * _TD_PER_YEAR
    active = r - b
    ir = (
        active.mean() / active.std(ddof=1) * np.sqrt(_TD_PER_YEAR)
        if active.std(ddof=1) > 0 else float("nan")
    )
    return float(alpha), float(beta), float(ir)


def compute_metrics(
    nav_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    benchmarks_df: pd.DataFrame,
) -> dict:
    """Return a flat dict for METRICS_SCHEMA."""
    if nav_df.empty:
        return {}
    nav_df = nav_df.sort_values("date").reset_index(drop=True)
    rets = nav_df["ret_daily"]
    nav = nav_df["nav"]

    ret_total = float(nav.iloc[-1] - 1.0)
    ret_ann = annualize_return(rets)
    vol_ann = annualize_volatility(rets)
    sr = sharpe_ratio(rets)
    sor = sortino_ratio(rets)
    mdd, mdd_dur = max_drawdown(nav)
    calmar = ret_ann / abs(mdd) if mdd < 0 else float("nan")

    win_daily = float((rets > 0).sum() / max(len(rets), 1))
    # Monthly win rate: sum daily returns in each calendar month
    monthly = (
        nav_df.assign(m=nav_df["date"].dt.to_period("M"))
        .groupby("m")["ret_daily"].apply(lambda s: (1 + s).prod() - 1)
    )
    win_monthly = (
        float((monthly > 0).sum() / len(monthly))
        if len(monthly) else float("nan")
    )

    turnover_ann = (
        float(nav_df["turnover_daily"].sum() * _TD_PER_YEAR / max(len(nav_df), 1))
    )

    if not trades_df.empty:
        total_cost = float(
            (trades_df["commission"] + trades_df["stamp_tax"]
             + trades_df["transfer_fee"]
             + trades_df["slippage_value"]).sum()
        )
        total_trade_value = float(trades_df["fill_value"].sum())
        total_cost_bps = (
            total_cost / total_trade_value * 1e4
            if total_trade_value > 0 else 0.0
        )
    else:
        total_cost_bps = 0.0

    # Benchmark stats (optional)
    bench_stats = {}
    if not benchmarks_df.empty:
        bench_rets = benchmarks_df.pct_change().iloc[1:]
        nav_rets_indexed = rets.copy()
        nav_rets_indexed.index = nav_df["date"]
        for col in bench_rets.columns:
            a, b, ir = _alpha_beta_ir(nav_rets_indexed, bench_rets[col])
            bench_stats[f"alpha_vs_{col}"] = a
            bench_stats[f"beta_vs_{col}"] = b
            bench_stats[f"ir_vs_{col}"] = ir

    out = {
        "ret_annual": ret_ann, "ret_total": ret_total,
        "vol_annual": vol_ann, "sharpe": sr, "sortino": sor,
        "max_drawdown": mdd, "max_dd_duration_days": int(mdd_dur),
        "calmar": calmar,
        "win_rate_daily": win_daily, "win_rate_monthly": win_monthly,
        "turnover_annual": turnover_ann,
        "total_cost_bps": total_cost_bps,
        "lot_drag_bps": 0.0,   # placeholder until execution tracks it
    }
    out.update(bench_stats)
    return out
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/metrics.py tests/backtest/test_metrics.py
git commit -m "feat(sub-3): metrics (sharpe/dd/calmar/alpha-beta-IR)"
```

---

### Task 12: Fingerprint

**Files:**
- Create: `instock/backtest/fingerprint.py`
- Test: `tests/backtest/test_fingerprint.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_fingerprint.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from instock.backtest.fingerprint import compute_fingerprint


def _make_file(tmp_path: Path, name: str, content: bytes) -> Path:
    p = tmp_path / name
    p.write_bytes(content)
    return p


def test_fingerprint_stable_same_inputs(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"data1")
    f2 = _make_file(tmp_path, "b.parquet", b"data2")
    cfg = {"strategy": "x", "rng_seed": 42}
    h1 = compute_fingerprint([f1, f2], cfg)
    h2 = compute_fingerprint([f1, f2], cfg)
    assert h1 == h2
    assert len(h1) == 64   # sha256 hex


def test_fingerprint_changes_on_file_content(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"data1")
    cfg = {"strategy": "x"}
    h1 = compute_fingerprint([f1], cfg)
    f1.write_bytes(b"data1-modified")
    h2 = compute_fingerprint([f1], cfg)
    assert h1 != h2


def test_fingerprint_changes_on_config(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"data1")
    h1 = compute_fingerprint([f1], {"rng_seed": 42})
    h2 = compute_fingerprint([f1], {"rng_seed": 43})
    assert h1 != h2


def test_fingerprint_order_invariant(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"x")
    f2 = _make_file(tmp_path, "b.parquet", b"y")
    h1 = compute_fingerprint([f1, f2], {})
    h2 = compute_fingerprint([f2, f1], {})
    assert h1 == h2


def test_fingerprint_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        compute_fingerprint([tmp_path / "missing.parquet"], {})
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/fingerprint.py`:

```python
"""compute_fingerprint: SHA-256 of all input artifact contents + config.

Order-invariant (files sorted by SHA before hashing).

Input file SHA uses file bytes (pyarrow-compatible Parquet files are
byte-stable for identical data + pyarrow version).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable


def _file_sha256(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_fingerprint(
    input_files: Iterable[Path],
    config_dict: dict,
) -> str:
    file_shas = sorted(_file_sha256(Path(p)) for p in input_files)
    cfg_blob = json.dumps(config_dict, sort_keys=True, default=str)
    cfg_sha = hashlib.sha256(cfg_blob.encode("utf-8")).hexdigest()
    combined = hashlib.sha256()
    for s in file_shas:
        combined.update(s.encode("ascii"))
    combined.update(cfg_sha.encode("ascii"))
    return combined.hexdigest()
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/fingerprint.py tests/backtest/test_fingerprint.py
git commit -m "feat(sub-3): fingerprint SHA over inputs + config"
```

---

### Task 13: WalkForward runner

**Files:**
- Create: `instock/backtest/walkforward.py`
- Test: `tests/backtest/test_walkforward.py`

- [ ] **Step 1: Write failing tests**

File `tests/backtest/test_walkforward.py`:

```python
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from instock.backtest.walkforward import (
    WalkForwardConfig, window_bounds,
)


def test_window_bounds_basic():
    cfg = WalkForwardConfig(
        train_window_months=12, test_window_months=3, step_months=3,
    )
    windows = window_bounds(
        date(2022, 1, 1), date(2023, 12, 31), cfg
    )
    # 2022-01 -> 2022-12 train, 2023-01 -> 2023-03 test
    # then step 3m
    assert windows[0].train_start == date(2022, 1, 1)
    assert windows[0].train_end == date(2022, 12, 31)
    assert windows[0].test_start == date(2023, 1, 1)
    assert windows[0].test_end == date(2023, 3, 31)
    # check step
    assert windows[1].train_start == date(2022, 4, 1)


def test_window_bounds_truncates_final():
    cfg = WalkForwardConfig(
        train_window_months=12, test_window_months=3, step_months=3,
    )
    windows = window_bounds(
        date(2022, 1, 1), date(2023, 6, 30), cfg
    )
    assert windows[-1].test_end <= date(2023, 6, 30)


def test_window_bounds_empty_when_window_too_large():
    cfg = WalkForwardConfig(
        train_window_months=24, test_window_months=6, step_months=6,
    )
    windows = window_bounds(
        date(2023, 1, 1), date(2023, 12, 31), cfg
    )
    assert windows == []
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/backtest/walkforward.py`:

```python
"""Walk-forward window generator + IS/OOS NAV splicer.

MVP scope: window_bounds + a thin runner that splices OOS NAV across
windows. Does NOT do parameter tuning (spec §8.3 follow-up).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List

from dateutil.relativedelta import relativedelta


@dataclass(frozen=True)
class WalkForwardConfig:
    train_window_months: int
    test_window_months: int
    step_months: int
    min_train_obs: int = 200


@dataclass(frozen=True)
class Window:
    train_start: date
    train_end: date
    test_start: date
    test_end: date


def window_bounds(
    start: date, end: date, cfg: WalkForwardConfig,
) -> List[Window]:
    """Yield sliding (train, test) windows that fit in [start, end]."""
    out: List[Window] = []
    cursor_train_start = start
    while True:
        train_end = (
            cursor_train_start
            + relativedelta(months=cfg.train_window_months) - relativedelta(days=1)
        )
        test_start = train_end + relativedelta(days=1)
        test_end = (
            test_start
            + relativedelta(months=cfg.test_window_months) - relativedelta(days=1)
        )
        if test_end > end:
            # truncate last window to [test_start, end] if still valid
            if test_start <= end:
                out.append(Window(
                    train_start=cursor_train_start, train_end=train_end,
                    test_start=test_start, test_end=end,
                ))
            break
        out.append(Window(
            train_start=cursor_train_start, train_end=train_end,
            test_start=test_start, test_end=test_end,
        ))
        cursor_train_start = (
            cursor_train_start + relativedelta(months=cfg.step_months)
        )
    return out
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/backtest/walkforward.py tests/backtest/test_walkforward.py
git commit -m "feat(sub-3): walk-forward window generator"
```

---

### Task 14: HTML report

**Files:**
- Create: `instock/backtest/report.py`
- Create: `instock/backtest/template.html`
- Test: `tests/backtest/test_report.py`

- [ ] **Step 1: Write failing test**

File `tests/backtest/test_report.py`:

```python
from __future__ import annotations

import pandas as pd
import pytest

from instock.backtest.report import render_report


def test_render_report_returns_html_with_required_sections():
    nav = pd.DataFrame({
        "date": pd.bdate_range("2023-01-04", periods=60),
        "nav": [1.0 + i * 0.001 for i in range(60)],
        "cash": [1000.0] * 60, "position_value": [0.0] * 60,
        "total_value": [1_000_000.0] * 60,
        "ret_daily": [0.001] * 60, "ret_cum": [i * 0.001 for i in range(60)],
        "turnover_daily": [0.0] * 60, "n_holdings": [0] * 60,
    })
    trades = pd.DataFrame()
    metrics = {
        "run_id": "x_abcd1234", "strategy": "x",
        "ret_annual": 0.1, "ret_total": 0.06, "vol_annual": 0.01,
        "sharpe": 8.0, "max_drawdown": 0.0, "total_cost_bps": 0.0,
        "fingerprint_sha": "a" * 64,
    }
    benchmarks = pd.DataFrame()
    html = render_report(
        run_id="x_abcd1234", nav=nav, trades=trades,
        metrics=metrics, benchmarks=benchmarks,
        refdata_as_of="2026-04-18",
    )
    assert "<html" in html.lower()
    assert "x_abcd1234" in html
    assert "refdata as of 2026-04-18" in html
    assert "a" * 16 in html   # prefix of fingerprint
    # NAV chart section present
    assert "data:image/png;base64," in html


def test_render_report_handles_missing_benchmarks():
    nav = pd.DataFrame({
        "date": pd.bdate_range("2023-01-04", periods=10),
        "nav": [1.0] * 10, "cash": [0.0] * 10,
        "position_value": [0.0] * 10, "total_value": [1e6] * 10,
        "ret_daily": [0.0] * 10, "ret_cum": [0.0] * 10,
        "turnover_daily": [0.0] * 10, "n_holdings": [0] * 10,
    })
    html = render_report(
        run_id="t", nav=nav, trades=pd.DataFrame(),
        metrics={"run_id": "t", "strategy": "t",
                 "fingerprint_sha": "a" * 64, "sharpe": 0.0},
        benchmarks=pd.DataFrame(),
        refdata_as_of=None,
    )
    assert "refdata not available" in html
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement template + render**

File `instock/backtest/template.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Backtest {{ run_id }}</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, sans-serif;
         margin: 2em auto; max-width: 1000px; color: #222; }
  h1, h2 { border-bottom: 1px solid #ccc; padding-bottom: 0.2em; }
  table { border-collapse: collapse; margin: 1em 0; }
  th, td { border: 1px solid #ccc; padding: 4px 10px; text-align: right; }
  th { background: #f0f0f0; }
  .watermark { color: #999; font-size: 0.8em; margin-top: 2em; }
  img { max-width: 100%; }
</style>
</head>
<body>

<h1>Backtest Report: {{ run_id }}</h1>

<h2>Metrics</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
{% for k, v in metrics.items() %}
  <tr><td style="text-align:left">{{ k }}</td><td>{{ v }}</td></tr>
{% endfor %}
</table>

<h2>NAV Curve</h2>
{% if nav_chart %}
  <img src="data:image/png;base64,{{ nav_chart }}" alt="NAV curve">
{% else %}
  <p>No NAV data.</p>
{% endif %}

<h2>Drawdown</h2>
{% if dd_chart %}
  <img src="data:image/png;base64,{{ dd_chart }}" alt="Drawdown">
{% endif %}

<h2>Refdata</h2>
{% if refdata_as_of %}
  <p>refdata as of {{ refdata_as_of }}; historical ST/industry approximated.</p>
{% else %}
  <p>refdata not available.</p>
{% endif %}

<div class="watermark">
Fingerprint: {{ fingerprint_prefix }}
</div>

</body>
</html>
```

File `instock/backtest/report.py`:

```python
"""HTML report rendering.

Renders NAV + drawdown charts to base64-PNG and stuffs into Jinja2
template. Tolerates missing trades/benchmarks.
"""
from __future__ import annotations

import base64
import io
from pathlib import Path
from typing import Optional

import pandas as pd

_TEMPLATE_PATH = Path(__file__).parent / "template.html"


def _chart_png_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
    import matplotlib.pyplot as plt
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _nav_chart(nav: pd.DataFrame, benchmarks: pd.DataFrame) -> str:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(nav["date"], nav["nav"], label="strategy", linewidth=1.5)
    if not benchmarks.empty:
        start_vals = benchmarks.iloc[0]
        for col in benchmarks.columns:
            ax.plot(
                benchmarks.index, benchmarks[col] / start_vals[col],
                label=col, linewidth=1.0, alpha=0.7,
            )
    ax.set_title("NAV vs Benchmarks")
    ax.legend(loc="best")
    ax.grid(alpha=0.3)
    return _chart_png_b64(fig)


def _drawdown_chart(nav: pd.DataFrame) -> str:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    dd = nav["nav"] / nav["nav"].cummax() - 1.0
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.fill_between(nav["date"], dd, 0, color="red", alpha=0.3)
    ax.set_title("Drawdown")
    ax.grid(alpha=0.3)
    return _chart_png_b64(fig)


def render_report(
    run_id: str,
    nav: pd.DataFrame,
    trades: pd.DataFrame,
    metrics: dict,
    benchmarks: pd.DataFrame,
    refdata_as_of: Optional[str],
) -> str:
    from jinja2 import Template
    tmpl = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))
    nav_b64 = _nav_chart(nav, benchmarks) if not nav.empty else ""
    dd_b64 = _drawdown_chart(nav) if not nav.empty else ""
    return tmpl.render(
        run_id=run_id,
        metrics=metrics,
        nav_chart=nav_b64,
        dd_chart=dd_b64,
        refdata_as_of=refdata_as_of,
        fingerprint_prefix=str(metrics.get("fingerprint_sha", ""))[:16],
    )
```

- [ ] **Step 4: Run (expect PASS)**

```
conda run -n base python -m pytest tests/backtest/test_report.py -v
```

- [ ] **Step 5: Commit**

```
git add instock/backtest/report.py instock/backtest/template.html tests/backtest/test_report.py
git commit -m "feat(sub-3): HTML report w/ NAV + drawdown charts"
```

---

### Task 15: CLI job

**Files:**
- Create: `instock/job/backtest_run_job.py`
- Test: `tests/job/test_backtest_run_job.py`

- [ ] **Step 1: Write failing test**

File `tests/job/test_backtest_run_job.py`:

```python
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from instock.job import backtest_run_job


def test_run_once_writes_artifacts(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_BACKTEST_ROOT", str(tmp_path))

    # Stub out source + store + holding reader
    src = MagicMock()
    src.get_trade_calendar.return_value = [
        d.date() for d in pd.bdate_range("2023-01-03", "2023-01-20")
    ]
    src.get_ohlcv.return_value = pd.DataFrame()  # no benchmarks

    codes = ["600000"]
    dates = pd.bdate_range("2023-01-03", "2023-01-20")
    ohlcv = pd.DataFrame([
        {"date": d, "code": c, "open": 10.0, "high": 10.1,
         "low": 9.9, "close": 10.0, "volume": 100_000, "amount": 1e6}
        for d in dates for c in codes
    ])
    store = MagicMock()
    store.get_panel.return_value = ohlcv

    holding = pd.DataFrame([{
        "date": pd.Timestamp("2023-01-04"), "code": "600000",
        "weight": 1.0, "score": 1.0, "strategy": "stub",
    }])

    run_id = backtest_run_job.run_once(
        strategy="stub",
        start=date(2023, 1, 3),
        end=date(2023, 1, 20),
        source=src,
        store=store,
        holding=holding,
        benchmarks=(),
        write_report=True,
    )
    assert (tmp_path / run_id).is_dir()
    assert (tmp_path / run_id / "nav.parquet").exists()
    assert (tmp_path / run_id / "report.html").exists()
    assert (tmp_path / "_metrics.parquet").exists()
```

- [ ] **Step 2: Run (expect FAIL)**

- [ ] **Step 3: Implement**

File `instock/job/backtest_run_job.py`:

```python
"""CLI entry for Sub-3 backtest run.

    python -m instock.job.backtest_run_job \
        --strategy default_topq \
        --start 2022-01-04 --end 2023-12-29 \
        --benchmarks 000300.SH,000905.SH,000852.SH \
        --report
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

from instock.backtest.benchmarks import load_benchmarks
from instock.backtest.config import BacktestConfig
from instock.backtest.engine import BacktestEngine
from instock.backtest.fingerprint import compute_fingerprint
from instock.backtest.metrics import compute_metrics
from instock.backtest.report import render_report
from instock.backtest import storage as bt_storage
from instock.datasource.registry import get_source
from instock.portfolio.storage import read_holding
from instock.refdata.ohlcv_store import OhlcvPanelStore

log = logging.getLogger(__name__)


def run_once(
    *,
    strategy: str,
    start: date,
    end: date,
    source=None,
    store=None,
    holding: Optional[pd.DataFrame] = None,
    benchmarks: Sequence[str] = (
        "000300.SH", "000905.SH", "000852.SH"
    ),
    enable_fees: bool = True,
    enable_slippage: bool = True,
    slippage_bps: float = 5.0,
    write_report: bool = True,
) -> str:
    """Run one backtest; return run_id."""
    source = source or get_source()
    store = store or OhlcvPanelStore(source=source)
    if holding is None:
        holding = read_holding(
            strategy, pd.Timestamp(start), pd.Timestamp(end)
        )

    cfg = BacktestConfig(
        strategy=strategy, start=start, end=end,
        enable_fees=enable_fees, enable_slippage=enable_slippage,
        slippage_bps=slippage_bps,
        benchmarks=tuple(benchmarks),
    )

    # Fingerprint: config + holding hash (file-level); OHLCV covered via store
    fingerprint = compute_fingerprint(
        input_files=[],       # holding in-memory; CLI fills from Parquet
        config_dict=cfg.to_dict(),
    )
    run_id = (
        f"{strategy}_{start.isoformat()}_{end.isoformat()}_{fingerprint[:8]}"
    )

    engine = BacktestEngine(source=source, ohlcv_store=store)
    result = engine.run(holding_schedule=holding, config=cfg)

    bench_df = load_benchmarks(source, list(benchmarks), start, end)
    metrics = compute_metrics(
        nav_df=result["nav"], trades_df=result["trades"],
        benchmarks_df=bench_df,
    )
    metrics.update({
        "run_id": run_id, "strategy": strategy,
        "start": pd.Timestamp(start), "end": pd.Timestamp(end),
        "fingerprint_sha": fingerprint,
        "refdata_as_of": datetime.now().strftime("%Y-%m-%d"),
    })
    bt_storage.write_run(
        run_id=run_id, trades=result["trades"],
        positions=result["positions"], nav=result["nav"],
    )
    bt_storage.append_metrics(metrics)

    if write_report:
        html = render_report(
            run_id=run_id, nav=result["nav"],
            trades=result["trades"], metrics=metrics,
            benchmarks=bench_df,
            refdata_as_of=metrics["refdata_as_of"],
        )
        (bt_storage._run_dir(run_id) / "report.html").write_text(
            html, encoding="utf-8",
        )
    return run_id


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> None:
    p = argparse.ArgumentParser(description="Sub-3 backtest runner")
    p.add_argument("--strategy", required=True)
    p.add_argument("--start", required=True, type=_parse_date)
    p.add_argument("--end", required=True, type=_parse_date)
    p.add_argument("--benchmarks", default="000300.SH,000905.SH,000852.SH")
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--no-fees", action="store_true")
    p.add_argument("--no-slippage", action="store_true")
    p.add_argument("--report", action="store_true")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO)
    run_id = run_once(
        strategy=args.strategy,
        start=args.start, end=args.end,
        benchmarks=tuple(args.benchmarks.split(",")),
        enable_fees=not args.no_fees,
        enable_slippage=not args.no_slippage,
        slippage_bps=args.slippage_bps,
        write_report=args.report,
    )
    print(run_id)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run (expect PASS)**

- [ ] **Step 5: Commit**

```
git add instock/job/backtest_run_job.py tests/job/test_backtest_run_job.py
git commit -m "feat(sub-3): CLI entry backtest_run_job"
```

---

### Task 16: Clean Sub-2 occupancy

**Files:**
- Modify: `instock/portfolio/pipeline.py:184-200` (remove `_load_ohlcv_panel("ALL", ...)`)
- Modify: `tests/portfolio/test_pipeline.py` (update existing test that monkeypatches it)

- [ ] **Step 1: Read current state**

```
conda run -n base python -m pytest tests/portfolio/test_pipeline.py -v
```
Note the passing tests; they must still pass after this refactor.

- [ ] **Step 2: Rewrite `_load_ohlcv_panel` to use OhlcvPanelStore**

Edit `instock/portfolio/pipeline.py` — replace the `_load_ohlcv_panel` method with:

```python
    def _load_ohlcv_panel(
        self, codes: list[str], start: date, end: date,
    ) -> pd.DataFrame:
        """Fetch OHLCV panel via OhlcvPanelStore (cache-first).

        120-day lookback is a conservative buffer for NewListingFilter(60).
        Unit tests may monkeypatch `get_source().get_ohlcv` to bypass this.
        """
        from instock.refdata.ohlcv_store import OhlcvPanelStore
        source = get_source()
        store = OhlcvPanelStore(source=source)
        lookback = start - timedelta(days=120)
        try:
            return store.get_panel(codes, lookback, end)
        except Exception as exc:
            log.warning("failed to load OHLCV panel: %s", exc)
            return pd.DataFrame(columns=["date", "code", "volume"])
```

And update the caller in `run()` (around line 115) to pass `codes`:

```python
        # Resolve universe at `start` to scope the panel
        resolver = config.universe_resolver or _default_universe_resolver()
        panel_codes = resolver(start)
        ohlcv = self._load_ohlcv_panel(panel_codes, start, end)
```

- [ ] **Step 3: Re-run Sub-2 tests**

```
conda run -n base python -m pytest tests/portfolio/ -v
```
Expected: all previously-passing tests still pass. If any test monkeypatches `source.get_ohlcv` return value, the mock should still flow through `OhlcvPanelStore.get_panel` and return that value.

- [ ] **Step 4: If a monkeypatched test breaks, update it**

In `tests/portfolio/test_pipeline.py`, change any `monkeypatch.setattr("instock.datasource.registry.get_source", ...)` setups that assumed `"ALL"` codepath. Replace stubs to pass `codes: list[str]` explicitly.

- [ ] **Step 5: Commit**

```
git add instock/portfolio/pipeline.py tests/portfolio/test_pipeline.py
git commit -m "refactor(sub-2): replace _load_ohlcv_panel('ALL',...) with OhlcvPanelStore"
```

---

### Task 17: Real-data smoke test

**Files:**
- Create: `tests/backtest/test_real_data_smoke.py`

- [ ] **Step 1: Write smoke test**

File `tests/backtest/test_real_data_smoke.py`:

```python
from __future__ import annotations

import os
from datetime import date

import pandas as pd
import pytest

from instock.backtest.config import BacktestConfig
from instock.backtest.engine import BacktestEngine
from instock.backtest.metrics import compute_metrics
from instock.datasource.registry import get_source
from instock.refdata.ohlcv_store import OhlcvPanelStore

_SMOKE_ENABLED = os.environ.get("INSTOCK_SUB3_SMOKE") == "1"


@pytest.mark.skipif(
    not _SMOKE_ENABLED, reason="set INSTOCK_SUB3_SMOKE=1 to run"
)
def test_smoke_run_6m_window(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_BACKTEST_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_OHLCV_ROOT", str(tmp_path / "ohlcv"))

    src = get_source()
    store = OhlcvPanelStore(source=src)
    codes = ["600000", "600519", "000001"]
    dates = [pd.Timestamp("2023-07-07"), pd.Timestamp("2023-10-13")]
    holding = pd.DataFrame([
        {"date": d, "code": c, "weight": 1.0 / len(codes),
         "score": 1.0, "strategy": "smoke"}
        for d in dates for c in codes
    ])

    cfg = BacktestConfig(
        strategy="smoke",
        start=date(2023, 7, 3), end=date(2023, 12, 29),
    )
    eng = BacktestEngine(source=src, ohlcv_store=store)
    res = eng.run(holding_schedule=holding, config=cfg)

    assert len(res["trades"]) > 0
    nav = res["nav"]
    assert len(nav) > 100
    assert nav["nav"].iloc[-1] > 0
    m = compute_metrics(nav, res["trades"], pd.DataFrame())
    for k in ("ret_annual", "vol_annual", "max_drawdown",
              "turnover_annual", "total_cost_bps"):
        assert k in m and not pd.isna(m[k])
```

- [ ] **Step 2: Run default (expect skip)**

```
conda run -n base python -m pytest tests/backtest/test_real_data_smoke.py -v
```
Expected: 1 skipped.

- [ ] **Step 3: Run with flag**

```
INSTOCK_SUB3_SMOKE=1 conda run -n base python -m pytest tests/backtest/test_real_data_smoke.py -v
```
Expected: 1 passed (may take 30–90 s depending on OHLCV cache).

- [ ] **Step 4: Commit**

```
git add tests/backtest/test_real_data_smoke.py
git commit -m "test(sub-3): real-data smoke test (INSTOCK_SUB3_SMOKE=1)"
```

---

### Task 18: Roadmap + followups doc

**Files:**
- Modify: `docs/superpowers/roadmap.md` (flip Sub-3 to ✅, update milestone table)
- Create: `docs/superpowers/followups/subproject-3-backtest-engine.md`

- [ ] **Step 1: Run full test suite**

```
conda run -n base python -m pytest -v
```
Expected: all prior tests + new backtest tests pass.

- [ ] **Step 2: Write follow-up doc**

File `docs/superpowers/followups/subproject-3-backtest-engine.md`:

```markdown
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
```

- [ ] **Step 3: Update roadmap**

Edit `docs/superpowers/roadmap.md`:
- Change Sub-3 status from `🎯 下一步` to `✅ 已合并`
- Update milestone table row Sub-3: `✅ 已合并` + actual task count = 18
- Add `follow-up`: pointer to new file

Find the Sub-3 section (starts with `### Sub-project 3 ⏳ 回测与交易模拟引擎`) and prefix with:

```markdown
### Sub-project 3 ✅ 回测与交易模拟引擎（已合并到 master，tag `subproject-3-backtest-engine-mvp`）

**实际交付**（MVP）：
- `instock/backtest/` 包：schemas + config + costs + constraints + portfolio_state
  + execution + engine + storage + benchmarks + metrics + fingerprint
  + walkforward + report（template.html）
- `instock/job/backtest_run_job.py` CLI 入口
- Sub-2 pipeline `_load_ohlcv_panel` 替换为 `OhlcvPanelStore` 注入（清 Sub-2.5 遗留）
- 真数据冒烟测试（`INSTOCK_SUB3_SMOKE=1`）

**测试**: <填最终数字>。无 FutureWarning 新增。

**MVP 验收已达成**：
- 在 2023-07 → 2023-12 窗口跑通；trades/nav/metrics/report 全产出
- 手续费 / 滑点可 CLI 关闭
- fingerprint 双跑一致（单测）
- HTML 报告含 refdata 水印

**follow-up**: `docs/superpowers/followups/subproject-3-backtest-engine.md`

---

### ~~Sub-project 3 ⏳ 回测与交易模拟引擎~~  <!-- 旧文案保留为历史记录 -->
```

And in the 里程碑汇总表, add/update row:

```markdown
| 3 | 回测与交易模拟 | 策略历史可验证 | ✅ 已合并 | 18（实际） |
```

- [ ] **Step 4: Commit**

```
git add docs/superpowers/roadmap.md docs/superpowers/followups/subproject-3-backtest-engine.md
git commit -m "docs(sub-3): mark shipped, link followups, update milestone table"
```

- [ ] **Step 5: Tag**

```
git tag subproject-3-backtest-engine-mvp
```

---

## Self-Review Notes

- **Spec §1 (scope 6 deliverables)**: Tasks 8 (engine), 5 (constraints), 3/4 (costs), 11 (metrics), 13 (walkforward), 14 (report) ✅
- **Spec §3 (data contracts)**: Task 1 (schemas) covers trade/position/nav/metrics ✅
- **Spec §4 (event loop)**: Task 8 implements pending-orders, T+1, mark-to-market, defer on suspend ✅
- **Spec §5 (cost model)**: Task 3/4 ✅
- **Spec §6 (constraints)**: Task 5 ✅; T+1 enforced implicitly by engine pending-queue design, not a separate constraint class (noted in engine docstring)
- **Spec §7 (metrics + multi-benchmark)**: Task 10 + 11 ✅
- **Spec §8 (walk-forward)**: Task 13 — MVP only generates windows; full splicing/diagnostics flagged in followups §A ✅
- **Spec §9 (fingerprint)**: Task 12; CLI path caveat in followups §A ✅
- **Spec §10 (CLI)**: Task 15 ✅
- **Spec §11 (Sub-2.5 integration)**: Task 16 ✅
- **Spec §12 (tests ≥40)**: Task count: schemas 6 + config 3 + costs 11 + constraints 11 + portfolio_state 6 + execution 4 + engine 3 + storage 3 + benchmarks 3 + metrics 6 + fingerprint 5 + walkforward 3 + report 2 + CLI 1 + smoke 1 = **68** ✅
- **Spec §13 (known trade-offs)**: Mirrored in followups ✅
- **Spec §14 (Sub-4 entry bar)**: followups §D ✅
- **No placeholders**: every step has concrete code or exact command
- **Type consistency**: `Order`, `PortfolioState`, `BacktestConfig`, `ConstraintChain`, `TRADE_SCHEMA` etc. used consistently across tasks
