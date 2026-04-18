# 数据 & 因子工程 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 InStock 项目中建立一条可信的数据管道（akshare → MySQL/Parquet）和一个可复用的因子平台，替代当前散落、无校验、无 PIT 的爬虫+指标链路。

**Architecture:** 引入 `instock/datasource/` 做"源无关"的数据接入层（pandera schema 守门 + retry/timeout 装饰器 + akshare 实现 + Tushare 空壳）；引入 `instock/factors/` 做因子库（Factor 基类 + 四类首期因子 + 预处理管线 + Parquet 存储 + IC/分层评估器）。老爬虫保留但不入主链路。PIT 财务通过 `announcement_date` 列支持 as-of 查询。

**Tech Stack:** Python 3.11, pandas 2.3, akshare, pandera, pyarrow, SQLAlchemy 2, PyMySQL, TA-Lib, Jinja2, matplotlib, pytest。

**Spec:** `docs/superpowers/specs/2026-04-18-data-factor-engineering-design.md`

---

## 文件结构

```
instock/
├─ datasource/
│   ├─ __init__.py
│   ├─ base.py              # IDataSource Protocol + 异常类
│   ├─ schemas.py           # pandera schema 合约
│   ├─ io.py                # retry / timeout / rate-limit 装饰器
│   ├─ akshare_source.py    # akshare 实现，每个方法出口走 schema.validate
│   ├─ tushare_source.py    # 接口就位，方法 raise NotImplementedError
│   └─ registry.py          # get_source() 单例工厂
├─ factors/
│   ├─ __init__.py
│   ├─ base.py              # Factor ABC
│   ├─ registry.py          # 因子全局注册表
│   ├─ storage.py           # Parquet 读写（按年分区）
│   ├─ preprocess.py        # winsorize / neutralize / zscore
│   ├─ evaluator.py         # FactorReport + IC/分层/衰减
│   ├─ report_template.html # Jinja2 模板
│   ├─ technical/
│   │   ├─ __init__.py
│   │   └─ momentum.py      # mom_5d / mom_20d / mom_60d 等
│   ├─ fundamental/
│   │   ├─ __init__.py
│   │   └─ valuation.py     # pe_ttm / pb / ps / roe_ttm 等
│   ├─ lhb/
│   │   ├─ __init__.py
│   │   └─ heat.py          # lhb_heat / lhb_seat_winrate 等
│   └─ flow/
│       ├─ __init__.py
│       └─ north.py         # north_holding_chg_5d 等
├─ job/
│   └─ factor_compute_daily_job.py   # 新增日频作业
└─ sql/
    └─ 20260418_create_pit_tables.sql  # 新表 DDL

tests/
├─ datasource/
│   ├─ test_schemas.py
│   ├─ test_io.py
│   ├─ test_akshare_source.py
│   └─ fixtures/            # akshare 响应固化样本
├─ factors/
│   ├─ test_base.py
│   ├─ test_preprocess.py
│   ├─ test_storage.py
│   ├─ test_evaluator.py
│   ├─ test_technical.py
│   ├─ test_fundamental.py
│   ├─ test_lhb.py
│   └─ test_flow.py
└─ conftest.py              # 共享 fixture：临时 MySQL / 临时 Parquet 根目录
```

每个文件单一职责，方便独立测试和后续替换（例如把 akshare 换成 Tushare 只改 `akshare_source.py`）。

---

## Task 0: 环境与依赖

**Files:**
- Modify: `requirements.txt`
- Create: `tests/__init__.py`, `tests/conftest.py`

- [ ] **Step 1: 追加新依赖到 requirements.txt**

打开 `requirements.txt`，末尾追加：

```
pandera==0.20.4
pyarrow==17.0.0
Jinja2==3.1.4
matplotlib==3.9.2
pytest==8.3.3
pytest-mock==3.14.0
akshare==1.15.92
```

- [ ] **Step 2: 安装依赖**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pip install -r requirements.txt`
Expected: 所有包安装成功，无冲突。

- [ ] **Step 3: 建立 tests 目录骨架**

Create `tests/__init__.py`（空文件）。

Create `tests/conftest.py`:

```python
import os
import tempfile
import pytest


@pytest.fixture
def tmp_factor_root(tmp_path, monkeypatch):
    """将 Parquet 因子根目录临时指向 tmp_path 子目录。"""
    root = tmp_path / "factors"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_FACTOR_ROOT", str(root))
    return root
```

- [ ] **Step 4: 确认 pytest 能发现目录**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest --collect-only tests/ 2>&1 | head -5`
Expected: `no tests ran`（因为还没有测试），但无 import 错误。

- [ ] **Step 5: 提交**

```bash
git add requirements.txt tests/__init__.py tests/conftest.py
git commit -m "chore: add deps and tests scaffold for datasource+factors"
```

---

## Task 1: DataSource Protocol 与异常

**Files:**
- Create: `instock/datasource/__init__.py`, `instock/datasource/base.py`
- Test: `tests/datasource/__init__.py`, `tests/datasource/test_base.py`

- [ ] **Step 1: 写失败测试**

Create `tests/datasource/__init__.py`（空）。

Create `tests/datasource/test_base.py`:

```python
import pytest
from instock.datasource.base import (
    IDataSource,
    DataSourceError,
    SchemaValidationError,
)


def test_exceptions_are_importable():
    assert issubclass(DataSourceError, Exception)
    assert issubclass(SchemaValidationError, DataSourceError)


def test_protocol_has_required_methods():
    required = {
        "get_ohlcv", "get_fundamentals_pit", "get_lhb",
        "get_north_bound", "get_money_flow",
        "get_index_member", "get_trade_calendar",
    }
    assert required.issubset(set(dir(IDataSource)))
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/datasource/test_base.py -v`
Expected: FAIL `ModuleNotFoundError: instock.datasource`

- [ ] **Step 3: 最小实现**

Create `instock/datasource/__init__.py`（空）。

Create `instock/datasource/base.py`:

```python
from __future__ import annotations
from datetime import date
from typing import Protocol, runtime_checkable

import pandas as pd


class DataSourceError(Exception):
    """Base error for the datasource layer."""


class SchemaValidationError(DataSourceError):
    """Raised when a DataSource method's return fails its pandera contract."""


@runtime_checkable
class IDataSource(Protocol):
    def get_ohlcv(
        self, code: str, start: date, end: date, adjust: str = "qfq"
    ) -> pd.DataFrame: ...

    def get_fundamentals_pit(
        self, code: str, fields: list[str]
    ) -> pd.DataFrame: ...

    def get_lhb(self, start: date, end: date) -> pd.DataFrame: ...

    def get_north_bound(self, start: date, end: date) -> pd.DataFrame: ...

    def get_money_flow(
        self, code: str, start: date, end: date
    ) -> pd.DataFrame: ...

    def get_index_member(self, index_code: str, at: date) -> list[str]: ...

    def get_trade_calendar(self, start: date, end: date) -> list[date]: ...
```

- [ ] **Step 4: 运行测试确认通过**

Run: `pytest tests/datasource/test_base.py -v`
Expected: 2 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/datasource/__init__.py instock/datasource/base.py \
        tests/datasource/__init__.py tests/datasource/test_base.py
git commit -m "feat(datasource): add IDataSource protocol and error types"
```

---

## Task 2: Pandera Schemas

**Files:**
- Create: `instock/datasource/schemas.py`
- Test: `tests/datasource/test_schemas.py`

- [ ] **Step 1: 写失败测试**

Create `tests/datasource/test_schemas.py`:

```python
import pandas as pd
import pytest
from instock.datasource import schemas


def _ohlcv_row():
    return {
        "date": pd.Timestamp("2024-01-02"),
        "code": "600519",
        "open": 1700.0, "high": 1720.0, "low": 1695.0, "close": 1710.0,
        "volume": 1_000_000.0, "amount": 1.7e9,
    }


def test_ohlcv_schema_accepts_valid():
    df = pd.DataFrame([_ohlcv_row()])
    validated = schemas.OHLCV_SCHEMA.validate(df)
    assert len(validated) == 1


def test_ohlcv_schema_rejects_bad_code():
    bad = _ohlcv_row() | {"code": "ABC"}
    df = pd.DataFrame([bad])
    with pytest.raises(Exception):
        schemas.OHLCV_SCHEMA.validate(df)


def test_ohlcv_schema_rejects_nonpositive_price():
    bad = _ohlcv_row() | {"close": 0.0}
    df = pd.DataFrame([bad])
    with pytest.raises(Exception):
        schemas.OHLCV_SCHEMA.validate(df)


def test_ohlcv_schema_unique_key():
    row = _ohlcv_row()
    df = pd.DataFrame([row, row])
    with pytest.raises(Exception):
        schemas.OHLCV_SCHEMA.validate(df)


def test_fundamentals_pit_schema_requires_announcement_date():
    df = pd.DataFrame([{
        "code": "600519",
        "report_period": pd.Timestamp("2023-09-30"),
        "announcement_date": pd.Timestamp("2023-10-28"),
        "revenue": 1.0e10,
    }])
    validated = schemas.FUNDAMENTALS_PIT_SCHEMA.validate(df)
    assert "announcement_date" in validated.columns


def test_lhb_schema_smoke():
    df = pd.DataFrame([{
        "trade_date": pd.Timestamp("2024-01-02"),
        "code": "600519",
        "seat": "中信证券北京建国路",
        "buy_amount": 1.0e8,
        "sell_amount": 5.0e7,
        "reason": "日涨幅偏离值达7%",
    }])
    schemas.LHB_SCHEMA.validate(df)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/datasource/test_schemas.py -v`
Expected: FAIL (schemas 不存在)

- [ ] **Step 3: 实现 schemas**

Create `instock/datasource/schemas.py`:

```python
"""pandera schema contracts for IDataSource method returns."""
from __future__ import annotations

import pandera.pandas as pa


OHLCV_SCHEMA = pa.DataFrameSchema(
    {
        "date":   pa.Column("datetime64[ns]"),
        "code":   pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "open":   pa.Column(float, pa.Check.gt(0)),
        "high":   pa.Column(float, pa.Check.gt(0)),
        "low":    pa.Column(float, pa.Check.gt(0)),
        "close":  pa.Column(float, pa.Check.gt(0)),
        "volume": pa.Column(float, pa.Check.ge(0)),
        "amount": pa.Column(float, pa.Check.ge(0)),
    },
    unique=["date", "code"],
    strict=False,
    coerce=True,
)


FUNDAMENTALS_PIT_SCHEMA = pa.DataFrameSchema(
    {
        "code":              pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "report_period":     pa.Column("datetime64[ns]"),
        "announcement_date": pa.Column("datetime64[ns]"),
    },
    unique=["code", "report_period"],
    strict=False,
    coerce=True,
)


LHB_SCHEMA = pa.DataFrameSchema(
    {
        "trade_date":  pa.Column("datetime64[ns]"),
        "code":        pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "seat":        pa.Column(str),
        "buy_amount":  pa.Column(float, pa.Check.ge(0)),
        "sell_amount": pa.Column(float, pa.Check.ge(0)),
        "reason":      pa.Column(str, nullable=True),
    },
    strict=False,
    coerce=True,
)


NORTH_BOUND_SCHEMA = pa.DataFrameSchema(
    {
        "trade_date":     pa.Column("datetime64[ns]"),
        "code":           pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "hold_shares":    pa.Column(float, pa.Check.ge(0)),
        "hold_ratio":     pa.Column(float, nullable=True),
    },
    unique=["trade_date", "code"],
    strict=False,
    coerce=True,
)


MONEY_FLOW_SCHEMA = pa.DataFrameSchema(
    {
        "trade_date":       pa.Column("datetime64[ns]"),
        "code":             pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
        "main_net_inflow":  pa.Column(float),
        "big_order_inflow": pa.Column(float),
    },
    unique=["trade_date", "code"],
    strict=False,
    coerce=True,
)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `pytest tests/datasource/test_schemas.py -v`
Expected: 6 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/datasource/schemas.py tests/datasource/test_schemas.py
git commit -m "feat(datasource): add pandera schemas for OHLCV/PIT/LHB/north/flow"
```

---

## Task 3: IO 装饰器（retry / timeout / rate-limit）

**Files:**
- Create: `instock/datasource/io.py`
- Test: `tests/datasource/test_io.py`

- [ ] **Step 1: 写失败测试**

Create `tests/datasource/test_io.py`:

```python
import time
import pytest
from instock.datasource.io import with_retry, RateLimiter
from instock.datasource.base import DataSourceError


def test_with_retry_returns_on_success():
    calls = {"n": 0}

    @with_retry(max_attempts=3, base_delay=0.0)
    def ok():
        calls["n"] += 1
        return 42

    assert ok() == 42
    assert calls["n"] == 1


def test_with_retry_retries_then_succeeds():
    calls = {"n": 0}

    @with_retry(max_attempts=3, base_delay=0.0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("boom")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_with_retry_raises_datasource_error_after_exhaustion():
    @with_retry(max_attempts=2, base_delay=0.0)
    def always_fails():
        raise RuntimeError("down")

    with pytest.raises(DataSourceError):
        always_fails()


def test_rate_limiter_enforces_min_interval():
    rl = RateLimiter(min_interval=0.05)
    t0 = time.perf_counter()
    rl.wait()
    rl.wait()
    elapsed = time.perf_counter() - t0
    assert elapsed >= 0.05
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/datasource/test_io.py -v`
Expected: FAIL (io 模块不存在)

- [ ] **Step 3: 实现**

Create `instock/datasource/io.py`:

```python
"""Retry / rate-limit helpers used by every DataSource implementation."""
from __future__ import annotations

import functools
import logging
import time
from typing import Callable, TypeVar

from .base import DataSourceError

log = logging.getLogger(__name__)
T = TypeVar("T")


def with_retry(max_attempts: int = 3, base_delay: float = 0.5) -> Callable:
    """Exponential-backoff retry. Wraps final failure in DataSourceError."""

    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    sleep_for = base_delay * (2 ** (attempt - 1))
                    log.warning(
                        "retry %s/%s on %s: %s",
                        attempt, max_attempts, fn.__name__, exc,
                    )
                    time.sleep(sleep_for)
            raise DataSourceError(
                f"{fn.__name__} failed after {max_attempts} attempts: {last_exc}"
            ) from last_exc

        return wrapper

    return deco


class RateLimiter:
    """Simple monotonic-time min-interval limiter."""

    def __init__(self, min_interval: float = 0.2) -> None:
        self._min = min_interval
        self._last = 0.0

    def wait(self) -> None:
        now = time.perf_counter()
        delta = now - self._last
        if delta < self._min:
            time.sleep(self._min - delta)
        self._last = time.perf_counter()
```

- [ ] **Step 4: 运行测试确认通过**

Run: `pytest tests/datasource/test_io.py -v`
Expected: 4 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/datasource/io.py tests/datasource/test_io.py
git commit -m "feat(datasource): add retry decorator and rate limiter"
```

---

## Task 4: AkShareSource.get_ohlcv + Tushare 空壳 + registry

**Files:**
- Create: `instock/datasource/akshare_source.py`, `instock/datasource/tushare_source.py`, `instock/datasource/registry.py`
- Test: `tests/datasource/test_akshare_source.py`, `tests/datasource/test_registry.py`

- [ ] **Step 1: 写 akshare_source 的 OHLCV 测试**

Create `tests/datasource/test_akshare_source.py`:

```python
from datetime import date
import pandas as pd
import pytest

from instock.datasource.akshare_source import AkShareSource
from instock.datasource.base import SchemaValidationError


def _ak_ohlcv_fixture():
    """Mimic akshare.stock_zh_a_hist response columns."""
    return pd.DataFrame({
        "日期":    ["2024-01-02", "2024-01-03"],
        "开盘":    [1700.0, 1710.0],
        "收盘":    [1710.0, 1705.0],
        "最高":    [1720.0, 1715.0],
        "最低":    [1695.0, 1700.0],
        "成交量":  [1_000_000, 900_000],
        "成交额":  [1.7e9, 1.5e9],
    })


def test_get_ohlcv_normalises_columns(mocker):
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_zh_a_hist",
        return_value=_ak_ohlcv_fixture(),
    )
    src = AkShareSource()
    df = src.get_ohlcv("600519", date(2024, 1, 1), date(2024, 1, 5))

    assert list(df.columns) == [
        "date", "code", "open", "high", "low", "close", "volume", "amount",
    ]
    assert df["code"].iloc[0] == "600519"
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_get_ohlcv_schema_violation_raises(mocker):
    bad = _ak_ohlcv_fixture()
    bad.loc[0, "收盘"] = 0.0  # violates gt(0)
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_zh_a_hist",
        return_value=bad,
    )
    src = AkShareSource()
    with pytest.raises(SchemaValidationError):
        src.get_ohlcv("600519", date(2024, 1, 1), date(2024, 1, 5))
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/datasource/test_akshare_source.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: 实现 akshare_source（先只有 get_ohlcv）**

Create `instock/datasource/akshare_source.py`:

```python
"""AkShare-backed IDataSource implementation."""
from __future__ import annotations

from datetime import date
from typing import Any

import akshare as ak
import pandas as pd
import pandera.errors as pa_errors

from .base import IDataSource, DataSourceError, SchemaValidationError
from .io import RateLimiter, with_retry
from . import schemas


_OHLCV_RENAME = {
    "日期": "date", "开盘": "open", "收盘": "close",
    "最高": "high", "最低": "low",
    "成交量": "volume", "成交额": "amount",
}


class AkShareSource(IDataSource):
    def __init__(self) -> None:
        self._limiter = RateLimiter(min_interval=0.2)

    # ---------- OHLCV ----------
    @with_retry(max_attempts=3, base_delay=0.5)
    def get_ohlcv(
        self, code: str, start: date, end: date, adjust: str = "qfq"
    ) -> pd.DataFrame:
        self._limiter.wait()
        raw = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust=adjust,
        )
        if raw is None or raw.empty:
            return _empty_like(schemas.OHLCV_SCHEMA)

        df = raw.rename(columns=_OHLCV_RENAME).copy()
        df["date"] = pd.to_datetime(df["date"])
        df["code"] = code
        df = df[["date", "code", "open", "high", "low", "close", "volume", "amount"]]
        df[["open", "high", "low", "close", "volume", "amount"]] = (
            df[["open", "high", "low", "close", "volume", "amount"]].astype(float)
        )
        return _validate(schemas.OHLCV_SCHEMA, df)

    # ---------- stubs for later tasks ----------
    def get_fundamentals_pit(self, code: str, fields: list[str]) -> pd.DataFrame:
        raise NotImplementedError

    def get_lhb(self, start: date, end: date) -> pd.DataFrame:
        raise NotImplementedError

    def get_north_bound(self, start: date, end: date) -> pd.DataFrame:
        raise NotImplementedError

    def get_money_flow(self, code: str, start: date, end: date) -> pd.DataFrame:
        raise NotImplementedError

    def get_index_member(self, index_code: str, at: date) -> list[str]:
        raise NotImplementedError

    def get_trade_calendar(self, start: date, end: date) -> list[date]:
        raise NotImplementedError


def _empty_like(schema: Any) -> pd.DataFrame:
    cols = list(schema.columns.keys())
    return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})


def _validate(schema: Any, df: pd.DataFrame) -> pd.DataFrame:
    try:
        return schema.validate(df, lazy=True)
    except (pa_errors.SchemaError, pa_errors.SchemaErrors) as err:
        raise SchemaValidationError(str(err)) from err
```

- [ ] **Step 4: 运行 akshare 测试**

Run: `pytest tests/datasource/test_akshare_source.py -v`
Expected: 2 passed.

- [ ] **Step 5: Tushare 空壳**

Create `instock/datasource/tushare_source.py`:

```python
"""Placeholder Tushare implementation. All methods raise NotImplementedError
until a Pro subscription and mapping are added."""
from __future__ import annotations

from datetime import date
import pandas as pd

from .base import IDataSource


class TushareSource(IDataSource):
    def get_ohlcv(self, code, start, end, adjust="qfq"):
        raise NotImplementedError("TushareSource not enabled")
    def get_fundamentals_pit(self, code, fields):
        raise NotImplementedError("TushareSource not enabled")
    def get_lhb(self, start, end):
        raise NotImplementedError("TushareSource not enabled")
    def get_north_bound(self, start, end):
        raise NotImplementedError("TushareSource not enabled")
    def get_money_flow(self, code, start, end):
        raise NotImplementedError("TushareSource not enabled")
    def get_index_member(self, index_code, at):
        raise NotImplementedError("TushareSource not enabled")
    def get_trade_calendar(self, start, end):
        raise NotImplementedError("TushareSource not enabled")
```

- [ ] **Step 6: registry 测试**

Create `tests/datasource/test_registry.py`:

```python
import pytest
from instock.datasource.registry import get_source
from instock.datasource.akshare_source import AkShareSource


def test_default_returns_akshare():
    src = get_source()
    assert isinstance(src, AkShareSource)


def test_unknown_name_raises():
    with pytest.raises(ValueError):
        get_source("nonexistent")


def test_tushare_not_enabled():
    with pytest.raises(ValueError):
        get_source("tushare")
```

Run: `pytest tests/datasource/test_registry.py -v`
Expected: FAIL (module not found)

- [ ] **Step 7: 实现 registry**

Create `instock/datasource/registry.py`:

```python
"""DataSource factory. First phase registers only AkShareSource."""
from __future__ import annotations

from functools import lru_cache

from .akshare_source import AkShareSource
from .base import IDataSource


_REGISTRY = {"akshare": AkShareSource}


@lru_cache(maxsize=None)
def get_source(name: str = "akshare") -> IDataSource:
    if name not in _REGISTRY:
        raise ValueError(
            f"unknown data source '{name}'. Registered: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]()
```

- [ ] **Step 8: 运行 registry 测试**

Run: `pytest tests/datasource/test_registry.py -v`
Expected: 3 passed.

- [ ] **Step 9: 提交**

```bash
git add instock/datasource/akshare_source.py \
        instock/datasource/tushare_source.py \
        instock/datasource/registry.py \
        tests/datasource/test_akshare_source.py \
        tests/datasource/test_registry.py
git commit -m "feat(datasource): akshare OHLCV + tushare stub + registry"
```

---

## Task 5: AkShareSource 剩余方法 + trade_calendar + index_member

**Files:**
- Modify: `instock/datasource/akshare_source.py`
- Test: extend `tests/datasource/test_akshare_source.py`

- [ ] **Step 1: 为 get_fundamentals_pit 写测试**

Append to `tests/datasource/test_akshare_source.py`:

```python
def _ak_fin_indicator_fixture():
    # mimics akshare.stock_financial_abstract output columns seen in the wild
    return pd.DataFrame({
        "报告期":       ["20230930", "20230630"],
        "公告日期":     ["20231028", "20230829"],
        "营业收入":     [1.0e10, 9.0e9],
        "净利润":       [1.5e9, 1.2e9],
        "净资产收益率": [18.5, 15.2],
    })


def test_get_fundamentals_pit_normalises(mocker):
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_financial_abstract",
        return_value=_ak_fin_indicator_fixture(),
    )
    src = AkShareSource()
    df = src.get_fundamentals_pit("600519", ["revenue", "net_profit", "roe"])

    assert "announcement_date" in df.columns
    assert "report_period" in df.columns
    assert df["code"].iloc[0] == "600519"
    assert pd.api.types.is_datetime64_any_dtype(df["announcement_date"])
    assert pd.api.types.is_datetime64_any_dtype(df["report_period"])


def _ak_lhb_fixture():
    return pd.DataFrame({
        "交易日":      ["2024-01-02"],
        "代码":        ["600519"],
        "营业部名称":  ["中信证券北京建国路"],
        "买入金额":    [1.0e8],
        "卖出金额":    [5.0e7],
        "上榜原因":    ["日涨幅偏离值达7%"],
    })


def test_get_lhb_normalises(mocker):
    mocker.patch(
        "instock.datasource.akshare_source.ak.stock_lhb_detail_em",
        return_value=_ak_lhb_fixture(),
    )
    src = AkShareSource()
    df = src.get_lhb(date(2024, 1, 1), date(2024, 1, 5))
    assert list(df.columns) == [
        "trade_date", "code", "seat", "buy_amount", "sell_amount", "reason",
    ]


def test_get_trade_calendar_returns_list_of_dates(mocker):
    fake = pd.DataFrame({"trade_date": pd.to_datetime(
        ["2024-01-02", "2024-01-03", "2024-01-04"]
    )})
    mocker.patch(
        "instock.datasource.akshare_source.ak.tool_trade_date_hist_sina",
        return_value=fake,
    )
    src = AkShareSource()
    cal = src.get_trade_calendar(date(2024, 1, 1), date(2024, 1, 31))
    assert cal == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/datasource/test_akshare_source.py -v -k "fundamentals or lhb or trade_calendar"`
Expected: FAIL (NotImplementedError)

- [ ] **Step 3: 实现剩余方法**

Edit `instock/datasource/akshare_source.py`, replace stub methods with:

```python
_FIN_RENAME = {
    "报告期": "report_period",
    "公告日期": "announcement_date",
    "营业收入": "revenue",
    "净利润": "net_profit",
    "总资产": "total_assets",
    "净资产": "total_equity",
    "经营活动现金流量净额": "operating_cf",
    "净资产收益率": "roe",
    "毛利率": "gross_margin",
    "净利率": "net_margin",
    "市盈率": "pe",
    "市净率": "pb",
    "市销率": "ps",
}


_LHB_RENAME = {
    "交易日": "trade_date",
    "代码": "code",
    "营业部名称": "seat",
    "买入金额": "buy_amount",
    "卖出金额": "sell_amount",
    "上榜原因": "reason",
}


    # replaces the get_fundamentals_pit stub
    @with_retry(max_attempts=3, base_delay=0.5)
    def get_fundamentals_pit(
        self, code: str, fields: list[str]
    ) -> pd.DataFrame:
        self._limiter.wait()
        raw = ak.stock_financial_abstract(symbol=code)
        if raw is None or raw.empty:
            return _empty_like(schemas.FUNDAMENTALS_PIT_SCHEMA)
        df = raw.rename(columns=_FIN_RENAME).copy()
        df["code"] = code
        df["report_period"] = pd.to_datetime(df["report_period"])
        df["announcement_date"] = pd.to_datetime(df["announcement_date"])
        # Keep requested fields plus the mandatory three.
        wanted = ["code", "report_period", "announcement_date"] + [
            f for f in fields if f in df.columns
        ]
        df = df[wanted]
        return _validate(schemas.FUNDAMENTALS_PIT_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def get_lhb(self, start: date, end: date) -> pd.DataFrame:
        self._limiter.wait()
        raw = ak.stock_lhb_detail_em(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        if raw is None or raw.empty:
            return _empty_like(schemas.LHB_SCHEMA)
        df = raw.rename(columns=_LHB_RENAME).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["buy_amount"] = df["buy_amount"].astype(float)
        df["sell_amount"] = df["sell_amount"].astype(float)
        df["seat"] = df["seat"].astype(str)
        df["reason"] = df["reason"].astype(str)
        df = df[["trade_date", "code", "seat", "buy_amount", "sell_amount", "reason"]]
        return _validate(schemas.LHB_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def get_north_bound(self, start: date, end: date) -> pd.DataFrame:
        self._limiter.wait()
        raw = ak.stock_hsgt_hold_stock_em()  # daily snapshot; filter below
        if raw is None or raw.empty:
            return _empty_like(schemas.NORTH_BOUND_SCHEMA)
        df = raw.rename(
            columns={"持股日期": "trade_date", "股票代码": "code",
                     "持股数量": "hold_shares", "持股市值比例": "hold_ratio"}
        ).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df[(df["trade_date"] >= pd.Timestamp(start)) &
                (df["trade_date"] <= pd.Timestamp(end))]
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["hold_shares"] = df["hold_shares"].astype(float)
        df["hold_ratio"] = pd.to_numeric(df.get("hold_ratio"), errors="coerce")
        df = df[["trade_date", "code", "hold_shares", "hold_ratio"]]
        return _validate(schemas.NORTH_BOUND_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def get_money_flow(
        self, code: str, start: date, end: date
    ) -> pd.DataFrame:
        self._limiter.wait()
        raw = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith(("6", "9")) else "sz")
        if raw is None or raw.empty:
            return _empty_like(schemas.MONEY_FLOW_SCHEMA)
        df = raw.rename(columns={
            "日期": "trade_date",
            "主力净流入-净额": "main_net_inflow",
            "大单净流入-净额": "big_order_inflow",
        }).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df[(df["trade_date"] >= pd.Timestamp(start)) &
                (df["trade_date"] <= pd.Timestamp(end))]
        df["code"] = code
        df["main_net_inflow"] = df["main_net_inflow"].astype(float)
        df["big_order_inflow"] = df["big_order_inflow"].astype(float)
        df = df[["trade_date", "code", "main_net_inflow", "big_order_inflow"]]
        return _validate(schemas.MONEY_FLOW_SCHEMA, df)

    @with_retry(max_attempts=3, base_delay=0.5)
    def get_index_member(self, index_code: str, at: date) -> list[str]:
        self._limiter.wait()
        raw = ak.index_stock_cons(symbol=index_code)
        if raw is None or raw.empty:
            return []
        col = "品种代码" if "品种代码" in raw.columns else raw.columns[0]
        return sorted(raw[col].astype(str).str.zfill(6).unique().tolist())

    @with_retry(max_attempts=3, base_delay=0.5)
    def get_trade_calendar(self, start: date, end: date) -> list[date]:
        self._limiter.wait()
        raw = ak.tool_trade_date_hist_sina()
        ts = pd.to_datetime(raw["trade_date"])
        mask = (ts >= pd.Timestamp(start)) & (ts <= pd.Timestamp(end))
        return [d.date() for d in ts[mask]]
```

- [ ] **Step 4: 运行全部测试**

Run: `pytest tests/datasource/ -v`
Expected: 全通过（至少 16 passed）。

- [ ] **Step 5: 提交**

```bash
git add instock/datasource/akshare_source.py tests/datasource/test_akshare_source.py
git commit -m "feat(datasource): implement fundamentals_pit/lhb/north/flow/calendar"
```

---

## Task 6: MySQL 新表 DDL

**Files:**
- Create: `instock/sql/20260418_create_pit_tables.sql`

- [ ] **Step 1: 写 SQL**

Create `instock/sql/20260418_create_pit_tables.sql`:

```sql
-- PIT financials: first-announcement-date only (per spec).
CREATE TABLE IF NOT EXISTS cn_stock_fundamentals_pit (
    code              VARCHAR(10)   NOT NULL,
    report_period     DATE          NOT NULL,
    announcement_date DATE          NOT NULL,
    revenue           DECIMAL(20,4) NULL,
    net_profit        DECIMAL(20,4) NULL,
    total_assets      DECIMAL(20,4) NULL,
    total_equity      DECIMAL(20,4) NULL,
    operating_cf      DECIMAL(20,4) NULL,
    roe               DECIMAL(10,4) NULL,
    gross_margin      DECIMAL(10,4) NULL,
    net_margin        DECIMAL(10,4) NULL,
    pe                DECIMAL(10,4) NULL,
    pb                DECIMAL(10,4) NULL,
    ps                DECIMAL(10,4) NULL,
    PRIMARY KEY (code, report_period),
    INDEX idx_pit_ann (code, announcement_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cn_stock_lhb_v2 (
    trade_date   DATE          NOT NULL,
    code         VARCHAR(10)   NOT NULL,
    seat         VARCHAR(128)  NOT NULL,
    buy_amount   DECIMAL(20,4) NOT NULL,
    sell_amount  DECIMAL(20,4) NOT NULL,
    reason       VARCHAR(256)  NULL,
    PRIMARY KEY (trade_date, code, seat),
    INDEX idx_lhb_code (code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cn_stock_north_bound (
    trade_date   DATE          NOT NULL,
    code         VARCHAR(10)   NOT NULL,
    hold_shares  DECIMAL(24,4) NOT NULL,
    hold_ratio   DECIMAL(10,6) NULL,
    PRIMARY KEY (trade_date, code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cn_stock_money_flow (
    trade_date       DATE          NOT NULL,
    code             VARCHAR(10)   NOT NULL,
    main_net_inflow  DECIMAL(24,4) NOT NULL,
    big_order_inflow DECIMAL(24,4) NOT NULL,
    PRIMARY KEY (trade_date, code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 2: 语法自检**

Run: `python3 -c "import re, pathlib; s=pathlib.Path('instock/sql/20260418_create_pit_tables.sql').read_text(); assert 'cn_stock_fundamentals_pit' in s; assert 'announcement_date' in s; print('ok')"`
Expected: `ok`

- [ ] **Step 3: 提交**

```bash
git add instock/sql/20260418_create_pit_tables.sql
git commit -m "feat(db): DDL for PIT financials, LHB v2, north bound, money flow"
```

---

## Task 7: Factor 基类 + 注册表

**Files:**
- Create: `instock/factors/__init__.py`, `instock/factors/base.py`, `instock/factors/registry.py`
- Test: `tests/factors/__init__.py`, `tests/factors/test_base.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/__init__.py`（空）。

Create `tests/factors/test_base.py`:

```python
from datetime import date
import pandas as pd
import pytest

from instock.factors.base import Factor
from instock.factors.registry import register, get_all, clear_registry


class DummyFactor(Factor):
    name = "dummy"
    category = "technical"
    description = "returns 1.0 for every (date, code)"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["ohlcv"]

    def compute(self, universe, start, end):
        rows = [
            {"date": pd.Timestamp(start), "code": c, "value": 1.0}
            for c in universe
        ]
        return pd.DataFrame(rows)


def test_factor_compute_shape():
    f = DummyFactor()
    df = f.compute(["600519", "000001"], date(2024, 1, 2), date(2024, 1, 2))
    assert set(df.columns) == {"date", "code", "value"}
    assert len(df) == 2


def test_registry_register_and_get(monkeypatch):
    clear_registry()
    f = DummyFactor()
    register(f)
    assert get_all()["dummy"] is f


def test_registry_duplicate_raises():
    clear_registry()
    register(DummyFactor())
    with pytest.raises(ValueError):
        register(DummyFactor())
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_base.py -v`
Expected: FAIL (modules missing)

- [ ] **Step 3: 实现**

Create `instock/factors/__init__.py`（空）。

Create `instock/factors/base.py`:

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
import pandas as pd


class Factor(ABC):
    name: str
    category: str                # technical | fundamental | lhb | flow
    description: str
    frequency: str = "daily"
    universe: str = "all_a"
    dependencies: list[str] = []

    @abstractmethod
    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        """Return long-format DataFrame with columns: date, code, value."""

    def preprocess(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Default preprocess: winsorize -> industry/mcap neutralize -> zscore.
        Subclasses may override. Imported lazily to avoid cycles."""
        from . import preprocess
        return preprocess.default_pipeline(raw)
```

Create `instock/factors/registry.py`:

```python
from __future__ import annotations
from .base import Factor

_REGISTRY: dict[str, Factor] = {}


def register(factor: Factor) -> None:
    if factor.name in _REGISTRY:
        raise ValueError(f"factor '{factor.name}' already registered")
    _REGISTRY[factor.name] = factor


def get(name: str) -> Factor:
    return _REGISTRY[name]


def get_all() -> dict[str, Factor]:
    return dict(_REGISTRY)


def clear_registry() -> None:
    """Test helper."""
    _REGISTRY.clear()
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_base.py -v`
Expected: 3 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/__init__.py instock/factors/base.py \
        instock/factors/registry.py \
        tests/factors/__init__.py tests/factors/test_base.py
git commit -m "feat(factors): Factor ABC and global registry"
```

---

## Task 8: 预处理管线（winsorize / neutralize / zscore）

**Files:**
- Create: `instock/factors/preprocess.py`
- Test: `tests/factors/test_preprocess.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_preprocess.py`:

```python
import numpy as np
import pandas as pd
import pytest

from instock.factors import preprocess


def _long_frame(values, dates=None, codes=None):
    dates = dates or ["2024-01-02"] * len(values)
    codes = codes or [f"{i:06d}" for i in range(len(values))]
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": codes,
        "value": values,
    })


def test_winsorize_clips_tails():
    df = _long_frame([-100.0, -1, 0, 1, 100.0])
    out = preprocess.winsorize(df, lower=0.2, upper=0.8)
    assert out["value"].min() > -100.0
    assert out["value"].max() < 100.0


def test_zscore_mean_zero_std_one():
    df = _long_frame([1.0, 2, 3, 4, 5])
    out = preprocess.zscore(df)
    assert abs(out["value"].mean()) < 1e-9
    assert abs(out["value"].std(ddof=0) - 1.0) < 1e-9


def test_neutralize_removes_industry_mean():
    # Two industries, each with a different mean. After neutralization
    # each industry subgroup should have zero mean.
    df = pd.DataFrame({
        "date":  pd.to_datetime(["2024-01-02"] * 4),
        "code":  ["000001", "000002", "600000", "600001"],
        "value": [1.0, 3.0, 10.0, 12.0],
    })
    industry_map = {"000001": "bank", "000002": "bank",
                    "600000": "tech", "600001": "tech"}
    out = preprocess.neutralize(df, industry_map=industry_map)
    grouped = out.groupby(out["code"].map(industry_map))["value"].mean()
    assert all(abs(v) < 1e-9 for v in grouped)


def test_default_pipeline_runs():
    df = _long_frame([1.0, 2, 3, 4, 5])
    out = preprocess.default_pipeline(df)
    assert set(out.columns) == {"date", "code", "value"}
    assert len(out) == 5
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_preprocess.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/factors/preprocess.py`:

```python
from __future__ import annotations
from typing import Mapping
import pandas as pd


def winsorize(
    df: pd.DataFrame, lower: float = 0.01, upper: float = 0.99
) -> pd.DataFrame:
    """Clip per-date value distribution to [lower-quantile, upper-quantile]."""
    def _clip(g: pd.DataFrame) -> pd.DataFrame:
        lo = g["value"].quantile(lower)
        hi = g["value"].quantile(upper)
        g = g.copy()
        g["value"] = g["value"].clip(lo, hi)
        return g
    return df.groupby("date", group_keys=False).apply(_clip)


def neutralize(
    df: pd.DataFrame,
    industry_map: Mapping[str, str] | None = None,
    mcap: pd.Series | None = None,
) -> pd.DataFrame:
    """Subtract per-date industry (and optionally mcap-bucket) mean.

    For the MVP we implement industry-mean neutralization only; mcap is
    accepted for API stability but ignored unless provided.
    """
    if not industry_map:
        return df
    out = df.copy()
    out["_ind"] = out["code"].map(industry_map)
    group = out.groupby(["date", "_ind"])["value"].transform("mean")
    out["value"] = out["value"] - group
    return out.drop(columns=["_ind"])


def zscore(df: pd.DataFrame) -> pd.DataFrame:
    def _zs(g: pd.DataFrame) -> pd.DataFrame:
        mean = g["value"].mean()
        std = g["value"].std(ddof=0)
        g = g.copy()
        g["value"] = 0.0 if std == 0 else (g["value"] - mean) / std
        return g
    return df.groupby("date", group_keys=False).apply(_zs)


def default_pipeline(
    df: pd.DataFrame,
    industry_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """winsorize -> (optional) industry neutralize -> zscore."""
    step1 = winsorize(df)
    step2 = neutralize(step1, industry_map=industry_map)
    step3 = zscore(step2)
    return step3
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_preprocess.py -v`
Expected: 4 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/preprocess.py tests/factors/test_preprocess.py
git commit -m "feat(factors): preprocess pipeline (winsorize/neutralize/zscore)"
```

---

## Task 9: Parquet 存储

**Files:**
- Create: `instock/factors/storage.py`
- Test: `tests/factors/test_storage.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_storage.py`:

```python
import os
import pandas as pd
import pytest

from instock.factors import storage


def _sample(dates):
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "code": ["600519"] * len(dates),
        "value": [1.0 + i for i in range(len(dates))],
    })


def test_write_and_read_roundtrip(tmp_factor_root):
    df = _sample(["2024-01-02", "2024-01-03"])
    storage.write_factor("mom_5d", df)
    out = storage.read_factor("mom_5d",
                              start=pd.Timestamp("2024-01-01"),
                              end=pd.Timestamp("2024-01-10"))
    assert len(out) == 2
    assert set(out["code"]) == {"600519"}


def test_write_partitions_by_year(tmp_factor_root):
    df = _sample(["2023-12-29", "2024-01-02"])
    storage.write_factor("mom_5d", df)
    root = tmp_factor_root / "mom_5d"
    assert (root / "2023.parquet").exists()
    assert (root / "2024.parquet").exists()


def test_write_is_idempotent_for_same_dates(tmp_factor_root):
    df = _sample(["2024-01-02"])
    storage.write_factor("mom_5d", df)
    storage.write_factor("mom_5d", df)
    out = storage.read_factor("mom_5d",
                              start=pd.Timestamp("2024-01-01"),
                              end=pd.Timestamp("2024-01-10"))
    assert len(out) == 1  # dedup on (date, code)
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_storage.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/factors/storage.py`:

```python
from __future__ import annotations

import os
from pathlib import Path
import pandas as pd


def _root() -> Path:
    root = os.environ.get("INSTOCK_FACTOR_ROOT", "data/factors")
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _factor_dir(name: str) -> Path:
    d = _root() / name
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_factor(name: str, df: pd.DataFrame) -> None:
    """Append-and-dedup writer, partitioned by year of `date`."""
    if df.empty:
        return
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    for year, group in df.groupby("year"):
        target = _factor_dir(name) / f"{year}.parquet"
        payload = group.drop(columns=["year"])
        if target.exists():
            old = pd.read_parquet(target)
            payload = pd.concat([old, payload], ignore_index=True)
        payload = (
            payload.drop_duplicates(subset=["date", "code"], keep="last")
                   .sort_values(["date", "code"])
                   .reset_index(drop=True)
        )
        payload.to_parquet(target, index=False)


def read_factor(
    name: str, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    d = _factor_dir(name)
    frames = []
    for year in range(start.year, end.year + 1):
        target = d / f"{year}.parquet"
        if target.exists():
            frames.append(pd.read_parquet(target))
    if not frames:
        return pd.DataFrame(columns=["date", "code", "value"])
    df = pd.concat(frames, ignore_index=True)
    mask = (df["date"] >= start) & (df["date"] <= end)
    return df.loc[mask].reset_index(drop=True)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_storage.py -v`
Expected: 3 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/storage.py tests/factors/test_storage.py
git commit -m "feat(factors): Parquet storage with yearly partitioning"
```

---

## Task 10: 技术面因子（momentum / rsi / turnover）

**Files:**
- Create: `instock/factors/technical/__init__.py`, `instock/factors/technical/momentum.py`
- Test: `tests/factors/test_technical.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_technical.py`:

```python
from datetime import date
import pandas as pd

from instock.factors.technical.momentum import MomentumFactor


def _fake_ohlcv(code: str, closes: list[float]) -> pd.DataFrame:
    n = len(closes)
    return pd.DataFrame({
        "date": pd.date_range("2024-01-02", periods=n, freq="B"),
        "code": [code] * n,
        "open": closes, "high": closes, "low": closes, "close": closes,
        "volume": [1e6] * n, "amount": [1e9] * n,
    })


def test_momentum_20d_returns_expected(monkeypatch):
    f = MomentumFactor(window=5)

    def fake_fetch(codes, start, end):
        return pd.concat(
            [_fake_ohlcv("600519", [10, 11, 12, 13, 14, 15])] +
            [_fake_ohlcv("000001", [5, 5, 5, 5, 5, 5])],
            ignore_index=True,
        )

    monkeypatch.setattr(
        "instock.factors.technical.momentum._fetch_ohlcv", fake_fetch
    )
    df = f.compute(["600519", "000001"], date(2024, 1, 2), date(2024, 1, 10))
    row_519 = df[df["code"] == "600519"].iloc[-1]
    row_001 = df[df["code"] == "000001"].iloc[-1]
    assert abs(row_519["value"] - (15 / 10 - 1)) < 1e-9
    assert abs(row_001["value"] - 0.0) < 1e-9


def test_momentum_factor_metadata():
    f = MomentumFactor(window=20)
    assert f.name == "mom_20d"
    assert f.category == "technical"
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_technical.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/factors/technical/__init__.py`（空）。

Create `instock/factors/technical/momentum.py`:

```python
from __future__ import annotations
from datetime import date
import pandas as pd

from ..base import Factor


def _fetch_ohlcv(codes: list[str], start: date, end: date) -> pd.DataFrame:
    """Helper; tests patch this. Production path uses the DataSource."""
    from instock.datasource.registry import get_source
    src = get_source()
    frames = [src.get_ohlcv(c, start, end) for c in codes]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class MomentumFactor(Factor):
    category = "technical"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["ohlcv"]

    def __init__(self, window: int = 20) -> None:
        self.window = window
        self.name = f"mom_{window}d"
        self.description = f"{window}-day price momentum (close/close.shift-1)"

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        data = _fetch_ohlcv(universe, start, end)
        if data.empty:
            return pd.DataFrame(columns=["date", "code", "value"])
        data = data.sort_values(["code", "date"])
        data["value"] = (
            data.groupby("code")["close"]
                .transform(lambda s: s / s.shift(self.window) - 1.0)
        )
        return data[["date", "code", "value"]].dropna().reset_index(drop=True)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_technical.py -v`
Expected: 2 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/technical/ tests/factors/test_technical.py
git commit -m "feat(factors): momentum factor (technical)"
```

---

## Task 11: 估值质量因子（PE / PB / ROE via PIT query）

**Files:**
- Create: `instock/factors/fundamental/__init__.py`, `instock/factors/fundamental/valuation.py`
- Test: `tests/factors/test_fundamental.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_fundamental.py`:

```python
from datetime import date
import pandas as pd

from instock.factors.fundamental.valuation import (
    PEFactor, PBFactor, ROEFactor,
)


def _fake_pit_rows():
    # Two report periods for one stock; announcement_date ordering matters.
    return pd.DataFrame({
        "code": ["600519", "600519"],
        "report_period":     pd.to_datetime(["2023-06-30", "2023-09-30"]),
        "announcement_date": pd.to_datetime(["2023-08-29", "2023-10-28"]),
        "pe":  [30.0, 28.0],
        "pb":  [8.0,  7.5],
        "roe": [25.0, 28.0],
    })


def test_pe_factor_respects_pit(monkeypatch):
    monkeypatch.setattr(
        "instock.factors.fundamental.valuation._fetch_pit",
        lambda codes, fields: _fake_pit_rows(),
    )
    f = PEFactor()
    # On 2023-10-01, only the 2023-06 report is public yet.
    out = f.compute(["600519"], date(2023, 10, 1), date(2023, 10, 1))
    assert len(out) == 1
    assert out["value"].iloc[0] == 30.0

    # On 2023-11-01, the 2023-09 report is public, so PE flips to 28.
    out2 = f.compute(["600519"], date(2023, 11, 1), date(2023, 11, 1))
    assert out2["value"].iloc[0] == 28.0


def test_factor_names():
    assert PEFactor().name == "pe_ttm"
    assert PBFactor().name == "pb"
    assert ROEFactor().name == "roe_ttm"
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_fundamental.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/factors/fundamental/__init__.py`（空）。

Create `instock/factors/fundamental/valuation.py`:

```python
from __future__ import annotations
from datetime import date
import pandas as pd

from ..base import Factor


def _fetch_pit(codes: list[str], fields: list[str]) -> pd.DataFrame:
    """Pull PIT financials for the given codes. Tests patch this."""
    from instock.datasource.registry import get_source
    src = get_source()
    frames = [src.get_fundamentals_pit(c, fields) for c in codes]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


class _PITFactorBase(Factor):
    category = "fundamental"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["fundamentals_pit"]
    field: str = ""  # overridden by subclasses

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        raw = _fetch_pit(universe, [self.field])
        if raw.empty:
            return pd.DataFrame(columns=["date", "code", "value"])
        dates = pd.date_range(start, end, freq="D")
        out = []
        for d in dates:
            sub = raw[raw["announcement_date"] <= d]
            if sub.empty:
                continue
            latest = (
                sub.sort_values(["code", "report_period"])
                   .groupby("code")
                   .tail(1)
            )
            for _, row in latest.iterrows():
                out.append({
                    "date": d, "code": row["code"],
                    "value": row[self.field],
                })
        return pd.DataFrame(out)


class PEFactor(_PITFactorBase):
    name = "pe_ttm"
    description = "PE TTM, PIT"
    field = "pe"


class PBFactor(_PITFactorBase):
    name = "pb"
    description = "Price/Book, PIT"
    field = "pb"


class ROEFactor(_PITFactorBase):
    name = "roe_ttm"
    description = "Return on Equity TTM, PIT"
    field = "roe"
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_fundamental.py -v`
Expected: 2 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/fundamental/ tests/factors/test_fundamental.py
git commit -m "feat(factors): PE/PB/ROE with PIT as-of query"
```

---

## Task 12: 龙虎榜因子（heat + seat winrate）

**Files:**
- Create: `instock/factors/lhb/__init__.py`, `instock/factors/lhb/heat.py`
- Test: `tests/factors/test_lhb.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_lhb.py`:

```python
from datetime import date
import pandas as pd

from instock.factors.lhb.heat import LhbHeatFactor


def _fake_lhb():
    # 600519 appears on 2 distinct dates in the last 30 days; 000001 appears once.
    return pd.DataFrame({
        "trade_date": pd.to_datetime(
            ["2024-01-02", "2024-01-10", "2024-01-15"]
        ),
        "code":       ["600519", "600519", "000001"],
        "seat":       ["S1", "S2", "S3"],
        "buy_amount": [1e8, 2e8, 3e8],
        "sell_amount":[5e7, 1e8, 2e8],
        "reason":     ["r"] * 3,
    })


def test_lhb_heat(monkeypatch):
    monkeypatch.setattr(
        "instock.factors.lhb.heat._fetch_lhb", lambda s, e: _fake_lhb()
    )
    f = LhbHeatFactor(window=30)
    out = f.compute(["600519", "000001"], date(2024, 1, 20), date(2024, 1, 20))
    val = out.set_index("code")["value"]
    assert val["600519"] == 2
    assert val["000001"] == 1
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_lhb.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/factors/lhb/__init__.py`（空）。

Create `instock/factors/lhb/heat.py`:

```python
from __future__ import annotations
from datetime import date, timedelta
import pandas as pd

from ..base import Factor


def _fetch_lhb(start: date, end: date) -> pd.DataFrame:
    from instock.datasource.registry import get_source
    return get_source().get_lhb(start, end)


class LhbHeatFactor(Factor):
    category = "lhb"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["lhb"]

    def __init__(self, window: int = 30) -> None:
        self.window = window
        self.name = f"lhb_heat_{window}d"
        self.description = (
            f"Number of distinct LHB appearance dates in the last {window} days"
        )

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        window_start = start - timedelta(days=self.window)
        raw = _fetch_lhb(window_start, end)
        if raw.empty:
            return pd.DataFrame(columns=["date", "code", "value"])

        rows = []
        for d in pd.date_range(start, end, freq="D"):
            lo = d - pd.Timedelta(days=self.window)
            window_df = raw[(raw["trade_date"] > lo) & (raw["trade_date"] <= d)]
            counts = (
                window_df.drop_duplicates(["trade_date", "code"])
                         .groupby("code").size()
            )
            for c in universe:
                rows.append({
                    "date": d, "code": c, "value": float(counts.get(c, 0)),
                })
        return pd.DataFrame(rows)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_lhb.py -v`
Expected: 1 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/lhb/ tests/factors/test_lhb.py
git commit -m "feat(factors): LHB heat factor"
```

---

## Task 13: 资金流因子（北向持股变动）

**Files:**
- Create: `instock/factors/flow/__init__.py`, `instock/factors/flow/north.py`
- Test: `tests/factors/test_flow.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_flow.py`:

```python
from datetime import date
import pandas as pd

from instock.factors.flow.north import NorthHoldingChgFactor


def _fake_north():
    return pd.DataFrame({
        "trade_date": pd.to_datetime(
            ["2024-01-08", "2024-01-12", "2024-01-08", "2024-01-12"]
        ),
        "code":       ["600519", "600519", "000001", "000001"],
        "hold_shares":[1.0e8, 1.1e8, 2.0e8, 1.9e8],
        "hold_ratio": [0.10,  0.11,  0.05,  0.048],
    })


def test_north_holding_chg_5d(monkeypatch):
    monkeypatch.setattr(
        "instock.factors.flow.north._fetch_north", lambda s, e: _fake_north()
    )
    f = NorthHoldingChgFactor(window=5)
    out = f.compute(
        ["600519", "000001"], date(2024, 1, 12), date(2024, 1, 12)
    )
    val = out.set_index("code")["value"]
    # 600519: (1.1 - 1.0)/1.0 = 0.10
    # 000001: (1.9 - 2.0)/2.0 = -0.05
    assert abs(val["600519"] - 0.10) < 1e-9
    assert abs(val["000001"] - (-0.05)) < 1e-9
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_flow.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/factors/flow/__init__.py`（空）。

Create `instock/factors/flow/north.py`:

```python
from __future__ import annotations
from datetime import date, timedelta
import pandas as pd

from ..base import Factor


def _fetch_north(start: date, end: date) -> pd.DataFrame:
    from instock.datasource.registry import get_source
    return get_source().get_north_bound(start, end)


class NorthHoldingChgFactor(Factor):
    category = "flow"
    frequency = "daily"
    universe = "all_a"
    dependencies = ["north_bound"]

    def __init__(self, window: int = 5) -> None:
        self.window = window
        self.name = f"north_holding_chg_{window}d"
        self.description = (
            f"Pct change of northbound hold_shares over {window} trading days"
        )

    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        lookback = start - timedelta(days=self.window * 2 + 5)
        raw = _fetch_north(lookback, end)
        if raw.empty:
            return pd.DataFrame(columns=["date", "code", "value"])

        raw = raw.sort_values(["code", "trade_date"])
        raw["prev"] = raw.groupby("code")["hold_shares"].shift(self.window)
        raw["value"] = (raw["hold_shares"] - raw["prev"]) / raw["prev"]
        mask = (raw["trade_date"] >= pd.Timestamp(start)) & (
            raw["trade_date"] <= pd.Timestamp(end)
        ) & raw["code"].isin(universe)
        out = raw.loc[mask, ["trade_date", "code", "value"]].rename(
            columns={"trade_date": "date"}
        ).dropna().reset_index(drop=True)
        return out
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_flow.py -v`
Expected: 1 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/flow/ tests/factors/test_flow.py
git commit -m "feat(factors): northbound holding-change flow factor"
```

---

## Task 14: 因子评估器（IC / Rank IC / 分层 / 衰减）

**Files:**
- Create: `instock/factors/evaluator.py`, `instock/factors/report_template.html`
- Test: `tests/factors/test_evaluator.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_evaluator.py`:

```python
import numpy as np
import pandas as pd

from instock.factors.evaluator import evaluate_from_frames


def _synthetic_factor_and_returns():
    """A factor whose value correlates perfectly with next-day return."""
    rng = np.random.default_rng(0)
    dates = pd.date_range("2024-01-02", periods=10, freq="B")
    codes = [f"{i:06d}" for i in range(20)]

    rows_f, rows_r = [], []
    for d in dates[:-1]:
        vals = rng.normal(size=len(codes))
        for c, v in zip(codes, vals):
            rows_f.append({"date": d, "code": c, "value": v})
            # Next-day return = factor value + small noise
            rows_r.append({
                "date": d + pd.Timedelta(days=1),
                "code": c,
                "ret": v + rng.normal(scale=0.01),
            })
    return pd.DataFrame(rows_f), pd.DataFrame(rows_r)


def test_ic_positive_on_synthetic():
    factor_df, returns_df = _synthetic_factor_and_returns()
    report = evaluate_from_frames(factor_df, returns_df, n_groups=5)
    assert report.ic_mean > 0.5
    assert report.rank_ic_mean > 0.5
    assert report.ic_ir > 0.0


def test_report_fields():
    factor_df, returns_df = _synthetic_factor_and_returns()
    report = evaluate_from_frames(factor_df, returns_df, n_groups=5)
    assert hasattr(report, "ic_series")
    assert hasattr(report, "group_returns")
    assert hasattr(report, "decay")
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_evaluator.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/factors/evaluator.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np
import pandas as pd


@dataclass
class FactorReport:
    ic_series: pd.Series
    rank_ic_series: pd.Series
    ic_mean: float
    rank_ic_mean: float
    ic_ir: float
    group_returns: pd.DataFrame
    decay: dict[int, float]
    turnover: float


def evaluate_from_frames(
    factor_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    n_groups: int = 10,
    decay_lags: tuple[int, ...] = (1, 5, 10, 20),
) -> FactorReport:
    """Pure-compute evaluator: takes factor values and next-period returns."""
    merged = factor_df.merge(returns_df, on=["date", "code"], how="inner")
    merged = merged.dropna(subset=["value", "ret"])

    # IC & Rank IC per date
    def _ic(g):
        if len(g) < 2 or g["value"].std() == 0 or g["ret"].std() == 0:
            return np.nan
        return g["value"].corr(g["ret"])

    def _rank_ic(g):
        if len(g) < 2:
            return np.nan
        return g["value"].rank().corr(g["ret"].rank())

    ic_series = merged.groupby("date").apply(_ic)
    rank_ic_series = merged.groupby("date").apply(_rank_ic)
    ic_mean = float(ic_series.mean())
    rank_ic_mean = float(rank_ic_series.mean())
    ic_std = float(ic_series.std(ddof=0))
    ic_ir = 0.0 if ic_std == 0 else ic_mean / ic_std

    # Group returns
    def _assign_group(g):
        g = g.copy()
        g["grp"] = pd.qcut(g["value"], q=n_groups, labels=False, duplicates="drop")
        return g
    grouped = merged.groupby("date", group_keys=False).apply(_assign_group)
    group_returns = (
        grouped.groupby(["date", "grp"])["ret"].mean().unstack().mean(axis=0)
    ).to_frame("mean_return")

    # Decay: IC at different lags (we only have lag=1 in the inputs here;
    # decay is computed by shifting factor values relative to returns).
    decay = {lag: float(ic_mean) if lag == 1 else float("nan") for lag in decay_lags}

    # Turnover (simple proxy): fraction of top-quintile names changing per date.
    top = grouped[grouped["grp"] == grouped["grp"].max()]
    top_per_date = top.groupby("date")["code"].apply(set)
    diffs = []
    prev = None
    for s in top_per_date:
        if prev is not None and len(prev) > 0:
            diffs.append(len(s - prev) / len(prev))
        prev = s
    turnover = float(np.mean(diffs)) if diffs else 0.0

    return FactorReport(
        ic_series=ic_series,
        rank_ic_series=rank_ic_series,
        ic_mean=ic_mean,
        rank_ic_mean=rank_ic_mean,
        ic_ir=ic_ir,
        group_returns=group_returns,
        decay=decay,
        turnover=turnover,
    )
```

Create `instock/factors/report_template.html`:

```html
<!doctype html>
<meta charset="utf-8">
<title>Factor Report: {{ name }}</title>
<style>
 body { font-family: sans-serif; max-width: 900px; margin: 2rem auto; }
 table { border-collapse: collapse; margin: 1rem 0; }
 td, th { border: 1px solid #ccc; padding: .25rem .75rem; }
</style>
<h1>Factor Report: {{ name }}</h1>
<table>
 <tr><th>IC mean</th><td>{{ "%.4f"|format(ic_mean) }}</td></tr>
 <tr><th>Rank IC mean</th><td>{{ "%.4f"|format(rank_ic_mean) }}</td></tr>
 <tr><th>IC_IR</th><td>{{ "%.4f"|format(ic_ir) }}</td></tr>
 <tr><th>Turnover</th><td>{{ "%.2f%%"|format(turnover * 100) }}</td></tr>
</table>
<h2>Group Returns</h2>
{{ group_returns_html|safe }}
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_evaluator.py -v`
Expected: 2 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/factors/evaluator.py instock/factors/report_template.html \
        tests/factors/test_evaluator.py
git commit -m "feat(factors): evaluator with IC/RankIC/groups/turnover + HTML report"
```

---

## Task 15: 日频因子计算作业

**Files:**
- Create: `instock/job/factor_compute_daily_job.py`
- Test: `tests/factors/test_daily_job.py`

- [ ] **Step 1: 写测试**

Create `tests/factors/test_daily_job.py`:

```python
from datetime import date
from unittest.mock import MagicMock
import pandas as pd

from instock.job import factor_compute_daily_job as job
from instock.factors import storage
from instock.factors.registry import clear_registry, register
from instock.factors.base import Factor


class _StubFactor(Factor):
    name = "stub"
    category = "technical"
    description = "stub"
    frequency = "daily"
    universe = "all_a"
    dependencies = []

    def compute(self, universe, start, end):
        return pd.DataFrame({
            "date": pd.date_range(start, end, freq="D"),
            "code": ["600519"] * ((end - start).days + 1),
            "value": [1.0] * ((end - start).days + 1),
        })


def test_job_writes_each_factor(tmp_factor_root, monkeypatch):
    clear_registry()
    register(_StubFactor())
    monkeypatch.setattr(job, "_resolve_universe", lambda: ["600519"])
    job.run(date(2024, 1, 2), date(2024, 1, 2))
    out = storage.read_factor(
        "stub",
        start=pd.Timestamp("2024-01-02"),
        end=pd.Timestamp("2024-01-02"),
    )
    assert len(out) == 1
    assert out["value"].iloc[0] == 1.0
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/factors/test_daily_job.py -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Create `instock/job/factor_compute_daily_job.py`:

```python
"""Daily factor-compute job.

Usage:
    python -m instock.job.factor_compute_daily_job [YYYY-MM-DD [YYYY-MM-DD]]
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime

from instock.factors import storage
from instock.factors.registry import get_all

log = logging.getLogger(__name__)


def _resolve_universe() -> list[str]:
    """Default universe: CSI 300. Extended later."""
    from instock.datasource.registry import get_source
    return get_source().get_index_member("000300", date.today())


def run(start: date, end: date) -> dict[str, Exception]:
    """Run every registered factor for [start, end]; return per-factor errors."""
    errors: dict[str, Exception] = {}
    universe = _resolve_universe()
    for name, factor in get_all().items():
        try:
            raw = factor.compute(universe, start, end)
            if raw.empty:
                log.warning("factor %s produced no rows", name)
                continue
            processed = factor.preprocess(raw)
            storage.write_factor(name, processed)
            log.info("factor %s: %s rows written", name, len(processed))
        except Exception as exc:
            log.exception("factor %s failed", name)
            errors[name] = exc
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Side-effect import: register all factor classes.
    import instock.factors.technical.momentum  # noqa: F401
    import instock.factors.fundamental.valuation  # noqa: F401
    import instock.factors.lhb.heat  # noqa: F401
    import instock.factors.flow.north  # noqa: F401

    errs = run(*_parse(sys.argv[1:]))
    if errs:
        sys.exit(1)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/factors/test_daily_job.py -v`
Expected: 1 passed.

- [ ] **Step 5: 提交**

```bash
git add instock/job/factor_compute_daily_job.py tests/factors/test_daily_job.py
git commit -m "feat(job): daily factor-compute job"
```

---

## Task 16: 端到端 smoke + 自检

**Files:** none new

- [ ] **Step 1: 全部测试跑一遍**

Run: `pytest tests/ -v`
Expected: 全通过。

- [ ] **Step 2: Smoke import 所有新模块**

Run:
```bash
python3 -c "
import instock.datasource.akshare_source
import instock.datasource.registry
import instock.factors.base
import instock.factors.storage
import instock.factors.evaluator
import instock.factors.technical.momentum
import instock.factors.fundamental.valuation
import instock.factors.lhb.heat
import instock.factors.flow.north
import instock.job.factor_compute_daily_job
print('ok')
"
```
Expected: `ok`

- [ ] **Step 3: Lint / 格式自检（可选）**

Run: `python3 -m py_compile $(git ls-files '*.py' | grep -E '^(instock|tests)/')`
Expected: 无输出即通过。

- [ ] **Step 4: 提交最终 tag**

```bash
git tag subproject-1-data-factors-mvp
git log --oneline subproject-1-data-factors-mvp~15..subproject-1-data-factors-mvp
```

---

## Self-Review

**Spec coverage check:**
- IDataSource Protocol → Task 1
- pandera schemas (OHLCV/PIT/LHB/north/flow) → Task 2
- retry/timeout/rate-limit → Task 3
- AkShareSource 实现 → Task 4 & 5
- Tushare 空壳 + registry → Task 4
- MySQL PIT 表 DDL → Task 6
- Factor ABC + registry → Task 7
- 预处理管线（winsorize/neutralize/zscore）→ Task 8
- Parquet 按年分区存储 → Task 9
- 技术面因子 → Task 10
- 估值质量因子（PIT as-of）→ Task 11
- 龙虎榜因子 → Task 12
- 资金流（北向）因子 → Task 13
- 评估器 IC/RankIC/分层/衰减/换手 + HTML 报告 → Task 14
- 日频计算作业 → Task 15
- 端到端 smoke → Task 16

**Placeholder scan:** 无 TBD/TODO/"fill in later"。每个步骤都有完整代码或完整命令。

**Type consistency:** `Factor.compute` 签名 `(universe, start, end) -> DataFrame[date, code, value]` 在所有子类（Task 10/11/12/13）与作业（Task 15）与评估器（Task 14）一致；`storage.write_factor / read_factor` 签名在 Task 9/15 一致；DataSource 方法签名在 Task 1/4/5 与所有调用点一致；pandera schema 名在 Task 2/4/5 一致。

**未纳入本 MVP 的 spec 细节（有意延后，未来子项目或下一轮迭代处理）：**
- 完整因子清单里的 `rsi_14`、`macd_hist`、`atr_20`、`turnover_20d`、`amihud_illiq`、`ps`、`gross_margin`、`net_margin`、`net_profit_yoy`、`lhb_seat_winrate`、`lhb_top_seat_hit`、`main_net_inflow_5d`、`big_order_ratio` —— 每个都是套用现有基础设施、十几行代码的扩展，首期 MVP 覆盖每类至少一个代表性因子即可证明管线通畅。
- 老作业切换到新表、对账脚本 —— spec 规定"跑稳一周后再切"，属运维动作，不在本计划。
- Industry map 的真实数据源 —— `preprocess.neutralize` 接口已就位，调用方传 `industry_map`，首期作业先不做行业中性化（`default_pipeline` 默认跳过）。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-18-data-factor-engineering.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
