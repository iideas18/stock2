# Sub-project 2.5 — 数据层对齐（Data Alignment）设计 spec

**创建日期**：2026-04-18
**定位**：把 Sub-2 留下的"数据层占位件"补齐 + 修 Sub-1 的 factor registry 静默 bug，
为 Sub-3 回测引擎提供真实可信的输入。
**前置**：Sub-1 ✅、Sub-2 ✅
**后续**：Sub-3（回测引擎）

---

## 1. 范围与非目标

### 范围（5 件交付物）
1. **industry_map**（akshare 东财板块，当前快照）
2. **listing_dates**（akshare `stock_individual_info_em`，逐股拉，全量 + 增量）
3. **st_flags**（akshare `stock_zh_a_st_em`，当前快照）
4. **OhlcvPanelStore**（封装 Sub-1 OHLCV，缺失则在线 IDataSource 补齐 + cache 到 parquet）
5. **factor registry bootstrap**（修 Sub-1 follow-up A：`instock/factors/bootstrap.py`）

### 非目标（显式不做）
- Sub-3 backtester 本身（独立子项目）
- 历史 ST / 历史行业（只存当前快照；as-of 查询返回最近快照，文档承认轻微 look-ahead）
- 申万行业、Tushare 实现
- Web 调度（Sub-4）
- 历史 OHLCV 大规模回填的批量化（MVP 提供独立 CLI 一次性预热，由人工触发）

### MVP 验收门槛
- [ ] 三个 refdata daily job 能跑通 → parquet 落盘 → reader 接口可读
- [ ] Sub-2 默认 `FilterChain` 升级为 `[Suspended, NewListing(refdata), ST, Limit]`，单测全过
- [ ] Sub-2 默认 `IndustryExposureConstraint` 注入真 industry_map
- [ ] Sub-1 + Sub-2 daily job 共用 `factors.bootstrap.register_default_factors()`，
      `get_all()` 非空可断言（消除 follow-up A 的 silent no-op）
- [ ] 一个 `INSTOCK_SUB25_SMOKE=1` 真数据冒烟测试

---

## 2. 模块结构

```
instock/
├── refdata/                          ← 新增包
│   ├── __init__.py
│   ├── schemas.py                    ← pandera schemas（industry/listing/st）
│   ├── industry.py                   ← read_industry_map(at) + write_industry_snapshot(df)
│   ├── listing.py                    ← read_listing_dates() + upsert_listing_dates(df)
│   ├── st.py                         ← read_st_flags(at) + write_st_snapshot(df)
│   └── ohlcv_store.py                ← OhlcvPanelStore（read + missing-on-demand fill）
├── factors/
│   └── bootstrap.py                  ← 新增：register_default_factors()
├── portfolio/
│   └── filters.py                    ← 加 STFilter + LimitFilter；NewListingFilter 改读 refdata
├── job/
│   ├── refdata_industry_snapshot_job.py   ← 新（推荐周频）
│   ├── refdata_listing_dates_job.py       ← 新（推荐月频，全量+增量）
│   ├── refdata_st_snapshot_job.py         ← 新（推荐周频）
│   ├── factor_compute_daily_job.py        ← 改：调 bootstrap helper
│   └── generate_holdings_daily_job.py     ← 改：去 inline workaround、注入新 filter/constraint
└── datasource/
    └── akshare_source.py             ← 加 fetcher：board_industry, individual_info, st_em
```

### 存储 layout

| 资源 | 路径 | 分区策略 |
|---|---|---|
| industry_map 快照 | `<INSTOCK_REFDATA_ROOT>/industry/<YYYYMMDD>.parquet` | 一次刷一个文件 |
| listing_dates | `<INSTOCK_REFDATA_ROOT>/listing_dates.parquet` | 单文件全量 + upsert |
| st_flags 快照 | `<INSTOCK_REFDATA_ROOT>/st/<YYYYMMDD>.parquet` | 一次刷一个文件 |
| OHLCV cache | `<INSTOCK_OHLCV_ROOT>/<YYYY>.parquet` | 按年分区，行键 (date, code) |

环境变量：
- `INSTOCK_REFDATA_ROOT`（默认 `data/refdata`）
- `INSTOCK_OHLCV_ROOT`（默认 `data/ohlcv`）

### as-of 查询语义（industry / st 通用）
- `read_industry_map(at: date) -> dict[str, str]`：
  列出 `<INSTOCK_REFDATA_ROOT>/industry/*.parquet` 中 `snapshot_date <= at` 的最近一个文件，
  加载为 `code -> industry` 映射。
- 找不到任何快照 → 抛 `RefdataNotAvailable`。
- `read_st_flags(at) -> set[str]` 同上语义，返回 ST 股 code 集合。
- `read_listing_dates() -> dict[str, date]`：单文件全量读，无 as-of 需求（上市日期是固定属性）。

### Schema 要点（pandera, `coerce=True, strict=False`）

| Schema | columns | 关键约束 |
|---|---|---|
| `INDUSTRY_SNAPSHOT_SCHEMA` | `code, industry, snapshot_date` | code 6 位，unique=`[code]` |
| `LISTING_DATES_SCHEMA` | `code, listing_date` | code 6 位，unique=`[code]` |
| `ST_SNAPSHOT_SCHEMA` | `code, is_st(bool), snapshot_date` | code 6 位，unique=`[code]` |
| `OHLCV_PANEL_SCHEMA` | 同 Sub-1 OHLCV_SCHEMA | 复用，不另立 |

---

## 3. 关键接口

### 3.1 OhlcvPanelStore（Sub-3 唯一入口）

```python
class OhlcvPanelStore:
    def __init__(
        self, source: IDataSource, root: Path | None = None
    ) -> None: ...

    def get_panel(
        self, codes: list[str], start: date, end: date, adjust: str = "qfq"
    ) -> pd.DataFrame:
        """长表 [date, code, open, high, low, close, volume, amount]。

        语义：
          1. 读本地 parquet 子集（按年）。
          2. 用 source.get_trade_calendar(start, end) 算应有 (date, code) 集合。
          3. 找出缺失的 (code) → 对每个 code 调 source.get_ohlcv 补；
             缺失判定按 trade_calendar，不按 date.range（避开节假日误报）。
          4. 写回 parquet（按年分区，drop_duplicates keep="last"）。
          5. 返回合并后的 [start, end] 子集。

        失败：单 code 拉取失败 → log warn 并跳过；返回的 panel 缺该 code。
              全量失败时调用方收到空表（与现有 IDataSource 风格一致）。
        """

    def warm_cache(self, codes: list[str], start: date, end: date) -> None:
        """显式预热（CLI 入口用）。等价于 get_panel(...) 但不返回数据。"""
```

### 3.2 LimitFilter / STFilter

```python
class STFilter(UniverseFilter):
    """根据 FilterContext.st_flags 过滤 ST 股。
    
    若 st_flags is None：warn 一次并 no-op（与现 IndustryExposureConstraint 风格一致）。
    """
    def apply(self, codes, at, context: FilterContext) -> list[str]: ...


def default_thresholds(code: str, is_st: bool) -> float:
    """A 股板块涨跌停阈值。
    
    - ST: 0.05
    - 创业板（300xxx）: 0.20
    - 科创板（688xxx）: 0.20
    - 北交所（8/4 开头）: 0.30
    - 主板其余: 0.10
    """


class LimitFilter(UniverseFilter):
    """T 日开盘前判：T-1 日 close 是否触及 limit_up，是则视为下一交易日不可买入。
    
    阈值表见 default_thresholds。判 limit-up（不判 limit-down，因 MVP 只做买入）。
    
    依赖：
      - context.ohlcv_panel 含 T-1 的 (close, prev_close)（prev_close 由 ohlcv 倒推）
      - context.st_flags 决定阈值
    
    缺数据（T-1 没行情）→ 保守视为不可交易，过滤掉。
    """
    def __init__(
        self,
        threshold_provider: Callable[[str, bool], float] = default_thresholds,
        tolerance: float = 1e-3,
    ) -> None: ...
```

### 3.3 FilterContext 扩展（向后兼容）

```python
@dataclass
class FilterContext:
    ohlcv_panel: pd.DataFrame
    listing_dates: dict[str, date] | None = None    # 新增
    st_flags: set[str] | None = None                # 新增（ST code 集合）
```

`NewListingFilter` 重写：
```python
def apply(self, codes, at, context):
    if context.listing_dates is not None:
        cutoff = at.date() - timedelta(days=self.min_days)
        return [c for c in codes
                if context.listing_dates.get(c) is not None
                and context.listing_dates[c] <= cutoff]
    # 回退：旧的 earliest-OHLCV 近似 + warn 一次
    ...
```

### 3.4 factors.bootstrap

```python
_REGISTERED = False

def register_default_factors() -> None:
    """Idempotent: 多次调用安全。注册 Sub-1 六个内置因子。"""
    global _REGISTERED
    if _REGISTERED:
        return
    from .technical.momentum import MomentumFactor
    from .fundamental.pe import PEFactor
    from .fundamental.pb import PBFactor
    from .fundamental.roe import ROEFactor
    from .lhb.heat import LhbHeatFactor
    from .flow.north import NorthHoldingChgFactor
    for cls in (
        MomentumFactor, PEFactor, PBFactor,
        ROEFactor, LhbHeatFactor, NorthHoldingChgFactor,
    ):
        registry.register(cls())
    _REGISTERED = True
```

调用点：
- `instock/job/factor_compute_daily_job.py::main` 入口
- `instock/job/generate_holdings_daily_job.py::_default_configs` 顶部
  （删除 Sub-2 临时 inline workaround）

---

## 4. 数据流

```
[refdata jobs（周/月频）] ──> refdata parquet
                                  │
                                  ▼
[Sub-1 OHLCV daily] ──> ohlcv parquet ──┐
                                          ▼
[Sub-1 factor_compute_daily]
   │  └─ register_default_factors()
   ▼
factor parquet
   │
   ▼
[Sub-2 generate_holdings_daily]
   ├─ register_default_factors()
   ├─ 读 factor parquet
   ├─ 读 refdata（listing_dates / st_flags / industry_map）
   ├─ 构造 FilterContext + ConstraintContext
   └─ 跑 pipeline ──> holding parquet
                          │
                          ▼
                  [Sub-3 backtester（未来）]
```

---

## 5. 错误处理约定

| 情况 | 行为 |
|---|---|
| `RefdataNotAvailable` | reader 抛；Sub-2 daily job 捕获后跳过该策略并 log（不阻断其余策略） |
| akshare 字段变更 | fetcher rename → pandera 验证 → 失败抛 `SchemaValidationError`，不静默 |
| `NewListingFilter` 无 listing_dates | 回退旧近似 + warn 一次 |
| `IndustryExposureConstraint` 无 industry_map | no-op + warn 一次（沿用现行为） |
| `STFilter` 无 st_flags | no-op + warn 一次 |
| `LimitFilter` 缺 T-1 数据 | 保守过滤掉该 code |
| `OhlcvPanelStore` 在线补齐失败 | 单 code log warn 跳过；全失败时返回空表 |
| `register_default_factors` 重复调 | 幂等 no-op |

---

## 6. 测试策略

沿用 Sub-2 约定：mock 为主 + `INSTOCK_SUB25_SMOKE=1` 门控的真数据冒烟。

| 模块 | 单测 | smoke |
|---|---|---|
| `refdata/schemas.py` | pandera invariant 边界 | — |
| `refdata/industry.py` | mock akshare → write → read as-of 取最近快照 | 真拉一次东财板块 |
| `refdata/listing.py` | mock + upsert 幂等 + 缺失 code 增量 | 真拉 5 个 code |
| `refdata/st.py` | mock + 当前快照 read | 真拉一次 |
| `refdata/ohlcv_store.py` | mock IDataSource，缺失补齐 + cache 写入 + trade_calendar 缺失判定 | 真拉 1 code 1 周 |
| `factors/bootstrap.py` | 调一次后 `get_all()` 含 6 个；幂等 | — |
| `portfolio/filters.py::STFilter` | st_flags 注入 vs 缺失回退 | — |
| `portfolio/filters.py::LimitFilter` | 阈值表（主板/创业板/科创板/北交/ST） + ST 差化 + 缺数据回退 | — |
| `portfolio/filters.py::NewListingFilter` | listing_dates 注入 vs 回退 | — |
| Sub-2 daily job 集成 | mock refdata，断言新 filter/constraint 串入 | 现有 smoke 扩展 |

---

## 7. 已知风险与折中

1. **当前 ST 快照 → 历史 ST look-ahead**：2024 年 ST 的股 2020 年回测会被错误标 ST。
   MVP 接受；Sub-3 启动时在报告里加水印"st 当前快照"。
2. **当前行业快照 → 历史行业 look-ahead**：A 股调行业不多，定性影响小。同水印。
3. **OhlcvPanelStore 在线补齐慢**：首次跑大 universe 历史会触发大量串行拉取
   （akshare 限频 0.2s × ~300 codes × N 年 = 数小时）。MVP 提供独立 CLI
   `python -m instock.refdata.ohlcv_store warm <universe> <start> <end>`
   一次性预热，Sub-3 启动前手动跑。
4. **listing_dates 增量缺口**：新上市股若没及时跑 listing job，会被 `NewListingFilter`
   错误剔除。Job 内对比上次结果，新增数量异常时 warn。
5. **bootstrap 与 module-level 行为差异**：bootstrap 后 `from ... import MomentumFactor`
   仍 work，但 registry 实例只有 bootstrap 注册的那一个（避免重复实例）。文档明示。

---

## 8. 任务粒度预估（plan 阶段细化）

约 10–13 个 task：

| # | 任务 |
|---|---|
| T0 | scaffold（refdata 包 + tmp_refdata_root / tmp_ohlcv_root fixtures） |
| T1 | refdata schemas（3 个 pandera schema + RefdataNotAvailable） |
| T2 | refdata.industry（read/write + as-of） |
| T3 | refdata.listing（read/upsert） |
| T4 | refdata.st（read/write + as-of） |
| T5 | OhlcvPanelStore（trade_calendar 缺失判定 + 在线补齐 + cache + warm CLI） |
| T6 | factors.bootstrap.register_default_factors() + 切换 Sub-1 daily job |
| T7 | Sub-2 daily job 切到 bootstrap 并删 inline workaround |
| T8 | STFilter + FilterContext.st_flags |
| T9 | LimitFilter + default_thresholds（按板块差化） |
| T10 | NewListingFilter 重接 refdata + 兼容回退 |
| T11 | IndustryExposureConstraint 注入真 industry_map（`generate_holdings_daily_job._default_configs` 改） |
| T12 | 三个 refdata job + INSTOCK_SUB25_SMOKE 真数据冒烟测试 |
| T13 | 收尾（roadmap、followup、tag `subproject-2.5-data-alignment-mvp`） |

实际 task 数以 plan 为准。

---

## 9. 与其他子项目的接口契约

### 对 Sub-3 的承诺
- `OhlcvPanelStore.get_panel(...)` 返回 Sub-1 `OHLCV_SCHEMA` 形状的 DataFrame
- `read_industry_map(at) -> dict[str, str]`，`read_st_flags(at) -> set[str]`，
  `read_listing_dates() -> dict[str, date]`
- 三者均可能抛 `RefdataNotAvailable`；调用方决定是否致命

### 不破坏 Sub-2
- `FilterContext` 新增字段为 keyword + 默认 None，老调用兼容
- `NewListingFilter` 行为兼容（无 listing_dates 时回退旧近似 + warn）
- `IndustryExposureConstraint` 行为兼容（无 industry_map 时仍是 no-op）

### 不破坏 Sub-1
- `factors.bootstrap` 是新模块；`registry` 行为不改
- `factor_compute_daily_job` 唯一变化：`run()` 入口先调 bootstrap helper
