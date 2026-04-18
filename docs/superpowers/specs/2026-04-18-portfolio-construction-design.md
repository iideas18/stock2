# Sub-Project 2: 选股与组合构建 — 设计文档

**Created**: 2026-04-18
**Status**: Design approved
**Depends on**: Sub-project 1 (data & factor engineering), merged at `603f311`
**Roadmap**: `docs/superpowers/roadmap.md`

---

## 1. 目标与非目标

### 目标
把 Sub-1 产出的因子数据（`instock/factors/storage.py` 的 Parquet 目录）转换成可下单的 `HoldingSchedule` 产出，写回 Parquet 供 Sub-3 回测和 Sub-4 Web 消费。

### 非目标（MVP 明确不做）
- **不引入 cvxpy / 凸优化**。优化器留作插件位，未来若需要 mean-variance / tracking-error 约束再引入。
- **不做空头 / 对冲组合**。A 股融券成本和标的限制使得 long/short 在 MVP 阶段 YAGNI。
- **不做市值加权 / 逆波动率加权的默认实现**。接口留好，MVP 只发 `EqualWeighter`。
- **不做实盘下单 / 订单管理 / 成交回报**。Sub-2 产出停在 `HoldingSchedule`（目标持仓清单），不碰交易。
- **不做 ST / 涨跌停过滤**。要新数据源，放 follow-up。MVP 只做 OHLCV 能派生的过滤（停牌、次新）。
- **不做真实 industry map 接入**。`IndustryExposureConstraint` 代码就位，但 `industry_map=None` 时跳过。
- **不写 MySQL 镜像表**。Parquet 是唯一真理之源，MySQL 镜像留给 Sub-4。

### 核心设计原则
1. **合约先于算法** — `HoldingSchedule` schema 是 sub-3 / sub-4 的硬契约，算法可以替换、合约不可以随意改。
2. **组件化 / 可替换** — 每个环节（合成、过滤、选股、加权、约束、调度）都有 ABC + 默认实现，未来接入更好的算法只需添加新实现。
3. **和 Sub-1 对称** — 目录结构、Parquet 分区方式、pandera 校验风格、作业 CLI 风格全部与 sub-1 对齐。

---

## 2. 分层架构

```
┌─────────────────────────────────────────────────────────┐
│  instock.job.generate_holdings_daily_job                 │  ← 入口作业
└─────────────────────────────────────────────────────────┘
         │ (start, end, strategy_config)
         ▼
┌─────────────────────────────────────────────────────────┐
│  instock.portfolio.pipeline.StrategyPipeline             │
│    因子读取 → 合成 → 过滤 → 选股 → 权重 → 约束 → 输出     │
└─────────────────────────────────────────────────────────┘
         │
         ├── FactorCombiner ────── EqualRankCombiner (MVP 默认)
         ├── UniverseFilter ────── FilterChain
         │                           ├─ SuspendedFilter
         │                           └─ NewListingFilter
         ├── Selector ──────────── TopQuantileSelector (MVP 默认)
         │                         TopNSelector
         ├── Weighter ──────────── EqualWeighter (MVP 默认)
         ├── PortfolioConstraint ─ MaxWeightConstraint (默认启用)
         │                         IndustryExposureConstraint (默认 no-op)
         ├── RebalanceSchedule ─── DailyRebalance
         │                         WeeklyRebalance("FRI")   ← MVP 默认
         └── HoldingStore ─────── Parquet yearly-partition (镜像 Sub-1 storage)
```

---

## 3. 合约：`HoldingSchedule` Schema

### 3.1 pandera 定义（`instock/portfolio/schemas.py`）

```python
import pandera.pandas as pa

HOLDING_SCHEDULE_SCHEMA = pa.DataFrameSchema({
    "date":     pa.Column("datetime64[ns]"),
    "code":     pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
    "weight":   pa.Column(float, pa.Check.in_range(0.0, 1.0)),
    "score":    pa.Column(float, nullable=True),
    "strategy": pa.Column(str, pa.Check.str_length(min_value=1)),
}, coerce=True, strict=False)
```

### 3.2 附加不变式（`write_holding` 层强制，schema 表达不了）

对于每一个 `(date, strategy)` 分组：
- `weight.sum() ∈ [1.0 - 1e-6, 1.0 + 1e-6]`
- `code` 唯一（无重复持仓行）

违反任一条 → 抛 `HoldingScheduleValidationError`（定义在 `instock/portfolio/schemas.py`）。不重试（和 sub-1 的 SchemaValidationError 一样是 fail-fast 设计失误信号）。

### 3.3 语义约定（写入模块 docstring + 设计文档本节）

> **`HoldingSchedule.date = T` 表示使用 T 日收盘数据生成、T+1 开盘可执行的目标持仓。**
>
> 因此：
> - 因子、价格、停牌状态等所有输入只能用 T 日及以前的数据
> - Sub-3 回测器执行 T+1 开盘价（或 T+1 成交均价，视成本模型）
> - T 日 close 是停牌 / ST / 涨跌停判定的基准时点

此约定是 sub-3 回测器正确性的基石。破坏此约定的因子或选股逻辑会让回测结果不可信。

---

## 4. 核心模块清单

新增目录 `instock/portfolio/`；新增作业 `instock/job/generate_holdings_daily_job.py`。

| 模块 | 文件 | 关键类 / 函数 |
|---|---|---|
| Schema | `instock/portfolio/schemas.py` | `HOLDING_SCHEDULE_SCHEMA`, `HoldingScheduleValidationError` |
| 合约便利 | `instock/portfolio/holding.py` | `HoldingSchedule`（可选 dataclass wrapper；见 §11 备注） |
| 合成 | `instock/portfolio/combiner.py` | `FactorCombiner` ABC, `EqualRankCombiner` |
| 过滤 | `instock/portfolio/filters.py` | `UniverseFilter` ABC, `FilterChain`, `SuspendedFilter`, `NewListingFilter` |
| 选股 | `instock/portfolio/selector.py` | `Selector` ABC, `TopQuantileSelector`, `TopNSelector` |
| 加权 | `instock/portfolio/weighter.py` | `Weighter` ABC, `EqualWeighter` |
| 约束 | `instock/portfolio/constraints.py` | `PortfolioConstraint` ABC, `MaxWeightConstraint`, `IndustryExposureConstraint` |
| 调度 | `instock/portfolio/schedule.py` | `RebalanceSchedule` ABC, `DailyRebalance`, `WeeklyRebalance` |
| 存储 | `instock/portfolio/storage.py` | `write_holding(strategy, df)`, `read_holding(strategy, start, end)` |
| 管线 | `instock/portfolio/pipeline.py` | `StrategyConfig`, `StrategyPipeline` |
| 作业 | `instock/job/generate_holdings_daily_job.py` | `run(start, end, configs)`, CLI |

---

## 5. 接口契约

### 5.1 `FactorCombiner`

```python
class FactorCombiner(ABC):
    @abstractmethod
    def combine(self, factor_panel: pd.DataFrame) -> pd.DataFrame:
        """factor_panel: wide DataFrame indexed by (date, code), columns=factor names.
        Returns: DataFrame[date, code, score]."""
```

`EqualRankCombiner`:
- 每个因子在每个日期截面做 `rank(pct=True, method="average")`
- 跨因子简单算术平均
- 缺失处理：任何因子在某 (date, code) 为 NaN → 整行从该日剔除（严格模式，避免 mask 污染 rank）

### 5.2 `UniverseFilter`

```python
class UniverseFilter(ABC):
    @abstractmethod
    def apply(self, codes: list[str], at: pd.Timestamp,
              context: FilterContext) -> list[str]:
        """Return the subset of codes that pass this filter on date `at`."""
```

`FilterContext` 包含 `ohlcv_panel: pd.DataFrame`（OHLCV 全量，截至 at），避免每个 filter 自己去拉数据。

`SuspendedFilter`:
- 停牌判定：`at` 日 OHLCV 不存在，或 `volume == 0`

`NewListingFilter(min_days=60)`:
- 上市不满 `min_days` 个**自然日**：`at - first_seen_in_ohlcv < min_days`
- `min_days` 默认 60（约 3 个月）

`FilterChain`: 按顺序应用多个 filter，累计缩小池子。短路：任一 filter 返回空集即返回空集。

### 5.3 `Selector`

```python
class Selector(ABC):
    @abstractmethod
    def select(self, scores: pd.Series, universe: list[str]) -> list[str]:
        """scores: index=code, value=score on a single date.
        universe: already filtered list of codes.
        Returns: selected codes (subset of universe)."""
```

`TopQuantileSelector(quantile=0.1)`: 选 scores 最高的前 `quantile × len(universe)` 只。`max(1, round(...))` 保底至少 1 只。

`TopNSelector(n=50)`: 选 top-n。不足 n 则返回全部 universe。

### 5.4 `Weighter`

```python
class Weighter(ABC):
    @abstractmethod
    def weigh(self, selected: list[str], context: WeighterContext) -> pd.Series:
        """Returns: Series indexed by code, values sum to 1.0."""
```

`WeighterContext` 预留字段：
- `price_panel: pd.DataFrame`（用于未来 InverseVolWeighter）
- `mcap_panel: pd.DataFrame | None`（用于未来 MarketCapWeighter）
- `scores: pd.Series`（用于 score-weighted）

MVP 只用到 `selected`，但 context 留好字段，插件替换不改接口。

`EqualWeighter`: `Series(1.0 / n, index=selected)`。

### 5.5 `PortfolioConstraint`

```python
class PortfolioConstraint(ABC):
    @abstractmethod
    def apply(self, weights: pd.Series,
              context: ConstraintContext) -> pd.Series:
        """Return new weights. Must still sum to 1.0 when done."""
```

`MaxWeightConstraint(max_weight=0.03)`:
- 算法：迭代裁剪 — 把 weight > max_weight 的裁到 max_weight，剩余权重按原比例重分到其他未封顶的票；重复直到收敛或全部封顶
- 边界：若 `n_selected * max_weight < 1.0`（即无论如何分都凑不够 100%），直接抛 `ValueError`（说明 selector 选的票太少或 max_weight 太严）

`IndustryExposureConstraint(max_industry_weight=0.30, industry_map=None)`:
- `industry_map=None` → no-op（warn 一次）
- 否则：对每个行业总权重 > 上限，按比例缩放该行业内所有权重；剩余权重重分给其他行业
- MVP 不做精确最优解（那需要 LP/QP），只做启发式裁剪；warning 记录偏离度

### 5.6 `RebalanceSchedule`

```python
class RebalanceSchedule(ABC):
    @abstractmethod
    def rebalance_dates(self, start: date, end: date,
                        trade_calendar: list[date]) -> list[date]:
        ...
```

`DailyRebalance`: 返回 `[d for d in trade_calendar if start <= d <= end]`

`WeeklyRebalance(weekday="FRI")`:
- 对每个自然周，取 `weekday` 或该周内小于等于 weekday 的最后一个交易日
- 示例：周五放假 → 取周四；连续放假 → 取再往前的交易日

### 5.7 `Storage`

和 sub-1 `factors/storage.py` 完全对称：
- 根目录：`INSTOCK_HOLDING_ROOT` 环境变量，默认 `data/holdings`
- 文件布局：`<root>/<strategy>/<year>.parquet`
- `write_holding(strategy, df)`:
  1. pandera schema 校验
  2. 校验 sum=1.0 不变式（groupby date）
  3. 按 year 切分，append 到 `<year>.parquet`；(date, code, strategy) 去重，keep="last"
- `read_holding(strategy, start, end)`:
  - 按年读取所有命中文件，concat，过滤到 `[start, end]`

---

## 6. `StrategyConfig` 与 `StrategyPipeline`

### 6.1 `StrategyConfig`（dataclass）

```python
@dataclass
class StrategyConfig:
    name: str                                    # 策略名，写入 HoldingSchedule.strategy
    factors: list[str]                           # 要消费的因子名（from Sub-1 registry）
    combiner: FactorCombiner = field(default_factory=EqualRankCombiner)
    filters: list[UniverseFilter] = field(default_factory=lambda: [
        SuspendedFilter(),
        NewListingFilter(min_days=60),
    ])
    selector: Selector = field(default_factory=lambda: TopQuantileSelector(0.1))
    weighter: Weighter = field(default_factory=EqualWeighter)
    constraints: list[PortfolioConstraint] = field(default_factory=lambda: [
        MaxWeightConstraint(max_weight=0.03),
    ])
    schedule: RebalanceSchedule = field(default_factory=lambda: WeeklyRebalance("FRI"))
    universe_resolver: Callable[[date], list[str]] = None  # None = 复用 sub-1 _resolve_universe
```

### 6.2 `StrategyPipeline.run(start, end, config) -> pd.DataFrame`

流程：
1. 取交易日历：`get_source().get_trade_calendar(start, end)`
2. 取调仓日：`config.schedule.rebalance_dates(start, end, calendar)`
3. 一次性读取：
   - 每个 `config.factors` 的 factor df（`read_factor`），拼成 wide panel
   - OHLCV panel（覆盖 `[start - 90d, end]`，用于 filter context）
4. 合成：`scores_panel = config.combiner.combine(factor_panel)`  → DataFrame[date,code,score]
5. 对每个调仓日 `T` in rebalance_dates：
   - 取 universe（`config.universe_resolver(T)` or sub-1 的 `_resolve_universe(T)`）
   - 构造 `FilterContext`（OHLCV 截至 T）
   - `filtered = FilterChain(config.filters).apply(universe, T, ctx)`
   - `scores_T = scores_panel.query("date == @T").set_index("code")["score"]`
   - `selected = config.selector.select(scores_T.loc[filtered], filtered)`
   - `weights = config.weighter.weigh(selected, weighter_ctx)`
   - 依次 `weights = constraint.apply(weights, constraint_ctx)`
   - 收集 `(T, code, weight, score_T[code], config.name)` 行
6. 拼成 DataFrame，schema 校验，返回

### 6.3 错误处理

- 管线内任何单日失败：log warn，继续下一个调仓日（避免全量作业因单日数据缺失而挂）
- 合成 / schema 校验 / MaxWeight infeasible 等 fail-fast 错误：抛出，作业层 try/except per 策略
- 空 universe / 空 selection：返回空行，warn log

---

## 7. 作业：`generate_holdings_daily_job`

```python
def run(start: date, end: date,
        configs: list[StrategyConfig]) -> dict[str, Exception]:
    """Run every strategy for [start, end]; return per-strategy errors."""
    errors = {}
    for cfg in configs:
        try:
            df = StrategyPipeline().run(start, end, cfg)
            if df.empty:
                log.warning("strategy %s produced no rows", cfg.name)
                continue
            storage.write_holding(cfg.name, df)
        except Exception as exc:
            log.exception("strategy %s failed", cfg.name)
            errors[cfg.name] = exc
    return errors
```

CLI：`python -m instock.job.generate_holdings_daily_job [start] [end]`

`__main__` 块里硬编码一个默认策略（"default"，用全部已注册因子），和 Sub-1 的作业对称。

---

## 8. 测试策略

### 8.1 单元测试（每个 ABC 实现一个单测）

- `tests/portfolio/test_schemas.py` — 合约 + 不变式
- `tests/portfolio/test_combiner.py` — `EqualRankCombiner`
- `tests/portfolio/test_filters.py` — `SuspendedFilter`, `NewListingFilter`, `FilterChain`
- `tests/portfolio/test_selector.py` — `TopQuantileSelector`, `TopNSelector`, 边界（空集、n>universe）
- `tests/portfolio/test_weighter.py` — `EqualWeighter`
- `tests/portfolio/test_constraints.py` — `MaxWeightConstraint`（含 infeasible 抛错）、`IndustryExposureConstraint`（`industry_map=None` 跳过；有图时按比例裁剪）
- `tests/portfolio/test_schedule.py` — `WeeklyRebalance` 遇周末 / 节假日回退
- `tests/portfolio/test_storage.py` — roundtrip、年分区、去重

### 8.2 集成测试

- `tests/portfolio/test_pipeline_integration.py`
  - 小 fixture：5 只股票、10 个交易日、2 个因子
  - 跑通 `StrategyPipeline.run` → 校验 schema、weight sum、max weight 被满足

### 8.3 真实数据 smoke（Q8 B 门槛）

- `tests/portfolio/test_real_data_smoke.py`（默认 skip，需要 `INSTOCK_FACTOR_ROOT` 指向真实数据）
- 读取 sub-1 已落盘的 momentum/PE/PB 等因子
- CSI 300 × 3 个月跑通 StrategyPipeline
- 断言：
  - 每个调仓日 weight 和 = 1.0
  - 每只票 weight ≤ 3%
  - 每个调仓日都产出了非空持仓
  - 无 schema 错误
- 行业分布 dump 到 stderr（肉眼检查用）

---

## 9. 依赖与进入门槛

### 进入 Sub-2 前必须完成（Sub-1 follow-up 4 条硬门槛）
已于 commit `d7c22f4` 全部关闭。

### 进入 Sub-3 前必须完成（Sub-2 的 follow-up 硬门槛）
- [ ] Industry map 真实数据源接入（目前 `IndustryExposureConstraint` 默认 no-op）
- [ ] 股票上市日期的真实数据源（目前 `NewListingFilter` 用 OHLCV 最早日期近似）
- [ ] ST / 涨跌停过滤（目前被 YAGNI 掉）

这些不卡 Sub-2 MVP 合并，但卡 Sub-3 回测开工。

---

## 10. 里程碑与 task 切分

预估 14 个 task，按 subagent-driven-development 执行：

1. Schema + `HoldingScheduleValidationError` + 不变式校验辅助函数
2. Storage（write / read / 测试）
3. `FactorCombiner` ABC + `EqualRankCombiner`
4. `UniverseFilter` ABC + `FilterChain` + `SuspendedFilter` + `NewListingFilter`
5. `Selector` ABC + `TopQuantileSelector` + `TopNSelector`
6. `Weighter` ABC + `EqualWeighter`
7. `PortfolioConstraint` ABC + `MaxWeightConstraint`
8. `IndustryExposureConstraint`（skeleton）
9. `RebalanceSchedule` ABC + `DailyRebalance` + `WeeklyRebalance`
10. `StrategyConfig` + `StrategyPipeline`（主集成）
11. `generate_holdings_daily_job` CLI
12. 集成测试（小 fixture）
13. 真实数据 smoke 测试（Q8 B 门槛）
14. 端到端自检 + follow-up 清单更新

---

## 11. 开放 / 待后续澄清

- `HoldingSchedule` dataclass wrapper 是否真的需要？pandera 只校验 DataFrame，调用方直接用 DataFrame 是否更干净？倾向**不做 wrapper**（YAGNI），§4 表里暂留 `holding.py` 作占位，task 规划阶段如未被其他模块需要则删除。
- `StrategyPipeline` 是否需要缓存 factor panel / ohlcv panel？MVP 不做缓存，每次作业重新读；如果性能成为问题，follow-up 引入。
- `RebalanceSchedule.WeeklyRebalance` 在月底 / 年底跨自然周的边界行为（例如 12/31 是周四）：按"自然周内最后一个不晚于 weekday 的交易日"实现，写单测钉死。

---

## 12. 与 Sub-1 的对称性 checklist

- [x] 目录结构：`instock/portfolio/` 对称 `instock/factors/`
- [x] Schema 位置：`instock/portfolio/schemas.py` 对称 `instock/datasource/schemas.py`
- [x] Storage 设计：Parquet 按年分区 + `INSTOCK_*_ROOT` 环境变量 + 去重 keep="last"
- [x] 作业风格：`instock/job/generate_holdings_daily_job.py` 对称 `factor_compute_daily_job.py`；CLI 0/1/2 个日期参数
- [x] 错误分层：作业层 per-strategy try/except（对应 sub-1 per-factor），管线内 fail-fast
- [x] 测试命名：`tests/portfolio/test_*.py` 对称 `tests/factors/test_*.py`
- [x] Schema 违规：立即 raise（不重试），对称 `SchemaValidationError`

---

## Self-Review Notes

**Placeholder scan**: 无 TBD / TODO，所有接口签名已给出。

**Internal consistency**: Schema 定义（§3）与 pipeline 产出列（§6.2 第 5 步）一致；`StrategyConfig` 默认值（§6.1）与 §2 架构图一致；task 切分（§10）覆盖所有模块（§4）。

**Scope check**: 14 tasks 单 plan 可承载。若未来发现 task 10（pipeline 集成）膨胀超过 5 个文件改动，拆成 10a（管线 + filter/scoring 串联）+ 10b（constraints/schedule 串联）。

**Ambiguity check**:
- `HoldingSchedule.date` 语义（§3.3）已明确 T 日生成 T+1 执行
- `MaxWeightConstraint` 的 infeasible 边界（§5.5）已明确抛错
- `NewListingFilter` 的 "上市日" 用 OHLCV 最早日期近似已明确
- `WeeklyRebalance` 遇节假日回退已明确
