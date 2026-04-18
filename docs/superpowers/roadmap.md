# InStock 专业量化研究平台 — 总路线图

**创建日期**: 2026-04-18
**定位**: 专业量化研究平台（以因子研究、策略验证、组合构建为核心；Web 作为研究员内部工具，不是对外产品）
**交付哲学**: 每个子项目 MVP 跑通 → follow-up 打磨到位 → 再进下一个（上门槛优先于速度）

---

## 目标与非目标

### 终极目标
- 研究员能在这套平台上**独立完成**从原始数据到可部署策略的全链路：
  1. 拉数据 → 2. 造因子 → 3. 评估因子 → 4. 合成 alpha → 5. 组合构建 → 6. 严格回测 → 7. 上线监控
- 每一步都**可复现**、**数据对齐无前视**、**有自动化测试**、**产出可审计**。

### 非目标
- 不做 2C 产品；Web 只服务于研究员自己。
- 不追求实盘交易闭环；Sub-5 先到"生成目标持仓清单"为止。
- 不追求框架通用性；只服务 A 股。

---

## 已完成

### Sub-project 1 ✅ 数据与因子工程（已合并到 master，tag `subproject-1-data-factors-mvp`）
**交付物**：
- `IDataSource` Protocol + akshare 实现 + Tushare 空壳 + registry
- pandera schema（OHLCV / 财务 PIT / 龙虎榜 / 北向 / 资金流）
- retry 装饰器 + RateLimiter
- MySQL 新表 DDL（PIT 财务、LHB v2、北向、资金流）
- `Factor` ABC + registry + 预处理管线（winsorize/neutralize/zscore）
- Parquet 按年分区存储
- 代表性因子：`mom_Nd` / `pe_ttm`/`pb`/`roe_ttm`（PIT as-of）/ `lhb_heat_Nd` / `north_holding_chg_Nd`
- 评估器：IC / RankIC / IC_IR / 分层 / 换手 + Jinja2 HTML 报告
- 日频因子计算作业 + CLI

**follow-up**: `docs/superpowers/followups/subproject-1-data-factors.md`

---

## 规划中

### Sub-project 2 ✅ 选股与组合构建（已合并到 master，tag `subproject-2-portfolio-construction-mvp`）

**实际交付**（MVP）：
- `instock/portfolio/` 包：schema + storage + 6 个组件 ABC + 默认实现
  - `HoldingSchedule` pandera schema（`weight∈[0,1]`、6 位 code、score nullable）
    + `validate_holding_invariants` 强制 (date,strategy) 内 weight.sum()=1±1e-6 + code 唯一
  - Parquet 按年分区存储（`<INSTOCK_HOLDING_ROOT>/<strategy>/<year>.parquet`）
  - `EqualRankCombiner`（per-date `rank(pct=True, method="average")` 后均值）
  - `SuspendedFilter`（volume==0 / 缺行）+ `NewListingFilter(min_days)`（首见日历日近似）+ `FilterChain`
  - `TopQuantileSelector(quantile)`（`max(1, round(n*q))` 下限）+ `TopNSelector(n)`
  - `EqualWeighter` + `WeighterContext(price_panel, mcap_panel, scores)`
  - `MaxWeightConstraint`（迭代 clip+按比例再分配，`n*cap<1` 抛错）
    + `IndustryExposureConstraint`（None map 时 no-op；否则按行业缩放再分配）
  - `DailyRebalance` + `WeeklyRebalance(weekday)`（节假日回退：取目标日往前同周内最后一个交易日）
  - `StrategyConfig` + `StrategyPipeline`：combine → filter → select → weigh → constrain → 行
- 日频生成作业 `instock/job/generate_holdings_daily_job.py` + CLI + `_default_configs()`
- 真实数据冒烟测试（`INSTOCK_SUB2_SMOKE=1` 才跑）

**测试**: 83 passed + 1 skipped（real-data smoke）。无 FutureWarning 新增。

**MVP 验收已达成**：
- 单/多因子 → `HoldingSchedule` pipeline 跑通（`tests/portfolio/test_pipeline.py`）
- 行业中性约束（None map 时正确 no-op + warn 一次）+ 单票上限均有单测
- weight.sum()=1±1e-6 由 schema invariant 强制

**follow-up**: `docs/superpowers/followups/subproject-2-portfolio-construction.md`
（含一项跨切关键问题：Sub-1 factor registry 实际为空的潜伏 bug，详见 follow-up A 区）

---

### ~~Sub-project 2 🎯 选股与组合构建（下一步）~~  <!-- 旧文案保留以下作为历史记录 -->
**定位**: 把原始因子变成可下单的持仓权重。是 alpha 研究的主干，也是整套平台价值链最长的一段。

**核心交付**：
1. **因子合成层**
   - 等权 / IC 加权 / IC_IR 加权 / rank 平均
   - 因子协方差估计（最简 Ledoit-Wolf shrinkage）
   - 去相关化 / 正交化（schmidt）
2. **选股引擎**
   - 分层选股（top-N 分位）
   - 带约束的选股（行业中性 / 市值中性 / 单票上限）
3. **组合构建器**
   - 等权 / 市值加权 / 逆波动率加权
   - 简版 mean-variance 优化（cvxpy 可选依赖；不强制）
   - 单票权重上下限、行业偏离上限、总换手上限
4. **约束与规则层**
   - 行业/风格暴露约束（接 industry_map，真实化 sub-1 的占位）
   - ST / 停牌 / 上市不满 N 天 过滤
   - 涨跌停不可交易过滤
5. **输出契约**
   - `HoldingSchedule` DataFrame：`[date, code, weight, target_shares?]`
   - 写入 Parquet / MySQL，供 Sub-3 回测和 Sub-4 监控消费

**依赖**: Sub-1 的 factor storage + evaluator。

**门槛**（MVP 验收）：
- 从单因子 / 多因子 → 组合权重的 pipeline 跑通
- 在至少 3 个月历史区间产出合法 `HoldingSchedule`
- 行业中性 / 单票上限约束均有单测证明被满足
- 与 sub-1 的因子评估器对接：能读取因子后直接跑选股

**follow-up 进入 sub-3 前必须完成**：行业数据源真实化（不再用 None / 空 dict）。

---

### Sub-project 2.5 ✅ 数据层对齐（已合并到 master，tag `subproject-2.5-data-alignment-mvp`）

**实际交付**（MVP）：
- `instock/refdata/` 包：industry / listing / st / ohlcv_store + schemas
  - `read_industry_map(at)`、`read_st_flags(at)`：as-of 查询最近快照
  - `read_listing_dates()` / `upsert_listing_dates`
  - `OhlcvPanelStore`：本地 Parquet 缓存 + 缺失时 IDataSource 在线补齐；
    按 trade_calendar 判缺失（不按 date.range）
- `instock/factors/bootstrap.py::register_default_factors()`：修 Sub-1 follow-up A
  silent-no-op bug；Sub-1 / Sub-2 daily job 共用
- `instock/portfolio/filters.py`：新增 `STFilter` + `LimitFilter`（按板块差化阈值
  10/20/30%、ST 5%）；`NewListingFilter` 改为优先用 `listing_dates`，缺失时回退
- `instock/portfolio/pipeline.py`：自动注入 refdata 到 `FilterContext` /
  `ConstraintContext`；缺 refdata 时各 filter/constraint 回退 + warn 一次
- 三个 refdata daily job：industry（推荐周）/ listing（推荐月）/ ST（推荐周）
- akshare fetcher：`board_industry_name_em` / `board_industry_cons_em` /
  `individual_info_em` / `zh_a_st_em`

**测试**：122 passed, 5 skipped（4 个 INSTOCK_SUB25_SMOKE smoke + Sub-2 旧 smoke）。
无 FutureWarning 新增。

**MVP 验收已达成**：
- refdata reader 从空态到有态的全链路单测
- `OhlcvPanelStore` cache-hit / cache-miss / trade_calendar-gap / 单 code 失败跳过单测
- Sub-2 `FilterContext` 扩展 + 真 industry_map 注入路径单测
- `INSTOCK_SUB25_SMOKE=1` 真数据冒烟 4 例

**follow-up**: `docs/superpowers/followups/subproject-2.5-data-alignment.md`

---

### Sub-project 3 ⏳ 回测与交易模拟引擎
**定位**: 在 Sub-2 产出的持仓清单上做严格的历史回测，这是 alpha 研究可信度的决定性环节。

**核心交付**：
1. **事件驱动回测器**
   - T+1 成交、涨跌停约束、停牌跳过
   - 按 `HoldingSchedule` 调仓（每日 / 每周 / 每月触发）
   - PnL / NAV / 持仓快照
2. **交易成本模型**
   - 固定费率（佣金、印花税、过户费）
   - 滑点：固定 bps / 成交量百分比 / VWAP 模型三选一
3. **绩效分析**
   - 年化收益、波动、夏普、最大回撤、Calmar、胜率
   - 对比基准（沪深 300 / 中证 500 / 可配置）
   - 分年度 / 分月度拆解
4. **Walk-forward / 样本外验证**
   - 滚动窗口训练 → 样本外持仓 → 拼接评估
   - 参数过拟合自查（同一策略在 IS/OOS 差异告警）
5. **回测报告**
   - 扩展 Sub-1 的 HTML 模板，加入净值曲线、回撤曲线、因子归因

**依赖**: Sub-2 的 `HoldingSchedule` 契约；Sub-1 的 OHLCV 数据。

**门槛**:
- 能在至少 2 个完整年度上跑通一个 sub-2 策略
- 手续费/滑点通过参数可关闭，便于 A/B 对比
- 回测结果可导出、可复现（固定随机种子 / 固定数据快照）

**显式不做**: 实盘下单、订单簿级别模拟、期权/期货、T+0。

---

### Sub-project 4 ⏳ 研究 Web 交互层 & 监控
**定位**: 研究员日常入口。**刻意放在第 4**，因为没有前三个子项目的沉淀，Web 页面也没东西展示。

**核心交付**：
1. **因子浏览器**
   - 因子列表（来自 registry）
   - 单因子时序图、截面分位分布
   - 直接内嵌 Sub-1 评估器生成的 HTML 报告
2. **策略浏览器**
   - Sub-2 策略列表、最新持仓、持仓变化 diff
   - Sub-3 回测结果对比面板（多策略并排）
3. **监控告警**
   - 日频作业健康（每个因子昨日是否成功写入）
   - 因子衰减告警（IC_IR 30 日 vs 历史阈值）
   - 数据源状态（akshare 今日拉取成功率）
4. **调度层**
   - 现有 `execute_daily_job` 扩展：加上 Sub-1 的 `factor_compute_daily_job`、Sub-2 的策略生成、Sub-3 的回测刷新
   - 失败重试、告警通知（先用邮件或 webhook，不做微信/钉钉）
5. **老系统切换**
   - 老选股作业切到新因子栈（Sub-1 follow-up 里已记录"跑稳一周后再切"的原则）

**依赖**: Sub-1/2/3 都得有东西可以展示。

**门槛**:
- 研究员一天的工作流能完全在 Web 上走通：看因子 → 选策略 → 看回测 → 看监控
- 所有页面数据来自 Parquet / MySQL，不再走临时脚本

---

## 依赖与并行可能

```
Sub-1 (数据与因子) ─┬─> Sub-2 (选股组合) ─┬─> Sub-3 (回测) ─┐
                    │                      │                  ├──> Sub-4 (Web/监控)
                    └──────────────────────┴──────────────────┘
```

- **不能并行**: Sub-2 严格依赖 Sub-1 的 factor storage；Sub-3 严格依赖 Sub-2 的 HoldingSchedule
- **可小部分并行**: Sub-4 里的"因子浏览器"其实 Sub-1 之后就能做，但把它放在 Sub-4 统一做可以避免 Web 技术栈与数据契约反复返工
- **横切事项**（跨多个子项目）：
  - industry_map 真实化（Sub-2 强制，Sub-1 预留接口）
  - 数据快照/版本化（Sub-3 回测复现性的前提；届时再决定放哪里）

---

## 每个子项目的"进入条件"

**进入 Sub-2 前必须完成（sub-1 follow-up 门槛）**：
- [ ] `NorthHoldingChgFactor` description 修正
- [ ] evaluator date-alignment docstring
- [ ] SchemaValidationError 回归测试（C-3）
- [ ] 作业 universe 改为以 `end` 为锚的 index_member（B-2，避免回测时成分股前视）

其余 follow-up 项可随 Sub-2 顺手做，不卡门槛。

**进入 Sub-3 前必须完成**：
- Sub-2 MVP 门槛全部达成
- `HoldingSchedule` schema 用 pandera 定义并测试（类比 Sub-1 的 schemas.py）
- 行业数据源真实化

**进入 Sub-4 前必须完成**：
- Sub-3 MVP 跑通
- Sub-1 老作业已切换到新表或明确废弃

---

## 里程碑汇总表

| 子项目 | 主题 | 核心价值 | 状态 | 预估 task 数* |
|-------|------|---------|------|---------------|
| 1 | 数据与因子工程 | 能造因子、能存、能评估 | ✅ 已合并 | 17（实际） |
| 2 | 选股与组合构建 | 因子 → 可下单权重 | ✅ 已合并 | 13（实际） |
| 2.5 | 数据层对齐 | refdata + OhlcvPanelStore + factor bootstrap | ✅ 已合并 | 14（实际） |
| 3 | 回测与交易模拟 | 策略历史可验证 | 🎯 下一步 | ~12–15 |
| 4 | Web 交互 & 监控 | 研究员日常入口 | ⏳ | ~15–20 |

*实际 task 数以各子项目单独 plan 为准。

---

## 本文件维护

- 每个子项目**启动时**：更新状态为 "🎯 进行中"；开独立 plan 文件到 `docs/superpowers/plans/`
- 每个子项目**完成时**：更新状态为 "✅ 已合并"；把 follow-up 链接指向 `docs/superpowers/followups/` 对应文件
- 发现跨子项目的共同问题：更新"横切事项"区块
- 方向变更：先改本文件、再改对应 plan；不要只改 plan
