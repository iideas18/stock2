# Sub-project 3 — 回测与交易模拟引擎（Backtest Engine）设计 spec

**创建日期**：2026-04-18
**定位**：在 Sub-2 产出的 `HoldingSchedule` 上做严格、可复现的历史回测；
为 alpha 研究提供可信净值曲线、绩效拆解与样本外验证。
**前置**：Sub-1 ✅、Sub-2 ✅、Sub-2.5 ✅
**后续**：Sub-4（Web 与监控）

---

## 1. 范围与非目标

### 范围（MVP 6 件交付物）
1. **事件驱动日频回测器**：按交易日推进 portfolio 状态机
2. **交易约束层**：T+1 卖出、停牌跳过、涨跌停封板不可成交、退市清仓
3. **成本模型**：佣金/印花税/过户费 + 滑点（bps 模型）
4. **绩效分析**：年化、夏普、最大回撤、Calmar、胜率、超额、按年/月拆解
5. **Walk-forward 验证**：滚动训练窗 → 样本外持仓 → 拼接评估
6. **HTML 回测报告**：净值/回撤曲线、持仓分布、benchmark 对比、IS/OOS 差异水印

### 非目标（显式不做）
- 实盘下单 / 订单簿 / Level-2 模拟
- 期权、期货、可转债
- T+0 日内策略
- 多账户、保证金、融券
- 跨市场（港股、美股）
- 实时回测（live replay）；只做离线历史
- 优化器自动 walk-forward 调参（只支持 walk-forward 评估，不做参数搜索）

### MVP 验收门槛
- [ ] 在至少 2 个完整年度（如 2022-01 → 2023-12）跑通一个 Sub-2 默认策略
- [ ] 手续费、滑点可参数化关闭，便于 A/B
- [ ] 回测结果二次运行 byte-for-byte 一致（输入快照 fingerprint + 固定随机种子）
- [ ] 至少一个真数据冒烟测试（`INSTOCK_SUB3_SMOKE=1`）
- [ ] HTML 报告含 IS/OOS 拆分 + refdata 水印（"st/industry as of YYYY-MM-DD"）

---

## 2. 模块结构

```
instock/
├── backtest/                              ← 新增包
│   ├── __init__.py
│   ├── schemas.py                         ← pandera schemas（trade / position / nav / metrics）
│   ├── engine.py                          ← BacktestEngine（事件循环主控）
│   ├── portfolio_state.py                 ← Portfolio 状态机（cash / positions / pending T+1）
│   ├── execution.py                       ← Executor：撮合、滑点、成交价
│   ├── costs.py                           ← FeeModel + SlippageModel ABC + 默认实现
│   ├── constraints.py                     ← TradeConstraint：停牌 / 涨跌停 / T+1 / 退市
│   ├── metrics.py                         ← 绩效计算（年化、夏普、回撤、Calmar、胜率、超额）
│   ├── benchmarks.py                      ← Benchmark 加载（沪深300 / 中证500 / 中证1000）
│   ├── walkforward.py                     ← WalkForwardRunner（滚动窗 + IS/OOS 拼接）
│   ├── storage.py                         ← Parquet 写：trades / positions / nav
│   ├── fingerprint.py                     ← 输入快照 SHA（持仓清单 + OHLCV + refdata + 配置）
│   └── report.py                          ← HTML 报告（Jinja2 复用 Sub-1 模板风格）
├── job/
│   └── backtest_run_job.py                ← CLI 入口
└── tests/backtest/                        ← 完整单测 + smoke
```

---

## 3. 核心数据契约

### 3.1 输入

| 名称 | 来源 | 必需 | 说明 |
|------|------|------|------|
| `HoldingSchedule` | Sub-2 Parquet | 是 | `[date, code, weight, score, strategy]` |
| OHLCV panel | `OhlcvPanelStore`（Sub-2.5） | 是 | qfq；含 open/high/low/close/volume/amount |
| trade_calendar | IDataSource | 是 | A 股交易日 |
| `industry_map(at)` | refdata | 否 | 用于报告归因 |
| `st_flags(at)` | refdata | 否 | 用于交易约束（ST 强制 5% 板块） |
| `listing_dates` | refdata | 否 | 用于退市/上市校验 |
| benchmarks | IDataSource | 否 | 默认 hs300+csi500+csi1000 |

### 3.2 输出

#### TradeRecord
```
[date, code, side, target_w, filled_shares, fill_price, fill_value,
 commission, stamp_tax, transfer_fee, slippage_value, gross_value,
 net_cash_change, reason]
```
- `side ∈ {BUY, SELL}`
- `reason ∈ {REBALANCE, ST_FORCE_OUT, DELIST_FORCE_OUT, NO_OP}`

#### PositionSnapshot（每日 EOD）
```
[date, code, shares, avg_cost, market_value, unrealized_pnl, weight]
```

#### NavCurve（每日）
```
[date, nav, cash, position_value, total_value, ret_daily, ret_cum,
 turnover_daily, n_holdings]
```

#### MetricsSummary（一次回测一行）
```
{run_id, strategy, start, end,
 ret_annual, ret_total, vol_annual, sharpe, sortino,
 max_drawdown, max_dd_duration, calmar,
 win_rate_daily, win_rate_monthly,
 alpha_vs_<bench>, beta_vs_<bench>, ir_vs_<bench>,
 turnover_annual, total_cost_bps,
 fingerprint_sha, refdata_as_of}
```

存储路径：
- trades / positions / nav：`<INSTOCK_BACKTEST_ROOT>/<run_id>/{trades,positions,nav}.parquet`
- metrics：`<INSTOCK_BACKTEST_ROOT>/_metrics.parquet`（append-by-run_id）
- report：`<INSTOCK_BACKTEST_ROOT>/<run_id>/report.html`

`run_id = f"{strategy}_{start}_{end}_{fingerprint_sha[:8]}"`

---

## 4. 事件循环（核心算法）

```
for d in trade_calendar[start..end]:
    # 1. EOD of previous day → mark-to-market
    portfolio.mark_to_market(prices_close[d-1])

    # 2. 开盘前：处理今日生效的目标持仓（来自 d-1 EOD 决策）
    if d in pending_orders:
        orders = pending_orders.pop(d)
        for order in orders:
            constraints.check(order, ohlcv[d])  # 涨跌停封板？停牌？退市？
            executor.execute(order, prices_open[d], slippage, fees)

    # 3. EOD：mark-to-market on close
    portfolio.mark_to_market(prices_close[d])
    nav.append(d, portfolio.snapshot())

    # 4. EOD：检查 d 是否触发新一轮 rebalance（HoldingSchedule 含 d 行）
    if d in holding_schedule.dates:
        target = holding_schedule.loc[d]
        # 强制清仓：ST 升级、退市预警
        target = apply_force_outs(target, st_flags(d), listing_dates)
        # 计算订单 → 推到 d+1 早盘
        orders = compute_orders(portfolio, target, prices_close[d])
        pending_orders[next_trade_day(d)] = orders
```

**T+1 实现**：`HoldingSchedule[d]` 在 `d` 收盘后产出订单，`d+1` 开盘成交。
**乘数**：100 股最小交易单位；权重换算到目标股数时按 lot_size 取整，剩余权重转为现金缓冲（pro-rata）。
**重要**：开盘价缺失（停牌）→ 订单延期到下一交易日；连续 5 个交易日延期 → 报警丢弃。

---

## 5. 成本与滑点模型

### 5.1 默认 FeeModel（A 股标准）
```
commission   = max(value * 0.00025, 5.0)   # 0.025%，最低 5 元
stamp_tax    = value * 0.0005 if side == SELL else 0     # 印花税 0.05%（卖方）
transfer_fee = value * 0.00001                            # 过户费 0.001%（沪市），简化全市场计提
```
- 全部参数化，可关闭（`FeeModel.zero()`）做对照。
- 退市清仓：豁免最低佣金；其他费用照计。

### 5.2 默认 SlippageModel
```
fill_price = open_price * (1 + side_sign * bps * 1e-4)
# 默认 bps = 5（即 0.05%）
```
- 备选：`VolumePctSlippage`（按 ADV 比例缩放）；MVP 仅实现 bps，VolumePct 留 ABC 占位。
- `SlippageModel.zero()` 用于 A/B。

---

## 6. 交易约束（TradeConstraint 链）

按顺序检查；任一失败 → 订单跳过 + 写入 `TradeRecord(reason=...)`：

1. **SuspendedConstraint**：`volume[d] == 0` → 跳过（pending 延期）
2. **LimitUpConstraint**：开盘价 ≥ 昨收 × (1 + threshold − ε) 且 BUY → 跳过；按板块差化阈值
   （主板 10% / 创业板&科创板 20% / 北交所 30% / ST 5%，复用 Sub-2.5 `default_thresholds`）
3. **LimitDownConstraint**：开盘价 ≤ 昨收 × (1 − threshold + ε) 且 SELL → 跳过
4. **DelistConstraint**：listing_dates 缺失或 OHLCV 已断 N 日 → 强制 SELL（market）
5. **T1Constraint**：当日 BUY 不可当日 SELL（只在 same-day 多次调仓时生效；MVP 单日只一次）

---

## 7. 绩效与归因

### 7.1 基础指标（metrics.py）
- 年化收益、年化波动、夏普、Sortino
- 最大回撤、最大回撤持续期、Calmar
- 日胜率、月胜率
- 年化换手 = Σ|Δw| / 年数
- 总成本 bps = Σ(commission + stamp_tax + slippage_value) / Σ |trade_value|

### 7.2 Benchmark 对比（multi-benchmark）
- 默认同时计算 vs hs300 / csi500 / csi1000
- 每个 benchmark 输出 alpha / beta / IR / 超额回撤
- benchmark 价格来自 IDataSource；缺失 → 跳过该 benchmark + warn

### 7.3 拆解视图（HTML 报告）
- 净值曲线（vs benchmarks）
- 回撤曲线
- 月度收益热力图
- 行业暴露时序（依赖 industry_map；缺失 → 占位"refdata not available"）
- 持仓数 / turnover 时序
- IS vs OOS 对比表（walk-forward 启用时）

---

## 8. Walk-Forward 验证（MVP 必须）

### 8.1 配置
```python
WalkForwardConfig(
    train_window_months=12,
    test_window_months=3,
    step_months=3,
    min_train_obs=200,    # 最少训练样本数门槛
)
```

### 8.2 流程
1. 切窗：`(t0..t0+12m)` 训练 → `(t0+12m..t0+15m)` 测试 → 滚动 step=3m
2. **训练阶段**：当前 MVP 不做参数自动搜索；仅产生"该窗内的 IS HoldingSchedule"
   （由 Sub-2 pipeline 配置，参数固定。Walk-forward 主要价值在 OOS 拼接 + IS/OOS 漂移诊断）
3. **测试阶段**：用 `t0+12m` 时点已知的因子 → 生成 OOS 持仓 → 进入 backtester
4. **拼接**：所有 OOS 段拼成总 NAV，IS 段拼成另一条 NAV
5. **诊断指标**：IS sharpe vs OOS sharpe 差值；超过阈值 → HTML 报告高亮

### 8.3 不做（留 Sub-3 follow-up）
- 自动参数网格 / 贝叶斯优化
- 嵌套 walk-forward
- Bootstrap 置信区间

---

## 9. 复现性（fingerprint + 快照）

### 9.1 输入快照 SHA
fingerprint 输入：
- `HoldingSchedule` Parquet 文件 SHA-256
- 涉及到的 OHLCV Parquet 分区文件 SHA-256（按 (year, code) 排序）
- 涉及到的 refdata 快照文件 SHA-256
- `BacktestConfig` JSON SHA（包括 fee/slippage/benchmark 配置）
- `INSTOCK_BACKTEST_RNG_SEED`（默认 42）

输出：`fingerprint_sha = sha256(sorted(input_shas))`

### 9.2 复现性合同
- 同一 fingerprint → 相同 trades / nav / metrics（byte-for-byte）
- fingerprint 写入 metrics 行 + HTML 报告 footer
- CI 测试：跑同一配置两次 → diff 必须为空

### 9.3 不做
- 自动数据归档（依赖 Parquet 文件不被外部覆盖；Sub-2.5 已确保 refdata as-of 写入）
- 跨机器哈希一致性（依赖 pyarrow 同版本）

---

## 10. CLI 与作业接口

### 10.1 单次回测
```
python -m instock.job.backtest_run_job \
    --strategy default_topq \
    --start 2022-01-04 --end 2023-12-29 \
    --benchmarks hs300,csi500,csi1000 \
    --walkforward 12,3,3 \
    --report
```

### 10.2 默认策略列表
复用 Sub-2 `_default_configs()`；可 `--strategy all` 批量。

### 10.3 不做
- 长驻 daemon / cron 集成（Sub-4）
- 多策略并行（MVP 单进程顺序）

---

## 11. 与 Sub-2.5 的对接

Backtester 必须：
1. 接受 `OhlcvPanelStore` 实例，而非直接 `IDataSource.get_ohlcv("ALL", ...)`
2. 每次 rebalance 时按 `at=d` 调 `read_industry_map / read_st_flags / read_listing_dates`
3. HTML 报告 footer 写入 "refdata as of YYYY-MM-DD; historical ST/industry approximated"
4. `_load_ohlcv_panel("ALL", ...)` 占位件清理（替换为 OhlcvPanelStore 注入）

---

## 12. 测试策略

### 单元测试（≥40 个）
- portfolio_state：cash/positions 进出 + mark-to-market
- execution：T+1 延迟 + 涨跌停封板跳过 + 退市强制
- costs：佣金最低 5 元、印花税仅卖方、bps 滑点对称
- constraints：每条 constraint 单独 + 链式短路
- metrics：sharpe / max_dd / Calmar 已知数值校验
- walkforward：窗切分边界、min_train_obs 门槛
- fingerprint：相同输入 → 相同 SHA；任一变化 → SHA 变
- benchmark：缺失 benchmark 不崩，跳过 + warn
- report：HTML 渲染不抛异常 + 含必要 section

### 冒烟测试
- `INSTOCK_SUB3_SMOKE=1`：跑一个真实 6 个月窗口（2023-07 → 2023-12）默认策略，
  断言：trades 行数 > 0、NAV 单调递推、metrics 全字段非空、fingerprint 稳定

### 不做
- 实盘对账（无实盘）
- 大窗口性能 benchmark（>10 年）

---

## 13. 风险与已知 trade-offs

| 风险 | 缓解 |
|------|------|
| ST/industry 当前快照 look-ahead | 报告水印；follow-up 等待历史快照源 |
| 早盘开盘价滑点估计偏小 | 默认 5 bps 偏保守；提供 `--slippage-bps` 调参 |
| 100-股取整剩余权重转现金会拖累净值 | 在 metrics 输出 `lot_drag_bps` 单独行 |
| Walk-forward 训练阶段不做调参 | MVP 仅验证 IS/OOS 漂移；调参列入 follow-up |
| pyarrow 跨版本哈希不稳 | requirements.txt 锁版本；fingerprint footer 写 pyarrow 版本 |
| 涨跌停 ε 阈值（1e-3）可能误判涨幅 9.99% | 与 Sub-2.5 LimitFilter 一致，已是市场惯例 |

---

## 14. 进入 Sub-4 前必须完成

- [ ] 至少 1 个完整 2 年回测跑通 + 报告产出
- [ ] CLI 双次运行 fingerprint 一致
- [ ] Sub-3 follow-up 文档建立
- [ ] Sub-2 占位件 `_load_ohlcv_panel("ALL", ...)` 清理

---

## 15. 横切事项

- **复现性**：本子项目首次引入 fingerprint；Sub-4 调度需读取并展示
- **报告模板**：复用 Sub-1 Jinja2 风格；为 Sub-4 Web 嵌入预留 `<div id="bt-report">` 锚点
- **walkforward 拼接 NAV**：未来可能需要被 Sub-4 监控 daily refresh —— 接口预留批处理调用入口
