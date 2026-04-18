# 子项目 1 —— 数据 & 因子工程 设计

**日期:** 2026-04-18
**状态:** Spec（待实现）
**上游 spec:** `2026-04-18-quant-platform-roadmap-design.md`
**项目:** InStock

## 背景与范围

路线图中的子项目 1。目标是把 InStock 的数据层从"能跑"升级到"可信"，并在其上建立因子库框架，为后续回测、风控、实盘奠基。

### 已敲定的范围决策

| 决策项 | 选择 | 含义 |
|--------|------|------|
| 数据频率 | 日频 | 不做日内、不做真实滑点建模；MySQL 继续作为主存储 |
| PIT 财务 | 只留首次公告日 | 每字段加 `announcement_date`；不保留修订版历史 |
| 因子库定位 | 完整因子平台 | 新增 `instock/factors/` 模块，含基类、预处理、评估器 |
| 爬虫层 | 换成熟库 | 用 akshare 替代自研爬虫 |
| 主数据源 | akshare 主 + Tushare 预留接口 | Tushare 接口签名先就位，实现空壳 |
| 因子存储 | MySQL + Parquet | MySQL 存行情/财务/龙虎榜原始数据；Parquet 存因子值 |
| 因子类别（第一期） | 技术面 / 估值质量 / 龙虎榜 / 资金流 | 四类全做 |
| 迁移风格 | 渐进替换 | 新链路与老爬虫并存，先上 3 个关键表 |
| Schema 校验 | pandera | 每个 DataSource 方法出口强制校验 |
| 老爬虫 | 保留却不入链路 | 作为应急工具 + 对账工具保留；不注册进 DataSource；akshare 挂了直接 raise |

### 非目标

- 分钟 / tick 级数据
- 修订版历史财务
- 自动失败降级到老爬虫
- 重构 / 删除老爬虫
- 任何回测、风控、实盘相关能力（在后续子项目）

## 架构

```
instock/
├─ datasource/              ← 新增
│   ├─ base.py              IDataSource Protocol
│   ├─ schemas.py           pandera 所有合约 schema
│   ├─ akshare_source.py    默认实现
│   ├─ tushare_source.py    空壳实现（NotImplementedError），接口就位
│   ├─ registry.py          数据源注册与选择
│   └─ io.py                retry / timeout / 限流装饰器
├─ factors/                 ← 新增
│   ├─ base.py              Factor 基类
│   ├─ registry.py          因子注册表
│   ├─ technical/           talib 技术指标因子
│   ├─ fundamental/         估值/质量因子（读 PIT 财务）
│   ├─ lhb/                 龙虎榜因子
│   ├─ flow/                资金流因子
│   ├─ preprocess.py        去极值 / 中性化 / 标准化
│   ├─ evaluator.py         IC / Rank IC / IC_IR / 分层回测
│   └─ storage.py           Parquet 读写（按年分区）
├─ job/
│   └─ factor_compute_daily_job.py   ← 新增：日频因子计算作业
├─ core/crawling/           ← 保留，不动，不入 DataSource 链路
└─ lib/database.py          ← 继续负责原始数据的 MySQL
```

## 组件设计

### 1. DataSource 抽象层

```python
# instock/datasource/base.py
class IDataSource(Protocol):
    def get_ohlcv(self, code: str, start: date, end: date, adjust: str) -> pd.DataFrame: ...
    def get_fundamentals_pit(self, code: str, fields: list[str]) -> pd.DataFrame: ...
    def get_lhb(self, start: date, end: date) -> pd.DataFrame: ...
    def get_north_bound(self, start: date, end: date) -> pd.DataFrame: ...
    def get_money_flow(self, code: str, start: date, end: date) -> pd.DataFrame: ...
    def get_index_member(self, index_code: str, at: date) -> list[str]: ...
    def get_trade_calendar(self, start: date, end: date) -> list[date]: ...
```

**设计约定：**
- Schema 合约写死；akshare/tushare 返回值在实现内部归一化到合约列名。
- 每个方法 `return` 前调用对应的 pandera schema `.validate(df)`，失败立即 raise。
- 所有网络 I/O 走 `io.py` 统一装饰器：timeout=10s，指数退避 retry×3，本地限流避免触发 akshare 内部 rate limit。
- `registry.py` 第一期只注册 `AkShareSource`；Tushare 接口类就位但不注册；akshare 失败 **直接 raise**，不做自动降级。

### 2. pandera Schema 示例

```python
# instock/datasource/schemas.py
import pandera.pandas as pa

OHLCV_SCHEMA = pa.DataFrameSchema({
    "date":   pa.Column(pa.DateTime),
    "code":   pa.Column(str, pa.Check.str_matches(r"^\d{6}$")),
    "open":   pa.Column(float, pa.Check.gt(0)),
    "high":   pa.Column(float, pa.Check.gt(0)),
    "low":    pa.Column(float, pa.Check.gt(0)),
    "close":  pa.Column(float, pa.Check.gt(0)),
    "volume": pa.Column(float, pa.Check.ge(0)),
    "amount": pa.Column(float, pa.Check.ge(0)),
}, unique=["date", "code"])

FUNDAMENTALS_PIT_SCHEMA = pa.DataFrameSchema({
    "code":              pa.Column(str),
    "report_period":     pa.Column(pa.DateTime),
    "announcement_date": pa.Column(pa.DateTime),
    # 其余字段 nullable=True
}, unique=["code", "report_period"])
```

### 3. MySQL 表结构

**新增表：**

```sql
CREATE TABLE cn_stock_fundamentals_pit (
    code VARCHAR(10),
    report_period DATE,
    announcement_date DATE,
    revenue DECIMAL(20,4),
    net_profit DECIMAL(20,4),
    total_assets DECIMAL(20,4),
    total_equity DECIMAL(20,4),
    operating_cf DECIMAL(20,4),
    roe DECIMAL(10,4),
    gross_margin DECIMAL(10,4),
    net_margin DECIMAL(10,4),
    pe DECIMAL(10,4),
    pb DECIMAL(10,4),
    ps DECIMAL(10,4),
    PRIMARY KEY (code, report_period),
    INDEX idx_ann (code, announcement_date)
);
```

**as-of 查询模板：**
```sql
SELECT * FROM cn_stock_fundamentals_pit
WHERE code = ? AND announcement_date <= ?
ORDER BY report_period DESC LIMIT 1;
```

其他新表：`cn_stock_lhb`（龙虎榜明细，以 `trade_date + code + seat` 为主键）、`cn_stock_north_bound`（北向持股日频）、`cn_stock_money_flow`（资金流日频）。现有 OHLCV 表沿用。

### 4. 因子基类

```python
# instock/factors/base.py
class Factor(ABC):
    name: str               # 唯一 ID
    category: str           # technical | fundamental | lhb | flow
    description: str
    frequency: str          # daily
    universe: str           # all_a | csi300 | ...
    dependencies: list[str] # 依赖的原始表或其他因子

    @abstractmethod
    def compute(self, universe: list[str], start: date, end: date) -> pd.DataFrame:
        """返回 long format: columns = [date, code, value]"""

    def preprocess(self, raw: pd.DataFrame) -> pd.DataFrame:
        """默认：winsorize → 行业市值中性化 → zscore"""
```

### 5. Parquet 因子存储

```
data/factors/
├─ <factor_name>/
│   ├─ 2020.parquet
│   ├─ 2021.parquet
│   └─ ...
└─ _metadata.json         所有已注册因子及元数据
```

按年分区便于增量写入与按时间切片读取。使用 `pyarrow` 后端，列式存储，读写速度远高于 MySQL。

### 6. 第一期因子清单

| 分类 | 因子 |
|------|------|
| 技术面 | mom_5d / mom_20d / mom_60d，rsi_14，macd_hist，atr_20，turnover_20d，amihud_illiq |
| 估值质量 | pe_ttm，pb，ps，roe_ttm，gross_margin，net_margin，net_profit_yoy |
| 龙虎榜 | lhb_heat（30 日内上榜次数），lhb_seat_winrate（关联席位历史胜率），lhb_top_seat_hit |
| 资金流 | north_holding_chg_5d，main_net_inflow_5d，big_order_ratio |

所有因子统一走 `preprocess` 默认管线（去极值 → 行业市值中性化 → zscore），再落 Parquet。

### 7. 因子评估器

```python
def evaluate(factor: Factor, start: date, end: date, n_groups: int = 10) -> FactorReport:
    """返回 IC / Rank IC / IC_IR / 分层收益 / 换手率 / 衰减曲线"""
```

输出一份 HTML 报告（Jinja2 模板 + matplotlib PNG 内嵌）。不引入新前端依赖。

### 8. 作业链路

```
akshare (每日收盘后)
   ↓ datasource (pandera 校验)
MySQL (OHLCV / fundamentals_pit / lhb / north / money_flow)
   ↓ factors
Parquet 因子文件（按年分区增量写入）
   ↓ evaluator（按需）
HTML 报告
```

新增 `instock/job/factor_compute_daily_job.py`，供 cron 调用。老作业 `basic_data_daily_job.py` 保持不动。

## 错误处理

- **DataSource 层：** 网络失败 retry×3 后 raise `DataSourceError`；schema 校验失败 raise `SchemaValidationError`，消息包含首个违规列与行号。
- **因子层：** 某因子计算失败不阻塞其他因子；整体作业汇总所有失败，日志落盘后再退出非零。
- **as-of 查询：** 某股票在 T 时刻无可见财务（新上市未出首份年报），返回空 DataFrame，因子 compute 方内决定用 NaN 填充还是跳过该股票。

## 测试策略

- **DataSource：** mock akshare，用固化 fixture 验证归一化逻辑 + schema 校验的正反例。
- **PIT 查询：** 单元测试构造"T=2022-03-01，应返回 2021Q3 财报而非 2021Q4"等关键场景。
- **因子：** 每个因子提供一个确定性小样本单测（硬编码输入+期望输出）。
- **评估器：** 用一个已知 IC 恒为正的合成因子做回归测试。

## 迁移步骤

1. 引入 `datasource/` 与 `factors/` 骨架，pandera + 接口。
2. 实现 akshare `get_ohlcv` / `get_fundamentals_pit` / `get_lhb`，写入新表。
3. 保留老作业跑老表不变；新建 `factor_compute_daily_job.py` 只读新表。
4. 实现四类因子第一期清单 + 评估器。
5. 跑稳一周，对老爬虫做一次随机采样对账。
6. 后续子项目（回测引擎）直接读新表与 Parquet 因子，不读老表。

## 依赖新增

- `akshare`（主数据源）
- `pandera`（schema 校验）
- `pyarrow`（Parquet 后端，pandas 已间接依赖可能已在）
- `jinja2`（评估报告模板；若未在）

## 交付物

- `instock/datasource/` 完整模块
- `instock/factors/` 完整模块
- `instock/job/factor_compute_daily_job.py`
- 新 MySQL 表 DDL 脚本
- 一份 HTML 评估报告样例（基于一个真实因子跑出来）
- 所有新代码的单元测试
