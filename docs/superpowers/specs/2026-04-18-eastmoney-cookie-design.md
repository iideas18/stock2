# 东方财富 Cookie 采集与更新脚本设计

**日期:** 2026-04-18
**状态:** Spec（待实现）
**项目:** InStock

## 背景

当前仓库已经具备东方财富 Cookie 的消费链路：

- `instock/core/eastmoney_fetcher.py` 优先从环境变量 `EAST_MONEY_COOKIE` 读取 Cookie
- 若环境变量未设置，则回退到 `instock/config/eastmoney_cookie.txt`
- 现有仓库缺少一个稳定、可复用、可校验的 Cookie 获取与更新工具

现状导致两个实际问题：

1. 用户只能手工打开浏览器开发者工具复制 Cookie，过程重复且容易出错
2. Cookie 写入和校验分散，没有统一的安全约束，存在写入无效 Cookie 或误提交敏感数据的风险

本设计的目标是为 InStock 增加一个本地可执行的浏览器自动化工具，用于打开东方财富网页、采集对当前抓取链路可用的 Eastmoney 会话 Cookie、校验可用性，并按用户选择写入文件或导出环境变量命令。

说明：登录东方财富账号仍然是推荐操作，因为通常能获得更稳定的 Cookie，但第一版脚本的成功标准不是“证明账号已登录”，而是“成功采集到对当前抓取链路可用的 Cookie 集合”。

## 目标

### 功能目标

1. 启动本地 Playwright 浏览器并打开东方财富行情页
2. 用户可选地手动完成登录
3. 自动采集对 `push2.eastmoney.com` 行情接口主链路可用的 Eastmoney Cookie
4. 将 Cookie 序列化为标准 HTTP `Cookie` 请求头字符串
5. 使用轻量东方财富 `push2` 接口进行有效性校验
6. 支持以下输出模式：
   - 写入 `instock/config/eastmoney_cookie.txt`
   - 输出 `export EAST_MONEY_COOKIE='...'`
   - 两者都做
7. 默认对敏感内容做脱敏输出，避免完整 Cookie 出现在普通日志中

### 非目标

1. 不自动填写账号密码
2. 不绕过验证码、设备校验或其他风控流程
3. 不实现长期驻留的自动刷新守护进程
4. 不直接修改用户当前 shell 的环境变量上下文
5. 不把 Cookie 上传到远端服务
6. 不改动现有抓取流程的 Cookie 读取优先级

## 方案选择

### 候选方案

#### 方案 A：依赖外部 `playwright-cli`

优点：实现快，开发期可以直接使用现有 IDE 工具。

缺点：依赖编辑器环境，不是仓库自带能力；其他开发者克隆仓库后未必具备同样工具。

#### 方案 B：仓库内置 Python + Playwright CLI 脚本

优点：能力内聚在仓库内；脚本可复用；接口、日志、校验和写入策略都可统一；最符合用户“获取或者更新 token”的目标。

缺点：需要新增 `playwright` 依赖，并要求首次安装浏览器运行时。

#### 方案 C：连接已开启调试端口的本地浏览器

优点：适合高级用户复用已有登录态。

缺点：配置复杂，平台差异大，不适合作为第一版默认路径。

### 结论

第一版采用方案 B，在仓库内新增 Python + Playwright 的 Cookie 采集与更新工具。后续若需要兼容 CDP 附着模式，可在同一模块中扩展。

## 用户流程

1. 用户执行命令，例如：

```bash
python instock/job/update_eastmoney_cookie.py --write file
python instock/job/update_eastmoney_cookie.py --write env
python instock/job/update_eastmoney_cookie.py --write both
```

2. 脚本创建一个全新的临时浏览器上下文并打开东方财富行情页面：

```text
https://quote.eastmoney.com/center/gridlist.html#hs_a_board
```

3. 用户在浏览器中手动登录东方财富账号
4. 脚本轮询浏览器 Cookie，直到检测到目标域 Cookie 且通过有效性校验，或超时退出
5. 脚本根据 `--write` 参数执行落盘或导出
6. 脚本输出脱敏结果、写入位置、后续建议

使用全新上下文而不是复用历史浏览器 profile 的原因是避免旧 Cookie 造成“未重新访问目标站点却误判成功”。第一版不提供复用已有浏览器数据目录的能力。

## CLI 设计

脚本位置：`instock/job/update_eastmoney_cookie.py`

参数设计：

- `--write {file,env,both}`
  - 默认值：`file`
  - 作用：选择写入文件、输出环境变量命令或两者都做
- `--browser {chromium,chrome,msedge}`
  - 默认值：`chromium`
  - 作用：选择浏览器通道
- `--timeout SECONDS`
  - 默认值：`300`
   - 作用：Cookie 获取窗口的最长时长，覆盖轮询、结构化预检查以及在截止时间前触发的最后一次校验
- `--show-cookie`
  - 默认关闭
   - 作用：仅在 `file` 模式下显式输出完整 Cookie
   - 约束：若与 `env` / `both` 组合使用，视为无效参数组合，返回退出码 `2`
### 输出通道契约

为解决敏感信息和自动化消费之间的冲突，脚本必须遵守以下输出规则：

1. 所有普通日志、提示、警告、脱敏摘要统一输出到 `stderr`
2. `stdout` 仅保留给机器可消费的敏感输出
3. 当 `--write env` 或 `--write both` 时，`stdout` 仅输出一行：

```bash
export EAST_MONEY_COOKIE='...'
```

4. 当 `--write file` 时，默认 `stdout` 为空；只有显式传入 `--show-cookie` 才允许输出完整 Cookie
5. 普通日志中永远不得回显完整 Cookie
6. `stderr` 中面向人的警告统一以 `WARNING:` 前缀开头，便于日志过滤和脚本识别

精确输出契约如下：

1. 任意非零退出码下，`stdout` 必须为空
2. `--write file`：
   - 默认 `stdout` 为空
   - 若传入 `--show-cookie`，则 `stdout` 仅输出完整 Cookie 字符串加一个换行符
   - 仅在文件写入成功后才允许输出
3. `--write env`：
   - `stdout` 仅输出一行 `export EAST_MONEY_COOKIE='...'` 加一个换行符
   - 仅在校验成功后才允许输出
4. `--write both`：
   - `stdout` 仅输出一行 `export EAST_MONEY_COOKIE='...'` 加一个换行符
   - 仅在文件写入成功后才允许输出

### 浏览器通道约束

第一版只保证 `chromium` 路径可用：

1. `chromium`：由 Playwright 管理浏览器运行时，作为默认且保证支持的路径
2. `chrome`：映射到本机 Chrome 通道；若本机未安装，脚本返回退出码 `2`
3. `msedge`：映射到本机 Edge 通道；若本机未安装，脚本返回退出码 `2`

实现时不单独做平台特定探测，而是直接以 Playwright 启动所选通道作为可用性检测：

1. `chromium`：调用 Playwright 默认浏览器启动逻辑
2. `chrome` / `msedge`：调用 Playwright 的 `channel` 启动参数
3. 若启动阶段抛出“executable doesn't exist”“channel not found”或等价错误，统一映射为退出码 `2`，并在 `stderr` 输出安装提示

### 环境变量优先级提示

脚本启动时必须检查当前进程环境中的 `EAST_MONEY_COOKIE`：

1. 使用 `os.environ.get("EAST_MONEY_COOKIE")` 读取；仅当值为非空字符串时才视为“已存在”
2. 若环境变量已存在且 `--write file`，脚本必须在 `stderr` 输出 `WARNING:`，说明运行时仍将优先使用环境变量而不是文件
3. 若环境变量已存在且 `--write both`，脚本必须在 `stderr` 输出 `WARNING:`，说明当前 shell / service 中仍然是环境变量优先
4. 若环境变量已存在且 `--write env`，脚本应在 `stderr` 输出 `WARNING:`，说明新值仅通过 `stdout` 导出，由用户自行 `source` 或复制执行

脚本第一版不自动修改当前 shell 环境，也不自动写入 `~/.bashrc` / `~/.zshrc`。

## 模块结构

### 新增模块

建议新增 `instock/core/eastmoney_cookie_manager.py`，职责如下：

1. `open_browser_and_wait_for_login(...)`
   - 启动 Playwright 浏览器
   - 打开东方财富页面
   - 轮询 Cookie 直到超时或成功

2. `collect_cookie_string(context) -> str`
   - 读取浏览器上下文内的 Cookie
   - 过滤 `eastmoney.com` 及其子域 Cookie
   - 转换为 `name=value; name2=value2` 格式

3. `validate_cookie(cookie: str, url: str) -> ValidationResult`
   - 使用 `requests` 或现有会话逻辑请求轻量接口
   - 返回成功、失败原因和必要诊断信息

4. `write_cookie_file(cookie: str, path: Path)`
   - 采用原子写入模式
   - 尽可能收紧文件权限到当前用户

5. `build_env_export(cookie: str) -> str`
   - 生成可直接复制执行的 `export EAST_MONEY_COOKIE='...'`

6. `mask_cookie(cookie: str) -> str`
   - 返回脱敏字符串，仅用于常规日志输出

### 归一化定义

本 spec 中所有“归一化后的 Cookie 字符串”都指同一个确定性结果，不允许不同模块各自定义：

1. 输入是已通过 Eastmoney 域筛选、过期过滤和目标主机匹配后的 Cookie 对象集合
2. 归一化过程只包含以下步骤：
   - 按本 spec 的主机优先级和同名冲突规则选择最终 Cookie 集
   - 对每个目标主机下的候选 Cookie，按 `(name ASC, normalized_domain ASC, path ASC, value ASC)` 做确定性排序
   - 以 `name=value` 形式序列化
   - 各对之间使用固定分隔符 `; ` 连接
3. 除上述确定性排序外，不做额外排序、不做大小写转换、不做空白裁剪、不做 URL 编码转换
4. 对文件读取结果的归一化，仅允许先剥离最后一个换行结尾（`\n`、`\r\n` 或单独 `\r`），其余字符必须保留，再参与字符串比较
5. 轮询变化检测、`unchanged` 比较和导出内容都必须基于这同一个归一化字符串

### Cookie 采集与归并规则

为保证实现一致性，采集逻辑需要遵循固定规则：

1. 采集范围覆盖整个 browser context，而不是单个 page，以支持登录弹窗、新标签页或 SSO 跳转
2. 仅接受 Eastmoney 站点族 Cookie，判定规则如下：
   - 先将 `cookie.domain` 规范化为 `normalized_domain = cookie.domain.lstrip('.')`
   - 当 `normalized_domain == 'eastmoney.com'` 或 `normalized_domain.endswith('.eastmoney.com')` 时，才允许保留
   - 因此 `eastmoney.com`、`.eastmoney.com`、`push2.eastmoney.com`、`.push2.eastmoney.com`、`api.push2.eastmoney.com` 都属于允许集合
   - 不额外要求必须带前导点；无前导点的 host-only Cookie 也允许参与后续匹配
3. 排除已过期 Cookie
4. 第一版的目标主机集合固定为东方财富 `push2` 主链路：

```text
push2.eastmoney.com
80.push2.eastmoney.com
82.push2.eastmoney.com
88.push2.eastmoney.com
```

5. 由于现有抓取链路只能消费一个静态 `Cookie` 请求头字符串，第一版采用“按目标主机集合做标准匹配后求并集”的策略：
   - 不依赖浏览器再次构造请求，而是在本地显式实现一份最小化匹配逻辑：`cookie_matches_target_host(cookie, scheme='https', host, request_path='/')`
   - 匹配规则固定为：
     - `normalized_domain = cookie.domain.lstrip('.')`
     - 若原始 `cookie.domain` 以 `.` 开头，则视为 domain cookie，当且仅当 `host == normalized_domain` 或 `host.endswith('.' + normalized_domain)` 时匹配
     - 若原始 `cookie.domain` 不以 `.` 开头，则视为 host-only cookie，仅当 `host == normalized_domain` 时匹配
     - `cookie.path` 为空时按 `/` 处理；仅当 `request_path` 以 `cookie.path` 为前缀时匹配
     - `secure=True` 的 Cookie 仅在 `scheme='https'` 时匹配；第一版校验请求固定为 HTTPS，因此允许参与
   - 在上述规则下，分别计算每个目标主机会携带的 Cookie 子集
   - 主机优先级固定为 `push2.eastmoney.com`、`80.push2.eastmoney.com`、`82.push2.eastmoney.com`、`88.push2.eastmoney.com`
   - 序列化时按主机优先级依次处理每个主机的已排序 Cookie 子集
   - 若某个 Cookie 名称尚未被更高优先级主机选中，则允许较低优先级主机为这个“新名称”提供值并进入最终结果
   - 若后续主机出现同名 Cookie，则保留更高优先级主机已经选中的值，不再覆盖
6. 最终序列化结果按首次选中顺序输出，保证稳定可测试
7. 若过滤后 Cookie 为空，则视为未访问成功或未采集成功

该规则的适用边界需要明确：第一版只承诺导出的静态 Cookie 头对 `push2` 行情接口主链路可用，不承诺覆盖东方财富所有页面或其他子域的浏览器语义。

此外，`st_si`、`st_psi`、`st_pvi`、`st_sp`、`st_asi` 等东方财富常见状态 Cookie 仅作为诊断增强项：

1. 若最终集合中包含至少一项，`stderr` 可输出简短提示，说明已观察到状态 Cookie
2. 若最终集合中不包含这些名称，不得仅凭此直接判定失败
3. 真正的成功与失败仍以远端 `push2` 校验结果为准，避免把匿名但可用的 Cookie 集合误判为无效

### 与现有代码的集成

`instock/core/eastmoney_fetcher.py` 保持现有读取优先级：

1. `EAST_MONEY_COOKIE` 环境变量
2. `instock/config/eastmoney_cookie.txt`
3. 默认兜底 Cookie

为避免未来出现两套规则不一致，需要将以下逻辑抽成共享能力：

- Cookie 文件路径
- Cookie 文件读取逻辑
- `update_cookie` 的持久化能力（必要时可改为调用共享工具）

采集脚本负责“获取、校验、写入”，抓取器继续负责“读取并发送请求”。

## 安全设计

### 敏感信息保护

1. 默认日志只显示脱敏 Cookie 摘要，不打印完整值
2. 仅在 `--show-cookie` 显式开启时输出完整 Cookie
3. 校验失败时不覆盖旧 Cookie 文件
4. 不将 Cookie 上报至外部服务
5. `build_env_export` 必须严格按以下格式生成：

```python
"export EAST_MONEY_COOKIE=" + shlex.quote(cookie) + "\n"
```

   - 不得额外再包一层单引号
   - 例如当 Cookie 为 `a=b'c` 时，输出必须等价于：

```bash
export EAST_MONEY_COOKIE='a=b'"'"'c'
```
6. 在落盘或导出前，若 Cookie 名称或值中出现换行、空字节或其他控制字符，必须拒绝并返回校验失败
7. 控制字符检查发生在候选 Cookie 完成归并之后、首次进入 `attempted_cookie_strings` 之前：
   - 若最终候选集合中任一 `name` 或 `value` 含控制字符，则整个候选字符串直接作废
   - 对这个作废候选不发起远端校验、不写文件、不生成导出命令
   - 脚本继续轮询，等待新的候选 Cookie 字符串；若直到 deadline 仍无新候选通过本地安全检查，则按退出码 `3` 结束

### Git 误提交防护

当前 `instock/config/eastmoney_cookie.txt` 未被显式忽略。实现时需更新：

- `instock/config/.gitignore`

新增：

```gitignore
eastmoney_cookie.txt
```

这样可以降低误提交真实 Cookie 的风险。

## 校验策略

Cookie 采集成功后必须执行一次轻量接口校验。第一版使用固定默认接口：

```text
GET https://push2.eastmoney.com/api/qt/clist/get
```

默认参数固定为：

```text
pn=1
pz=1
po=1
np=1
fltt=2
invt=2
fid=f12
fs=m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048
fields=f12,f14,f2
ut=bd1d9ddb04089700cf9c27f6f7426281
```

请求头要求：

1. `User-Agent` 与现有 `eastmoney_fetcher` 保持一致风格
2. `Referer` 固定为 `https://quote.eastmoney.com/`
3. `Cookie` 使用刚刚采集出的序列化结果

### 轮询与重校验节奏

1. `--timeout` 定义为获取窗口 deadline：从 `page.goto(target_url, wait_until='domcontentloaded')` 成功返回的那一刻开始计时，到 deadline 为止允许持续轮询与发现新候选 Cookie
2. 若在 deadline 之前检测到一个新的候选 Cookie 字符串，则允许该候选触发一次完整校验，即使这一次校验在 deadline 之后结束
3. 浏览器 Cookie 轮询间隔固定为 2 秒
4. 变更检测使用“归一化后的完整 Cookie 字符串”做精确字符串比较
5. 维护 `attempted_cookie_strings` 集合；同一运行过程中，每个不同的归一化 Cookie 字符串最多只允许触发一次远端校验，避免字符串来回抖动导致重复请求
6. 只有当归一化后的 Cookie 字符串非空、通过本地字符安全检查且不在 `attempted_cookie_strings` 中时，才允许触发新的接口校验
7. 两次接口校验的间隔采用全局 start-to-start 语义：任意两次校验开始时间之间至少间隔 3 秒，避免高频触发限流
8. 这 3 秒冷却属于整体运行时间的一部分；实现时维护一个全局 `next_validation_not_before` 时间戳
9. 若某个新候选 Cookie 在 deadline 前被首次观察到，但当时 `now < next_validation_not_before`，则将它登记为唯一的 `pending_candidate`
10. `pending_candidate` 始终保存“最近一次在 deadline 前首次观察到、且尚未尝试过”的候选字符串；当冷却结束后，只对这个最新的 `pending_candidate` 发起一次最终校验
11. 因此同一时刻最多只有一个排队中的候选校验；不存在多候选并发或无限堆积
12. 主浏览页面打开后，脚本在每个轮询 tick 前先检查 `page.is_closed()` 与 `browser.is_connected()`；若任一结果表明页面或浏览器已被用户关闭，或后续 `context.cookies()` 调用抛出“context/browser closed”类异常，则按退出码 `3` 退出并清理资源

重试与超时策略：

1. 单次请求超时 8 秒
2. 最多重试 3 次
3. 固定退避节奏为 0.5 秒、1 秒、2 秒
4. `429/500/502/503/504` 视为瞬时失败，允许重试
5. 第一版忽略 `Retry-After`，统一使用固定退避节奏，保持实现简单且可测试
6. `401/403`、反爬 HTML、JSON 解析失败、字段结构不满足契约，统一视为该候选 Cookie 的非重试失败
7. 某个候选 Cookie 非重试失败后，脚本继续轮询，等待新的、未尝试过的候选 Cookie；若直到 deadline 都没有新的候选通过校验，则整体按退出码 `4` 结束

校验成功的判定条件：

1. 返回 HTTP 200
2. 响应体可解析为 JSON
3. 顶层存在 `data`
4. `data.total` 为正整数
5. `data.diff` 为非空列表
6. 第一条记录必须满足：
   - `f12` 键存在，且值不为 `None`、`""`
   - `f14` 键存在，且值不为 `None`、`""`
   - `f2` 键存在，且值不为 `None`、`""`
   - 对 `f2`，数值 `0` 视为合法非空值，不因为 falsy 而失败
   - 任一字段缺键、值为 `None` 或空字符串 `""` 都算失败
7. 本地已采集到的 Cookie 集合满足 Eastmoney 目标域筛选、字符安全检查和序列化规则

需要明确的是：该校验用于确认采集结果足以支撑当前抓取链路下的稳定访问与反爬通过，不用于证明 Cookie 对“登录账号身份”有强绑定鉴权能力，因为该接口本身可能允许匿名访问。换言之，第一版的“成功”定义是“可用于当前抓取链路”，而不是“账号已登录”。

失败时输出明确错误原因：

- 未检测到目标域 Cookie
- 检测到的 Cookie 不包含关键状态项
- 已检测到 Cookie，但接口校验失败
- 接口返回 `403` / `429` / 反爬 HTML / 空 JSON
- 登录等待超时
- 浏览器启动失败或 Playwright 未安装

## 错误处理

### 主要错误场景

1. Playwright 未安装或浏览器运行时缺失
   - 提示执行相应安装命令
2. 用户在超时时间内未完成访问或未形成有效 Cookie
   - 返回超时错误，不写入
3. 浏览器中存在 Cookie，但不属于目标域
   - 提示用户确认是否已经成功访问东方财富主站或完成登录
4. 校验接口异常
   - 返回网络错误或响应解析错误，不覆盖旧值
5. 文件写入失败
   - 提示权限或路径问题
6. 用户手动关闭浏览器或目标标签页
   - 视为采集未完成，返回退出码 `3`
7. 候选 Cookie 完成本地收集，但所有已尝试候选都未通过远端 `push2` 校验
   - 返回退出码 `4`

### 退出码约定

为便于脚本化调用，CLI 需要提供稳定退出码：

- `0`：成功
- `2`：参数错误或运行环境缺失（例如 Playwright 未安装）
- `3`：获取窗口内未等到可发起校验的候选 Cookie，或用户主动关闭浏览器 / 页面
- `4`：至少有一个候选 Cookie 被成功采集并发起过远端校验，但在允许时间内全部校验失败
- `5`：写文件失败
- `6`：未知内部异常

其中，权限收紧（例如 `chmod 600`）属于尽力而为操作：

1. 原子写入失败是致命错误，返回 `5`
2. 文件已成功写入，但权限收紧失败时，只输出 `stderr` 警告，不改变成功退出码
3. 第一版原子写入采用同目录临时文件 + `flush` + `fsync` + `os.replace()` 模式；不额外做跨进程锁，语义为 last-writer-wins
4. 写入完成后，在 POSIX 平台上尽力执行一次 `os.chmod(path, 0o600)`；若抛出 `OSError`，只输出 `WARNING:`，不再额外校验文件 mode

## 依赖与文档变更

### 依赖

在 `requirements.txt` 中新增：

```text
playwright==<版本待实现时确定>
```

### 文档

在 `README.md` 新增一节说明：

1. 首次安装 Playwright 依赖
2. 首次安装浏览器运行时
3. 如何运行 Cookie 更新脚本
4. `--write` 参数的三种模式
5. Cookie 过期后的建议更新方式
6. `chromium` 是默认和保证支持的浏览器通道，`chrome` / `msedge` 依赖本机安装
7. `stdout` / `stderr` 的输出契约，便于脚本化使用

## 测试策略

不对真实登录流程做自动化测试。自动化测试只覆盖可稳定验证的纯逻辑部分。

### 单元测试范围

1. Cookie 列表转请求头字符串
2. `mask_cookie` 的脱敏逻辑
3. 环境变量导出命令生成
4. 原子写文件与覆盖行为
5. 校验函数对成功 / 失败响应的处理
6. 参数解析行为
7. 文件内容归一化规则：单行 UTF-8、可选尾随换行剥离

### Mock 集成测试范围

需要增加基于 mock 的流程测试，覆盖高风险主路径：

1. `poll -> validate -> write file` 成功
2. `poll -> validate -> stdout export` 成功
3. `poll -> validate -> both` 成功，且 `stdout` 仅包含导出命令
4. 超时退出且不覆盖旧文件
5. 校验失败时保留旧文件
6. 当前环境已存在 `EAST_MONEY_COOKIE` 时的 `stderr` 警告输出
7. `stdout` / `stderr` 分流符合约定
8. 新旧 Cookie 完全相同场景下返回成功并提示 `unchanged`
9. 用户主动关闭浏览器或页面时返回退出码 `3`

### unchanged 语义

`unchanged` 必须按输出目标分别计算，而不是只看文件：

1. 文件目标：若 `instock/config/eastmoney_cookie.txt` 存在，且其归一化内容与新采集 Cookie 完全一致，则视为文件侧 `unchanged`
2. 环境变量目标：若当前进程中的 `EAST_MONEY_COOKIE` 为非空，且其值与新采集 Cookie 完全一致，则视为环境变量侧 `unchanged`
3. 若目标文件不存在，则文件侧永远不算 `unchanged`，首次运行必须实际写入

当新采集到的归一化 Cookie 与现有目标内容完全一致时：

1. `--write file`：若文件侧 `unchanged`，则不重写文件、不变更 mtime，`stderr` 输出 `unchanged`
2. `--write env`：即使环境变量侧 `unchanged`，仍然输出 `export EAST_MONEY_COOKIE=...`，因为用户显式请求了环境变量导出产物；同时 `stderr` 可输出 `unchanged`
3. `--write both`：文件侧若 `unchanged` 则不重写文件，同时仍然输出 `export EAST_MONEY_COOKIE=...`；若环境变量侧也 `unchanged`，允许在 `stderr` 一并说明
4. `unchanged` 场景始终返回退出码 `0`

### 不测试范围

1. 真实东方财富账号登录
2. 验证码流程
3. 线上接口长期稳定性

## 验收标准

满足以下条件视为完成：

1. 用户可以通过一个 CLI 命令打开浏览器并开始访问东方财富流程
2. 脚本能自动采集出对 `push2` 行情接口主链路可用的 Cookie
3. 脚本会在写入前先执行有效性校验
4. `--write file` 能写入 `instock/config/eastmoney_cookie.txt`
5. `--write env` 能输出可直接复制执行的 `export EAST_MONEY_COOKIE='...'`
6. `--write both` 同时完成两种输出
7. 默认日志不会泄露完整 Cookie
8. `instock/config/eastmoney_cookie.txt` 被 Git 忽略
9. 成功标准明确为“当前抓取链路可用”，而不是“证明账号已登录”

## 文件格式契约

`instock/config/eastmoney_cookie.txt` 的文件格式必须固定为：

1. UTF-8 编码
2. 单行原始 Cookie 头字符串
3. 允许文件末尾存在一个换行符，但读取时只移除末尾 `\r` / `\n`，不额外裁剪其他字符
4. 读取归一化时仅允许剥离尾部 `\n`、`\r\n` 或单独 `\r`；前导空格、内部空格和其他可打印字符必须原样保留
5. 若末尾换行前存在空格或制表符，这些字符视为 Cookie 内容本身的一部分，不得自动删除
6. 不允许写入 JSON、YAML 或额外元数据

文件读取归一化算法必须固定为如下逻辑：

```python
raw = path.read_text(encoding="utf-8")
if raw.endswith("\r\n"):
   normalized = raw[:-2]
elif raw.endswith("\n") or raw.endswith("\r"):
   normalized = raw[:-1]
else:
   normalized = raw
```

说明：

1. 只剥离最末尾一个换行结尾，不循环剥离
2. 文件中间若存在混合换行，视为非法内容，但保留给上层校验或测试处理，不在归一化阶段自动修复

Cookie 文件路径、读写、归一化逻辑需要放入同一个共享 helper，避免与现有 `eastmoney_fetcher` 读取规则产生偏差。

## 后续扩展

以下能力不属于本次实现，但设计上应保留扩展空间：

1. 通过 CDP 附着到已有 Chrome / Edge 会话
2. 将 Cookie 元数据（更新时间、校验时间）单独记录到本地状态文件
3. 为 Web 管理界面提供一个“更新 Cookie”入口
4. 接入更多东方财富接口做更严格的可用性检查