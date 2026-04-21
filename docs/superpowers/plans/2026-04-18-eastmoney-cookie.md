# Eastmoney Cookie Automation Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 InStock 仓库内落地一个基于 Playwright 的东方财富 Cookie 自动采集与更新工具，支持校验、文件写入、环境变量导出，以及现有抓取链路复用。

**Architecture:** 把能力拆成两个核心单元：`instock/core/eastmoney_cookie_store.py` 负责共享 Cookie 路径、归一化、脱敏、文件读写、导出字符串；`instock/core/eastmoney_cookie_manager.py` 负责浏览器采集、目标主机匹配、候选轮询与 `push2` 校验。CLI 入口 `instock/job/update_eastmoney_cookie.py` 只处理参数解析、stdout/stderr 契约和写入目标编排；`instock/core/eastmoney_fetcher.py` 只做最小共享接入，不改现有优先级。

**Tech Stack:** Python 3.11, requests, Playwright, pytest, monkeypatch, tempfile, pathlib。

**Spec:** `docs/superpowers/specs/2026-04-18-eastmoney-cookie-design.md`

---

## 文件结构

```text
instock/
├─ core/
│  ├─ eastmoney_fetcher.py              # 继续消费 Cookie；改为调用共享 helper 读文件/归一化
│  ├─ eastmoney_cookie_store.py         # 新增：路径、归一化、脱敏、文件读写、export 生成
│  └─ eastmoney_cookie_manager.py       # 新增：域过滤、主机匹配、轮询、push2 校验、浏览器 orchestration
├─ job/
│  └─ update_eastmoney_cookie.py        # 新增 CLI：argparse + stdout/stderr 契约 + 写入编排

instock/config/
└─ .gitignore                           # 新增 eastmoney_cookie.txt 忽略规则

tests/
├─ core/
│  ├─ __init__.py
│  ├─ test_eastmoney_cookie_store.py    # 归一化、export、原子写入、unchanged
│  ├─ test_eastmoney_cookie_manager.py  # 主机匹配、候选合并、校验、轮询状态机
│  └─ test_eastmoney_fetcher.py         # 环境变量优先级与共享 helper 回归测试
└─ job/
   ├─ __init__.py
   └─ test_update_eastmoney_cookie.py   # CLI 参数、stdout/stderr、退出码
```

设计约束：

- `eastmoney_cookie_store.py` 只放纯格式/IO 逻辑，不直接依赖 Playwright。
- `eastmoney_cookie_manager.py` 只放采集与校验逻辑，不直接 print，不直接写 README，不直接改当前 shell 环境。
- CLI 只处理用户交互契约，不自行实现 Cookie 匹配或原子写入。
- 任何真实浏览器行为都通过依赖注入包装，测试里只用 fake browser/page/context，不访问真实东方财富站点。

## Chunk 1: Shared Cookie Storage And Existing Fetcher Integration

### Task 1: Add Playwright dependency and test package scaffolding

**Files:**
- Modify: `requirements.txt`
- Create: `tests/core/__init__.py`
- Create: `tests/job/__init__.py`

- [ ] **Step 1: Pin Playwright in `requirements.txt`**

在 `requirements.txt` 末尾追加：

```text
playwright==1.52.0
```

- [ ] **Step 2: Install Python dependency**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pip install -r requirements.txt`
Expected: `playwright` 安装成功，无 resolver 冲突。

- [ ] **Step 3: Install Chromium runtime for local development**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && python -m playwright install chromium`
Expected: Chromium runtime 下载完成；后续 `chromium` 通道可直接启动。

- [ ] **Step 4: Create empty test packages**

Create `tests/core/__init__.py` and `tests/job/__init__.py` as empty files.

- [ ] **Step 5: Verify pytest discovery sees the new packages**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest --collect-only tests/core tests/job -q`
Expected: 命令成功退出；当前可能显示 `no tests collected`，但不得有 import error。

- [ ] **Step 6: Commit scaffold**

```bash
git add requirements.txt tests/core/__init__.py tests/job/__init__.py
git commit -m "chore: add playwright dependency and cookie test packages"
```

### Task 2: Build the shared Cookie store helper with file-safe behavior

**Files:**
- Create: `instock/core/eastmoney_cookie_store.py`
- Create: `tests/core/test_eastmoney_cookie_store.py`

- [ ] **Step 1: Write the failing store helper tests**

Create `tests/core/test_eastmoney_cookie_store.py`:

```python
import pytest

from instock.core import eastmoney_cookie_store as store


def test_normalize_file_text_strips_only_one_line_ending():
    assert store.normalize_file_text("a=b\r\n") == "a=b"
    assert store.normalize_file_text("a=b\n") == "a=b"
    assert store.normalize_file_text("a=b\r") == "a=b"
    assert store.normalize_file_text("a=b \n") == "a=b "
    assert store.normalize_file_text("a=b\n\n") == "a=b\n"


def test_build_env_export_uses_shell_safe_quoting():
    export_line = store.build_env_export("a=b'c")
    assert export_line.startswith("export EAST_MONEY_COOKIE=")
    assert export_line.endswith("\n")
    assert '"\'"\'' in export_line


def test_read_cookie_file_returns_none_when_missing(tmp_path):
    assert store.read_cookie_file(tmp_path / "missing.txt") is None


def test_mask_cookie_redacts_middle_content():
    assert store.mask_cookie("abcdefgh12345678") == "abcdefgh...len=16"


def test_write_cookie_file_returns_changed_false_when_content_is_unchanged(tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"
    path.write_text("foo=bar\n", encoding="utf-8")

    result = store.write_cookie_file("foo=bar", path)

    assert result.changed is False
    assert path.read_text(encoding="utf-8") == "foo=bar\n"


def test_write_cookie_file_rejects_control_characters(tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"
    with pytest.raises(ValueError):
        store.write_cookie_file("foo=bar\nboom", path)


def test_write_cookie_file_returns_warning_when_chmod_fails(monkeypatch, tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"

    def raise_chmod(*_args, **_kwargs):
        raise OSError("no chmod")

    monkeypatch.setattr(store.os, "chmod", raise_chmod)
    result = store.write_cookie_file("foo=bar", path)

    assert result.changed is True
    assert result.warning == "chmod-failed"


def test_write_cookie_file_cleans_up_temp_file_when_replace_fails(monkeypatch, tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"
    created = {}
    original_replace = store.os.replace

    def tracking_replace(src, dst):
        created["tmp"] = src
        raise OSError("replace failed")

    monkeypatch.setattr(store.os, "replace", tracking_replace)

    with pytest.raises(OSError):
        store.write_cookie_file("foo=bar", path)

    assert created["tmp"].exists() is False


def test_write_cookie_file_propagates_mkdir_failure(monkeypatch, tmp_path):
    path = tmp_path / "nested" / "eastmoney_cookie.txt"

    def raise_mkdir(*_args, **_kwargs):
        raise OSError("mkdir failed")

    monkeypatch.setattr(store.Path, "mkdir", raise_mkdir)

    with pytest.raises(OSError):
        store.write_cookie_file("foo=bar", path)
```

- [ ] **Step 2: Run the store helper tests to verify failure**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_store.py -v`
Expected: FAIL with `ImportError` for `instock.core.eastmoney_cookie_store`.

- [ ] **Step 3: Implement the minimal shared helper**

Create `instock/core/eastmoney_cookie_store.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import shlex
import tempfile


COOKIE_FILE = Path(__file__).resolve().parent.parent / "config" / "eastmoney_cookie.txt"


@dataclass(frozen=True)
class CookieWriteResult:
    path: Path
    changed: bool
    warning: str | None = None


def normalize_file_text(raw: str) -> str:
    if raw.endswith("\r\n"):
        return raw[:-2]
    if raw.endswith("\n") or raw.endswith("\r"):
        return raw[:-1]
    return raw


def reject_control_characters(cookie: str) -> None:
    if any(ord(ch) < 32 for ch in cookie):
        raise ValueError("Cookie contains control characters")


def read_cookie_file(path: Path = COOKIE_FILE) -> str | None:
    if not path.exists():
        return None
    return normalize_file_text(path.read_text(encoding="utf-8"))


def build_env_export(cookie: str) -> str:
    reject_control_characters(cookie)
    return "export EAST_MONEY_COOKIE=" + shlex.quote(cookie) + "\n"


def mask_cookie(cookie: str) -> str:
    if len(cookie) <= 12:
        return "***"
    return f"{cookie[:8]}...len={len(cookie)}"


def write_cookie_file(cookie: str, path: Path = COOKIE_FILE) -> CookieWriteResult:
    reject_control_characters(cookie)
    existing = read_cookie_file(path)
    if existing == cookie:
        return CookieWriteResult(path=path, changed=False, warning=None)

    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        tmp.write(cookie + "\n")
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)

    try:
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    warning = None
    try:
        os.chmod(path, 0o600)
    except OSError:
        warning = "chmod-failed"
    return CookieWriteResult(path=path, changed=True, warning=warning)
```

这一层的 helper 契约要写死，避免后面 CLI 和 fetcher 各自猜：

- `COOKIE_FILE` 固定指向 `Path(__file__).resolve().parent.parent / "config" / "eastmoney_cookie.txt"`
- `reject_control_characters()` 直接抛出 `ValueError`，调用方据此拒绝候选或终止写入
- `fsync()` / `os.replace()` 失败时保持异常向上传播；这类错误由 CLI 映射为写文件失败退出码
- `os.replace()` 失败前必须删除同目录临时文件，避免遗留脏文件堆积
- `chmod()` 失败不是致命错误，通过 `CookieWriteResult.warning` 上报给上层，由上层决定是否打印 `WARNING:`

- [ ] **Step 4: Re-run the store helper tests**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_store.py -v`
Expected: PASS，至少 4 个测试通过。

- [ ] **Step 5: Commit the shared helper**

```bash
git add instock/core/eastmoney_cookie_store.py tests/core/test_eastmoney_cookie_store.py
git commit -m "feat(cookie): add shared eastmoney cookie store helper"
```

### Task 3: Refactor `eastmoney_fetcher` to reuse the shared helper without changing runtime precedence

**Files:**
- Modify: `instock/core/eastmoney_fetcher.py`
- Create: `tests/core/test_eastmoney_fetcher.py`

- [ ] **Step 1: Write failing regression tests for fetcher precedence**

Create `tests/core/test_eastmoney_fetcher.py`:

```python
from pathlib import Path

from instock.core import eastmoney_cookie_store as store
from instock.core.eastmoney_fetcher import eastmoney_fetcher


def test_fetcher_prefers_env_cookie(monkeypatch, tmp_path):
    cookie_path = tmp_path / "eastmoney_cookie.txt"
    cookie_path.write_text("file=1\n", encoding="utf-8")
    monkeypatch.setattr(store, "COOKIE_FILE", cookie_path)
    monkeypatch.setenv("EAST_MONEY_COOKIE", "env=1")

    fetcher = eastmoney_fetcher()

    assert fetcher.session.headers["Cookie"] == "env=1"


def test_fetcher_uses_file_cookie_when_env_missing(monkeypatch, tmp_path):
    cookie_path = tmp_path / "eastmoney_cookie.txt"
    cookie_path.write_text("file=1\n", encoding="utf-8")
    monkeypatch.setattr(store, "COOKIE_FILE", cookie_path)
    monkeypatch.delenv("EAST_MONEY_COOKIE", raising=False)

    fetcher = eastmoney_fetcher()

    assert fetcher.session.headers["Cookie"] == "file=1"
```

- [ ] **Step 2: Run the regression tests and confirm current failure or brittleness**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_fetcher.py -v`
Expected: 至少 1 个测试失败，因为 fetcher 仍然自己读固定路径且 `.strip()` 行为未共享。

- [ ] **Step 3: Refactor `instock/core/eastmoney_fetcher.py` to use the store helper**

In the existing `eastmoney_fetcher` class, replace the current `_get_cookie(self)` method body in place; do not create a second helper with a similar name, and do not duplicate the default-cookie literal in multiple places.

Before, the class contains a `_get_cookie()` method that:

- reads `EAST_MONEY_COOKIE` directly from `os.environ`
- opens `instock/config/eastmoney_cookie.txt` directly with `open(...).read().strip()`
- falls back to the hard-coded default Cookie string

After the refactor, that same `_get_cookie(self)` method should delegate to the new shared helper:

```python
from instock.core import eastmoney_cookie_store as cookie_store


DEFAULT_COOKIE = "st_si=..."


def _get_cookie(self):
    env_cookie = os.environ.get("EAST_MONEY_COOKIE")
    if env_cookie:
        return env_cookie

    file_cookie = cookie_store.read_cookie_file(cookie_store.COOKIE_FILE)
    if file_cookie:
        return file_cookie

    return DEFAULT_COOKIE
```

不要改 `_send_request()`、fallback hosts、刷新策略。这个任务只做共享读取逻辑，不扩大 fetcher 责任。

- [ ] **Step 4: Re-run the fetcher and store tests**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_store.py tests/core/test_eastmoney_fetcher.py -v`
Expected: PASS。

- [ ] **Step 5: Commit the refactor**

```bash
git add instock/core/eastmoney_fetcher.py tests/core/test_eastmoney_fetcher.py
git commit -m "refactor(fetcher): reuse shared eastmoney cookie helper"
```

## Chunk 2: Browser Cookie Acquisition And Validation Engine

### Task 4: Add pure cookie matching, merge, and validation helpers behind unit tests

**Files:**
- Create: `instock/core/eastmoney_cookie_manager.py`
- Create: `tests/core/test_eastmoney_cookie_manager.py`

- [ ] **Step 1: Write failing tests for pure matching and merge behavior**

Create `tests/core/test_eastmoney_cookie_manager.py` with an initial pure-function block:

```python
from instock.core import eastmoney_cookie_manager as manager


def test_cookie_matches_domain_cookie_for_subdomain():
    cookie = {"name": "st_si", "value": "1", "domain": ".eastmoney.com", "path": "/", "secure": True}
    assert manager.cookie_matches_target_host(cookie, "https", "push2.eastmoney.com", "/") is True


def test_cookie_matches_host_only_cookie_only_for_exact_host():
    cookie = {"name": "foo", "value": "1", "domain": "push2.eastmoney.com", "path": "/", "secure": False}
    assert manager.cookie_matches_target_host(cookie, "https", "push2.eastmoney.com", "/") is True
    assert manager.cookie_matches_target_host(cookie, "https", "80.push2.eastmoney.com", "/") is False


def test_collect_cookie_string_is_deterministic_across_host_priority():
    cookies = [
        {"name": "b", "value": "2", "domain": ".eastmoney.com", "path": "/", "secure": True},
        {"name": "a", "value": "1", "domain": "push2.eastmoney.com", "path": "/", "secure": True},
    ]
    assert manager.collect_cookie_string(cookies) == "a=1; b=2"


def test_validate_push2_payload_accepts_zero_price_but_not_empty_strings():
    payload = {"data": {"total": 1, "diff": [{"f12": "600519", "f14": "贵州茅台", "f2": 0}]}}
    assert manager.validate_push2_payload(payload).ok is True

    payload["data"]["diff"][0]["f14"] = ""
    assert manager.validate_push2_payload(payload).ok is False
```

- [ ] **Step 2: Run the pure manager tests to verify failure**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_manager.py -v`
Expected: FAIL with `ImportError` or missing symbol errors.

- [ ] **Step 3: Implement the pure functions and result dataclasses**

Create `instock/core/eastmoney_cookie_manager.py` with these minimum pieces first:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


TARGET_HOSTS = (
    "push2.eastmoney.com",
    "80.push2.eastmoney.com",
    "82.push2.eastmoney.com",
    "88.push2.eastmoney.com",
)


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    reason: str


def is_eastmoney_cookie(cookie: dict) -> bool:
    normalized_domain = cookie["domain"].lstrip(".")
    return normalized_domain == "eastmoney.com" or normalized_domain.endswith(".eastmoney.com")


def cookie_matches_target_host(cookie: dict, scheme: str, host: str, request_path: str = "/") -> bool:
    normalized_domain = cookie["domain"].lstrip(".")
    is_domain_cookie = cookie["domain"].startswith(".")
    path = cookie.get("path") or "/"
    if cookie.get("secure") and scheme != "https":
        return False
    if not request_path.startswith(path):
        return False
    if is_domain_cookie:
        return host == normalized_domain or host.endswith("." + normalized_domain)
    return host == normalized_domain


def collect_cookie_string(cookies: Iterable[dict]) -> str:
    selected: dict[str, str] = {}
    ordered_pairs: list[tuple[str, str]] = []
    eastmoney_cookies = [cookie for cookie in cookies if is_eastmoney_cookie(cookie)]
    for host in TARGET_HOSTS:
        matched = [
            cookie for cookie in eastmoney_cookies
            if cookie_matches_target_host(cookie, "https", host, "/")
        ]
        matched.sort(key=lambda item: (item["name"], item["domain"].lstrip("."), item.get("path") or "/", item["value"]))
        for cookie in matched:
            if cookie["name"] in selected:
                continue
            selected[cookie["name"]] = cookie["value"]
            ordered_pairs.append((cookie["name"], cookie["value"]))
    return "; ".join(f"{name}={value}" for name, value in ordered_pairs)


def validate_push2_payload(payload: dict) -> ValidationResult:
    try:
        record = payload["data"]["diff"][0]
        total = payload["data"]["total"]
    except (KeyError, IndexError, TypeError):
        return ValidationResult(False, "malformed-json")
    if not isinstance(total, int) or total <= 0:
        return ValidationResult(False, "empty-total")
    for field in ("f12", "f14", "f2"):
        if field not in record:
            return ValidationResult(False, f"missing-{field}")
        if record[field] is None or record[field] == "":
            return ValidationResult(False, f"empty-{field}")
    return ValidationResult(True, "ok")
```

- [ ] **Step 4: Re-run the pure manager tests**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_manager.py -v`
Expected: 当前纯函数测试全部通过。

- [ ] **Step 5: Commit the pure manager helpers**

```bash
git add instock/core/eastmoney_cookie_manager.py tests/core/test_eastmoney_cookie_manager.py
git commit -m "feat(cookie): add eastmoney cookie matching and payload validation helpers"
```

### Task 5: Add network validation and browser polling orchestration with fake browser tests

**Files:**
- Modify: `instock/core/eastmoney_cookie_manager.py`
- Modify: `tests/core/test_eastmoney_cookie_manager.py`

- [ ] **Step 1: Extend the manager tests with orchestration scenarios**

Append these tests to `tests/core/test_eastmoney_cookie_manager.py`:

```python
class FakePage:
    def __init__(self):
        self._closed = False
    def is_closed(self):
        return self._closed


class FakeBrowser:
    def __init__(self):
        self._connected = True
    def is_connected(self):
        return self._connected


class FakeContext:
    def __init__(self, snapshots):
        self._snapshots = list(snapshots)
        self._idx = 0
    def cookies(self):
        snapshot = self._snapshots[min(self._idx, len(self._snapshots) - 1)]
        self._idx += 1
        return snapshot


def test_acquire_cookie_validates_each_unique_candidate_once(monkeypatch):
    calls = []

    def fake_validate(cookie_string, *_args, **_kwargs):
        calls.append(cookie_string)
        return manager.ValidationResult(ok=cookie_string == "st_si=2", reason="ok" if cookie_string == "st_si=2" else "bad")

    result = manager.acquire_cookie_from_context(
        context=FakeContext([
            [{"name": "st_si", "value": "1", "domain": ".eastmoney.com", "path": "/", "secure": True}],
            [{"name": "st_si", "value": "1", "domain": ".eastmoney.com", "path": "/", "secure": True}],
            [{"name": "st_si", "value": "2", "domain": ".eastmoney.com", "path": "/", "secure": True}],
        ]),
        page=FakePage(),
        browser=FakeBrowser(),
        deadline=6.0,
        now_fn=manager.make_fake_clock([0.0, 0.0, 2.0, 3.1, 4.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=fake_validate,
    )

    assert result.exit_code == 0
    assert calls == ["st_si=1", "st_si=2"]


def test_acquire_cookie_returns_exit_code_3_when_browser_is_closed():
    page = FakePage()
    page._closed = True

    result = manager.acquire_cookie_from_context(
        context=FakeContext([[]]),
        page=page,
        browser=FakeBrowser(),
        timeout_seconds=3,
        now_fn=manager.make_fake_clock([0.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=lambda *_args, **_kwargs: manager.ValidationResult(ok=False, reason="should-not-run"),
    )

    assert result.exit_code == 3


def test_validate_cookie_retries_transient_statuses(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}
        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = 0
        def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return FakeResponse(502)
            if self.calls == 2:
                return FakeResponse(429)
            return FakeResponse(200, {"data": {"total": 1, "diff": [{"f12": "600519", "f14": "贵州茅台", "f2": 0}]}})

    sleeps = []
    result = manager.validate_cookie("st_si=1", session=FakeSession(), sleep_fn=sleeps.append)

    assert result.ok is True
    assert sleeps == [0.5, 1.0]


def test_acquire_cookie_skips_control_character_candidate_and_keeps_polling():
    calls = []

    def fake_validate(cookie_string, *_args, **_kwargs):
        calls.append(cookie_string)
        return manager.ValidationResult(ok=True, reason="ok")

    result = manager.acquire_cookie_from_context(
        context=FakeContext([
            [{"name": "st_si", "value": "bad\nvalue", "domain": ".eastmoney.com", "path": "/", "secure": True}],
            [{"name": "st_si", "value": "good", "domain": ".eastmoney.com", "path": "/", "secure": True}],
        ]),
        page=FakePage(),
        browser=FakeBrowser(),
        deadline=6.0,
        now_fn=manager.make_fake_clock([0.0, 2.0, 4.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=fake_validate,
    )

    assert result.exit_code == 0
    assert calls == ["st_si=good"]


def test_acquire_cookie_maps_closed_context_exception_to_exit_code_3():
    class RaisingContext:
        def cookies(self):
            raise RuntimeError("browser has been closed")

    result = manager.acquire_cookie_from_context(
        context=RaisingContext(),
        page=FakePage(),
        browser=FakeBrowser(),
        deadline=3.0,
        now_fn=manager.make_fake_clock([0.0]),
        sleep_fn=lambda _seconds: None,
        validate_fn=lambda *_args, **_kwargs: manager.ValidationResult(ok=False, reason="should-not-run"),
    )

    assert result.exit_code == 3


def test_acquire_cookie_via_browser_starts_deadline_after_goto(monkeypatch):
    events = []
    captured = {}

    class FakePageWithGoto(FakePage):
        def goto(self, url, wait_until):
            events.append(("goto", url, wait_until))

    class FakeContextWithPage(FakeContext):
        def new_page(self):
            return FakePageWithGoto()

    class FakeBrowserWithContext(FakeBrowser):
        def new_context(self):
            return FakeContextWithPage([[{"name": "st_si", "value": "ok", "domain": ".eastmoney.com", "path": "/", "secure": True}]])
        def close(self):
            events.append(("browser-close",))

    class FakeBrowserType:
        def launch(self, **_kwargs):
            return FakeBrowserWithContext()

    class FakePlaywright:
        chromium = FakeBrowserType()
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(manager, "sync_playwright", lambda: FakePlaywright())
    monkeypatch.setattr(
        manager,
        "acquire_cookie_from_context",
        lambda **kwargs: captured.update(kwargs) or manager.AcquisitionResult(exit_code=0, cookie="st_si=ok", validation_reason="ok"),
    )

    result = manager.acquire_cookie_via_browser("chromium", 300, now_fn=manager.make_fake_clock([10.0]))

    assert result.exit_code == 0
    assert events[0][0] == "goto"
    assert captured["deadline"] == 310.0
```

- [ ] **Step 2: Run the extended manager tests and confirm failure**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_manager.py -v`
Expected: FAIL on missing orchestration symbols such as `acquire_cookie_from_context`.

- [ ] **Step 3: Implement network validation and polling orchestration**

Extend `instock/core/eastmoney_cookie_manager.py` with a second layer:

```python
import time
import requests
from dataclasses import dataclass
from playwright.sync_api import sync_playwright

from instock.core import eastmoney_cookie_store as cookie_store


VALIDATION_URL = "https://push2.eastmoney.com/api/qt/clist/get"
VALIDATION_PARAMS = {
    "pn": "1",
    "pz": "1",
    "po": "1",
    "np": "1",
    "fltt": "2",
    "invt": "2",
    "fid": "f12",
    "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
    "fields": "f12,f14,f2",
    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
}


@dataclass(frozen=True)
class AcquisitionResult:
    exit_code: int
    cookie: str | None
    validation_reason: str
    observed_status_cookie_names: tuple[str, ...] = ()


def make_fake_clock(values):
    values = iter(values)
    return lambda: next(values)


def validate_cookie(cookie_string: str, session: requests.Session | None = None, sleep_fn=time.sleep) -> ValidationResult:
    session = session or requests.Session()
    transient_statuses = {429, 500, 502, 503, 504}
    backoffs = [0.0, 0.5, 1.0, 2.0]
    for attempt, delay in enumerate(backoffs, start=1):
        if delay:
            sleep_fn(delay)
        response = session.get(
            VALIDATION_URL,
            params=VALIDATION_PARAMS,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Cookie": cookie_string,
            },
            timeout=8,
        )
        if response.status_code in transient_statuses and attempt < len(backoffs):
            continue
        if response.status_code != 200:
            return ValidationResult(False, f"http-{response.status_code}")
        try:
            payload = response.json()
        except ValueError:
            return ValidationResult(False, "invalid-json")
        return validate_push2_payload(payload)
    return ValidationResult(False, "retry-exhausted")


def acquire_cookie_from_context(context, page, browser, deadline, now_fn=time.monotonic, sleep_fn=time.sleep, validate_fn=validate_cookie):
    attempted = set()
    next_validation_not_before = now_fn()
    pending_candidate = None

    while True:
        now = now_fn()
        if page.is_closed() or not browser.is_connected():
            return AcquisitionResult(exit_code=3, cookie=None, validation_reason="browser-closed")

        try:
            cookies = context.cookies()
        except Exception as exc:
            if "closed" in str(exc).lower():
                return AcquisitionResult(exit_code=3, cookie=None, validation_reason="browser-closed")
            raise

        candidate = collect_cookie_string(cookies)
        if candidate and candidate not in attempted:
            try:
                cookie_store.reject_control_characters(candidate)
            except ValueError:
                candidate = None

        if candidate and candidate not in attempted:
            if now >= next_validation_not_before:
                attempted.add(candidate)
                result = validate_fn(candidate)
                next_validation_not_before = now + 3.0
                if result.ok:
                    status_names = tuple(sorted(cookie["name"] for cookie in cookies if cookie["name"].startswith("st_")))
                    return AcquisitionResult(exit_code=0, cookie=candidate, validation_reason=result.reason, observed_status_cookie_names=status_names)
            elif now <= deadline:
                pending_candidate = candidate

        if now > deadline:
            if pending_candidate and pending_candidate not in attempted:
                attempted.add(pending_candidate)
                result = validate_fn(pending_candidate)
                if result.ok:
                    return AcquisitionResult(exit_code=0, cookie=pending_candidate, validation_reason=result.reason)
                return AcquisitionResult(exit_code=4, cookie=None, validation_reason=result.reason)
            return AcquisitionResult(exit_code=3 if not attempted else 4, cookie=None, validation_reason="timeout")

        sleep_fn(2.0)


def acquire_cookie_via_browser(browser_name: str, timeout_seconds: int, playwright_factory=sync_playwright, now_fn=time.monotonic, sleep_fn=time.sleep, validate_fn=validate_cookie):
    target_url = "https://quote.eastmoney.com/center/gridlist.html#hs_a_board"
    with playwright_factory() as pw:
        if browser_name == "chromium":
            browser = pw.chromium.launch()
        elif browser_name == "chrome":
            browser = pw.chromium.launch(channel="chrome")
        else:
            browser = pw.chromium.launch(channel="msedge")

        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            deadline = now_fn() + timeout_seconds
            return acquire_cookie_from_context(
                context=context,
                page=page,
                browser=browser,
                deadline=deadline,
                now_fn=now_fn,
                sleep_fn=sleep_fn,
                validate_fn=validate_fn,
            )
        finally:
            context.close()
            browser.close()
```

实现时在这个 task 内一次补齐这些细节，不要等 CLI 才补：

- 控制字符候选直接跳过，不加入 `attempted`
- 远端 `429/500/502/503/504` 做 0.5 / 1 / 2 秒退避重试
- `401/403`、invalid JSON、字段结构不合法视为当前候选的非重试失败
- `context.cookies()` 抛出 browser/context closed 异常时映射为退出码 `3`
- `acquire_cookie_via_browser()` 负责 `page.goto(..., wait_until="domcontentloaded")`，deadline 从这一步成功返回之后才开始计算

- [ ] **Step 4: Re-run the manager tests**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_manager.py -v`
Expected: PASS。

- [ ] **Step 5: Commit orchestration and validation**

```bash
git add instock/core/eastmoney_cookie_manager.py tests/core/test_eastmoney_cookie_manager.py
git commit -m "feat(cookie): add eastmoney cookie acquisition and validation engine"
```

## Chunk 3: CLI Contract, Docs, And Final Verification

### Task 6: Implement the CLI with exact stdout/stderr and exit-code behavior

**Files:**
- Modify: `instock/core/eastmoney_cookie_manager.py`
- Create: `instock/job/update_eastmoney_cookie.py`
- Create: `tests/job/test_update_eastmoney_cookie.py`

- [ ] **Step 1: Write failing CLI tests for argument and output contracts**

Create `tests/job/test_update_eastmoney_cookie.py`:

```python
from instock.job import update_eastmoney_cookie as cli
from instock.core.eastmoney_cookie_manager import AcquisitionResult


def test_parse_args_rejects_show_cookie_with_env_mode():
    try:
        cli.parse_args(["--write", "env", "--show-cookie"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected parser to reject invalid combination")


def test_main_outputs_export_to_stdout_only_for_env(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))

    exit_code = cli.main(["--write", "env"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert out.out == cli.cookie_store.build_env_export("foo=bar")
    assert "foo=bar" not in out.err


def test_main_keeps_stdout_empty_on_non_zero_exit(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=4, cookie=None, validation_reason="bad-cookie"))

    exit_code = cli.main(["--write", "file"])
    out = capsys.readouterr()

    assert exit_code == 4
    assert out.out == ""


def test_main_outputs_full_cookie_only_after_successful_file_write(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": True, "warning": None})())

    exit_code = cli.main(["--write", "file", "--show-cookie"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert out.out == "foo=bar\n"


def test_main_write_both_outputs_export_even_when_file_is_unchanged(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setenv("EAST_MONEY_COOKIE", "foo=bar")
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": False, "warning": None})())

    exit_code = cli.main(["--write", "both"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert out.out == cli.cookie_store.build_env_export("foo=bar")
    assert out.err.count("unchanged") >= 1


def test_main_maps_write_failure_to_exit_code_5(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))

    exit_code = cli.main(["--write", "file", "--show-cookie"])
    out = capsys.readouterr()

    assert exit_code == 5
    assert out.out == ""
    assert "WARNING:" in out.err


def test_main_logs_observed_status_cookie_names(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "acquire_cookie",
        lambda args: AcquisitionResult(
            exit_code=0,
            cookie="foo=bar",
            validation_reason="ok",
            observed_status_cookie_names=("st_pvi", "st_si"),
        ),
    )
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": True, "warning": None})())

    exit_code = cli.main(["--write", "file"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert "st_pvi" in out.err and "st_si" in out.err


def test_main_prints_mode_specific_env_warning_for_write_file(monkeypatch, capsys):
    monkeypatch.setenv("EAST_MONEY_COOKIE", "env=1")
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": True, "warning": None})())

    exit_code = cli.main(["--write", "file"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert "still prefers the environment value instead of the file" in out.err


def test_main_passes_parsed_browser_and_timeout_to_acquire(monkeypatch):
    captured = {}

    def fake_acquire(args):
        captured["browser"] = args.browser
        captured["timeout"] = args.timeout
        return AcquisitionResult(exit_code=3, cookie=None, validation_reason="timeout")

    monkeypatch.setattr(cli, "acquire_cookie", fake_acquire)

    exit_code = cli.main(["--write", "file", "--browser", "msedge", "--timeout", "42"])

    assert exit_code == 3
    assert captured == {"browser": "msedge", "timeout": 42}
```

- [ ] **Step 2: Run the CLI tests and confirm failure**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/job/test_update_eastmoney_cookie.py -v`
Expected: FAIL because CLI file does not exist yet.

- [ ] **Step 3: Implement the CLI entrypoint**

Create `instock/job/update_eastmoney_cookie.py`:

```python
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from instock.core import eastmoney_cookie_store as cookie_store
from instock.core import eastmoney_cookie_manager as cookie_manager


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Acquire and update Eastmoney cookie")
    parser.add_argument("--write", choices=("file", "env", "both"), default="file")
    parser.add_argument("--browser", choices=("chromium", "chrome", "msedge"), default="chromium")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--show-cookie", action="store_true")
    args = parser.parse_args(argv)
    if args.show_cookie and args.write != "file":
        parser.error("--show-cookie only works with --write file")
    return args


def acquire_cookie(args):
    return cookie_manager.acquire_cookie_via_browser(browser_name=args.browser, timeout_seconds=args.timeout)


def _warn_env_precedence(write_mode: str):
    current = os.environ.get("EAST_MONEY_COOKIE")
    if not current:
        return
    if write_mode == "file":
        print("WARNING: EAST_MONEY_COOKIE is already set; runtime still prefers the environment value instead of the file.", file=sys.stderr)
    elif write_mode == "both":
        print("WARNING: EAST_MONEY_COOKIE is already set; runtime still prefers the environment value even after writing the file and printing a new export.", file=sys.stderr)
    else:
        print("WARNING: EAST_MONEY_COOKIE is already set; the new value will only be emitted to stdout for you to source manually.", file=sys.stderr)


def main(argv=None):
    args = parse_args(argv)
    _warn_env_precedence(args.write)
    result = acquire_cookie(args)
    if result.exit_code != 0 or not result.cookie:
        print(f"WARNING: cookie acquisition failed: {result.validation_reason}", file=sys.stderr)
        return result.exit_code

    if result.observed_status_cookie_names:
        print("status cookies observed: " + ", ".join(result.observed_status_cookie_names), file=sys.stderr)

    env_unchanged = bool(os.environ.get("EAST_MONEY_COOKIE")) and os.environ.get("EAST_MONEY_COOKIE") == result.cookie
    write_result = None
    if args.write in ("file", "both"):
        try:
            write_result = cookie_store.write_cookie_file(result.cookie)
        except OSError as exc:
            print(f"WARNING: failed to write cookie file: {exc}", file=sys.stderr)
            return 5
        if write_result.warning == "chmod-failed":
            print("WARNING: cookie file written, but chmod(0o600) failed.", file=sys.stderr)
        if not write_result.changed:
            print("unchanged", file=sys.stderr)

    if args.write in ("env", "both") and env_unchanged:
        print("unchanged", file=sys.stderr)

    if args.write == "file" and args.show_cookie:
        sys.stdout.write(result.cookie + "\n")
    elif args.write in ("env", "both"):
        sys.stdout.write(cookie_store.build_env_export(result.cookie))

    print("cookie updated", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Before wiring the CLI, make one small follow-up patch in `instock/core/eastmoney_cookie_manager.py` so browser launch failures map cleanly to exit code `2` instead of bubbling out as unknown exceptions:

```python
from playwright.sync_api import Error as PlaywrightError


def acquire_cookie_via_browser(...):
    try:
        with playwright_factory() as pw:
            ...
    except PlaywrightError as exc:
        lowered = str(exc).lower()
        if "executable" in lowered or "channel" in lowered:
            return AcquisitionResult(exit_code=2, cookie=None, validation_reason="browser-not-available")
        raise
```

- [ ] **Step 4: Re-run the CLI tests**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/job/test_update_eastmoney_cookie.py -v`
Expected: PASS。

- [ ] **Step 5: Commit the CLI**

```bash
git add instock/job/update_eastmoney_cookie.py tests/job/test_update_eastmoney_cookie.py
git commit -m "feat(job): add eastmoney cookie update cli"
```

### Task 7: Update ignore rules and replace README manual-only instructions with the automated flow

**Files:**
- Modify: `instock/config/.gitignore`
- Modify: `README.md`

- [ ] **Step 1: Add the cookie file to config ignore rules**

Update `instock/config/.gitignore` by appending:

```gitignore
eastmoney_cookie.txt
```

- [ ] **Step 2: Replace the README cookie section with the automated command first**

Modify the section around `### 8.设置东方财富网Cookie` in `README.md` so it leads with the new tool, then keeps manual fallback as secondary guidance:

```markdown
### 8.设置东方财富网Cookie

东方财富数据获取频率过高时，建议优先使用仓库内置脚本自动采集并更新 Cookie：

先安装 Python 依赖：

```bash
pip install -r requirements.txt
```

```bash
python instock/job/update_eastmoney_cookie.py --write file
python instock/job/update_eastmoney_cookie.py --write env
python instock/job/update_eastmoney_cookie.py --write both
```

首次使用前需要安装浏览器运行时：

```bash
python -m playwright install chromium
```

说明：

- `--write file`：写入 `instock/config/eastmoney_cookie.txt`
- `--write env`：输出 `export EAST_MONEY_COOKIE='...'`
- `--write both`：同时完成两者
- 日志走 `stderr`，导出命令走 `stdout`
- `chromium` 是默认且保证支持的通道；`chrome` / `msedge` 依赖本机安装
- Cookie 过期后可直接重复运行同一条命令刷新；通常建议定期更新
- 如果你更习惯手工方式，下面保留原来的开发者工具复制方案作为 fallback
```

README 这一节最终必须覆盖 7 个点，不能只放示例命令：

1. 安装 Python 依赖 `playwright`
2. 安装浏览器运行时 `python -m playwright install chromium`
3. 三种脚本用法 `--write file|env|both`
4. 三种模式各自写到哪里 / 输出到哪里
5. Cookie 过期后的刷新建议
6. 浏览器通道说明：`chromium` 默认，`chrome` / `msedge` 依赖本机
7. stdout/stderr 契约与 `WARNING:` 前缀约定

- [ ] **Step 3: Keep Docker guidance aligned with the file-based path**

检查 README 里 Docker 挂载示例仍然指向 `instock/config/eastmoney_cookie.txt`，不要改动容器路径约定，只把“如何生成该文件”换成新脚本。

同时明确说明：这里是追加到现有 `instock/config/.gitignore`，不是新建第二个 ignore 文件。

- [ ] **Step 4: Run focused tests after doc/config changes**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core tests/job -v`
Expected: PASS。

- [ ] **Step 5: Commit docs and ignore rules**

```bash
git add instock/config/.gitignore README.md
git commit -m "docs: document automated eastmoney cookie workflow"
```

### Task 8: Execute the final verification pass

**Files:**
- Modify: none expected
- Verify: `instock/core/eastmoney_cookie_store.py`, `instock/core/eastmoney_cookie_manager.py`, `instock/core/eastmoney_fetcher.py`, `instock/job/update_eastmoney_cookie.py`, `README.md`, `instock/config/.gitignore`

- [ ] **Step 1: Run the targeted unit suite**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/core/test_eastmoney_cookie_store.py tests/core/test_eastmoney_fetcher.py tests/core/test_eastmoney_cookie_manager.py tests/job/test_update_eastmoney_cookie.py -v`
Expected: 全部通过。

- [ ] **Step 2: Run the broader regression suite that already exists in repo**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/datasource tests/factors -v`
Expected: 现有 datasource/factors 测试继续通过，证明这次功能没有破坏最近新增的数据因子链路。

- [ ] **Step 3: Perform one manual dry-run without logging in**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && python instock/job/update_eastmoney_cookie.py --write file --timeout 5 > /tmp/eastmoney.out 2> /tmp/eastmoney.err; echo $?`
Expected: 浏览器能正常打开，5 秒后以退出码 `3` 或 `4` 失败；`/tmp/eastmoney.out` 为空；不覆盖现有文件。

- [ ] **Step 4: Perform one manual happy-path run with interactive login**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && python instock/job/update_eastmoney_cookie.py --write both --timeout 300 > /tmp/eastmoney.out 2> /tmp/eastmoney.err; echo $? && cat /tmp/eastmoney.out && stat -c '%a' instock/config/eastmoney_cookie.txt`
Expected: 成功输出一行 `export EAST_MONEY_COOKIE=...` 到 `stdout`；`stderr` 只显示脱敏日志和可能的 `unchanged` / `WARNING:`；文件存在且权限优先为 `600`（若不是，stderr 应已有 warning）。

- [ ] **Step 5: Verify the negative exit-code contracts through focused pytest cases**

Run: `cd /mnt/disk1/zy/stock_related/stock2 && pytest tests/job/test_update_eastmoney_cookie.py -k 'write_failure or show_cookie or non_zero_exit' -v`
Expected: `exit code 5`、`stdout empty on failure`、参数组合错误等关键契约全部通过。

- [ ] **Step 6: Commit the final green build**

```bash
git add instock/core/eastmoney_cookie_store.py instock/core/eastmoney_cookie_manager.py \
        instock/core/eastmoney_fetcher.py instock/job/update_eastmoney_cookie.py \
        instock/config/.gitignore README.md tests/core tests/job
git commit -m "feat: add automated eastmoney cookie acquisition workflow"
```