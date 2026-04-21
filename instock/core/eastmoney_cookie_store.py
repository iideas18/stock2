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
