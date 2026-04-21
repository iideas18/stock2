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
