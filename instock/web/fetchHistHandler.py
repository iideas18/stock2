#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
import datetime
import os
import sys
import threading
import uuid
import subprocess
import concurrent.futures

from tornado import gen

import instock.web.base as webBase
import instock.core.stockfetch as stf
from instock.core.stockfetch import stock_hist_cache

__author__ = 'myh '
__date__ = '2025/12/23 '


_batch_jobs_lock = threading.Lock()
_batch_jobs = {}

_daily_jobs_lock = threading.Lock()
_daily_jobs = {}


def _parse_ymd_or_none(date_str, field_name):
    if not date_str:
        return None, None
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date(), None
    except ValueError:
        return None, f"{field_name} must be YYYY-MM-DD"


def _project_root_dir():
    # fetchHistHandler.py -> instock/web -> instock -> project root
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))


def _instock_log_dir():
    return os.path.join(_project_root_dir(), "instock", "log")


def _read_log_tail(path, max_bytes=4096):
    try:
        if not path or not os.path.isfile(path):
            return ""
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            f.seek(max(0, size - max_bytes))
            data = f.read(max_bytes)
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _hist_cache_file_path(code, date_start, adjust):
    try:
        base_dir = getattr(stf, "stock_hist_cache_path", None)
        if not base_dir:
            return None
        cache_dir = os.path.join(base_dir, date_start[0:6], date_start)
        return os.path.join(cache_dir, f"{code}{adjust}.gzip.pickle")
    except Exception:
        return None


def _hist_cache_exists(code, date_start, adjust):
    cache_file = _hist_cache_file_path(code, date_start, adjust)
    return bool(cache_file) and os.path.isfile(cache_file)


class FetchHistControlPanelHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        today = datetime.datetime.now().date().strftime("%Y-%m-%d")
        self.render(
            "fetch_hist_control.html",
            date_now=today,
            leftMenu=webBase.GetLeftMenu(self.request.uri),
        )


class FetchHistOneYearApiHandler(webBase.BaseHandler, ABC):
    def get(self):
        code = self.get_argument("code", default=None, strip=True)
        start_date_str = self.get_argument("start_date", default=None, strip=True)
        end_date_str = self.get_argument("end_date", default=None, strip=True)
        adjust = self.get_argument("adjust", default="qfq", strip=True)
        only_missing_str = self.get_argument("only_missing", default="0", strip=True)

        if not code:
            self.set_status(400)
            self.write({"ok": False, "error": "missing code"})
            return

        if not end_date_str:
            end_date = datetime.datetime.now().date()
        else:
            try:
                end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
            except ValueError:
                self.set_status(400)
                self.write({"ok": False, "error": "end_date must be YYYY-MM-DD"})
                return

        if start_date_str:
            try:
                start_date_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
            except ValueError:
                self.set_status(400)
                self.write({"ok": False, "error": "start_date must be YYYY-MM-DD"})
                return
        else:
            # Backwards-compatible default: last 1 year.
            start_date_date = end_date - datetime.timedelta(days=365)

        if start_date_date > end_date:
            self.set_status(400)
            self.write({"ok": False, "error": "start_date must be <= end_date"})
            return

        start_date = start_date_date.strftime("%Y%m%d")
        end_date_yyyymmdd = end_date.strftime("%Y%m%d")

        only_missing = str(only_missing_str).lower() not in ("0", "false", "no", "off", "")
        if only_missing and _hist_cache_exists(code, start_date, adjust):
            self.write({
                "ok": True,
                "skipped": True,
                "reason": "cache_exists",
                "code": code,
                "start_date": start_date,
                "end_date": end_date_yyyymmdd,
                "cache_file": _hist_cache_file_path(code, start_date, adjust),
                "rows": 0,
                "data": [],
            })
            return

        try:
            df = stock_hist_cache(
                code=code,
                date_start=start_date,
                date_end=end_date_yyyymmdd,
                is_cache=True,
                adjust=adjust,
            )
        except Exception as e:
            self.set_status(500)
            self.write({"ok": False, "error": str(e)})
            return

        if df is None:
            self.write({
                "ok": True,
                "code": code,
                "start_date": start_date,
                "end_date": end_date_yyyymmdd,
                "rows": 0,
                "data": [],
            })
            return

        # Keep response simple: list-of-records.
        records = df.reset_index(drop=True).to_dict(orient="records")
        self.write({
            "ok": True,
            "code": code,
            "start_date": start_date,
            "end_date": end_date_yyyymmdd,
            "rows": len(records),
            "columns": list(df.columns),
            "data": records,
        })


class FetchHistBatchStartHandler(webBase.BaseHandler, ABC):
    def get(self):
        start_date_str = self.get_argument("start_date", default=None, strip=True)
        end_date_str = self.get_argument("end_date", default=None, strip=True)
        adjust = self.get_argument("adjust", default="qfq", strip=True)
        workers_str = self.get_argument("workers", default="8", strip=True)
        only_missing_str = self.get_argument("only_missing", default="1", strip=True)

        end_date, err = _parse_ymd_or_none(end_date_str, "end_date")
        if err:
            self.set_status(400)
            self.write({"ok": False, "error": err})
            return
        if end_date is None:
            end_date = datetime.datetime.now().date()

        start_date, err = _parse_ymd_or_none(start_date_str, "start_date")
        if err:
            self.set_status(400)
            self.write({"ok": False, "error": err})
            return
        if start_date is None:
            start_date = end_date - datetime.timedelta(days=365)

        if start_date > end_date:
            self.set_status(400)
            self.write({"ok": False, "error": "start_date must be <= end_date"})
            return

        try:
            workers = int(workers_str)
        except ValueError:
            workers = 8
        workers = max(1, min(workers, 64))

        only_missing = str(only_missing_str).lower() not in ("0", "false", "no", "off", "")

        job_id = uuid.uuid4().hex
        job = {
            "job_id": job_id,
            "state": "queued",
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "adjust": adjust,
            "workers": workers,
            "only_missing": only_missing,
            "total_all": 0,
            "total": 0,
            "done": 0,
            "ok_count": 0,
            "fail_count": 0,
            "skipped": 0,
            "errors": [],
            "started_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "finished_at": None,
        }

        with _batch_jobs_lock:
            _batch_jobs[job_id] = job

        def _run():
            with _batch_jobs_lock:
                _batch_jobs[job_id]["state"] = "running"

            # Fetch all A-share codes (uses current spot list).
            try:
                spot = stf.fetch_stocks(None)
                if spot is None or len(spot.index) == 0:
                    raise RuntimeError("failed to fetch stock list")
                codes = sorted(set(spot["code"].astype(str).tolist()))
            except Exception as e:
                with _batch_jobs_lock:
                    _batch_jobs[job_id]["state"] = "failed"
                    _batch_jobs[job_id]["errors"].append(str(e))
                    _batch_jobs[job_id]["finished_at"] = datetime.datetime.now().isoformat(timespec="seconds")
                return

            with _batch_jobs_lock:
                _batch_jobs[job_id]["total_all"] = len(codes)

            if only_missing:
                codes_to_run = [c for c in codes if not _hist_cache_exists(c, job["start_date"], job["adjust"]) ]
                skipped = len(codes) - len(codes_to_run)
            else:
                codes_to_run = codes
                skipped = 0

            with _batch_jobs_lock:
                _batch_jobs[job_id]["total"] = len(codes_to_run)
                _batch_jobs[job_id]["skipped"] = skipped

            def _one(code):
                stock_hist_cache(
                    code=code,
                    date_start=job["start_date"],
                    date_end=job["end_date"],
                    is_cache=True,
                    adjust=job["adjust"],
                )
                return code

            # Nothing to do.
            if not codes_to_run:
                with _batch_jobs_lock:
                    _batch_jobs[job_id]["state"] = "finished"
                    _batch_jobs[job_id]["finished_at"] = datetime.datetime.now().isoformat(timespec="seconds")
                return

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_one, code): code for code in codes_to_run}
                for future in concurrent.futures.as_completed(futures):
                    code = futures[future]
                    try:
                        future.result()
                        with _batch_jobs_lock:
                            _batch_jobs[job_id]["ok_count"] += 1
                    except Exception as e:
                        with _batch_jobs_lock:
                            _batch_jobs[job_id]["fail_count"] += 1
                            errs = _batch_jobs[job_id]["errors"]
                            if len(errs) < 50:
                                errs.append(f"{code}: {e}")
                    finally:
                        with _batch_jobs_lock:
                            _batch_jobs[job_id]["done"] += 1

            with _batch_jobs_lock:
                _batch_jobs[job_id]["state"] = "finished"
                _batch_jobs[job_id]["finished_at"] = datetime.datetime.now().isoformat(timespec="seconds")

        t = threading.Thread(target=_run, daemon=True)
        t.start()

        self.write({"ok": True, "job_id": job_id, "job": job})


class FetchHistBatchStatusHandler(webBase.BaseHandler, ABC):
    def get(self):
        job_id = self.get_argument("job_id", default=None, strip=True)
        if not job_id:
            self.set_status(400)
            self.write({"ok": False, "error": "missing job_id"})
            return

        with _batch_jobs_lock:
            job = _batch_jobs.get(job_id)
            if not job:
                self.set_status(404)
                self.write({"ok": False, "error": "job not found"})
                return
            # return a shallow copy to avoid racey mutation during json serialization
            resp = dict(job)

        self.write({"ok": True, "job": resp})


class RunDailyJobStartHandler(webBase.BaseHandler, ABC):
    def get(self):
        only_missing_str = self.get_argument("only_missing", default="0", strip=True)
        only_missing = str(only_missing_str).lower() not in ("0", "false", "no", "off", "")

        job_id = uuid.uuid4().hex

        log_dir = _instock_log_dir()
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception:
            pass

        log_path = os.path.join(log_dir, f"daily_job_web_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{job_id}.log")

        script_path = os.path.join(_project_root_dir(), "instock", "job", "execute_daily_job.py")
        if not os.path.isfile(script_path):
            self.set_status(500)
            self.write({"ok": False, "error": "execute_daily_job.py not found"})
            return

        try:
            f = open(log_path, "ab", buffering=0)
        except Exception as e:
            self.set_status(500)
            self.write({"ok": False, "error": str(e)})
            return

        try:
            cmd = [sys.executable, "-u", script_path]
            if only_missing:
                cmd.append("--only-missing-cache")
            proc = subprocess.Popen(
                cmd,
                cwd=_project_root_dir(),
                stdout=f,
                stderr=subprocess.STDOUT,
            )
        except Exception as e:
            try:
                f.close()
            except Exception:
                pass
            self.set_status(500)
            self.write({"ok": False, "error": str(e)})
            return

        job = {
            "job_id": job_id,
            "pid": proc.pid,
            "log_path": log_path,
            "started_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "only_missing_cache": only_missing,
        }

        with _daily_jobs_lock:
            _daily_jobs[job_id] = {"proc": proc, "file": f, **job}

        self.write({"ok": True, "job": job})


class RunDailyJobStatusHandler(webBase.BaseHandler, ABC):
    def get(self):
        job_id = self.get_argument("job_id", default=None, strip=True)
        max_bytes_str = self.get_argument("max_bytes", default="4096", strip=True)
        if not job_id:
            self.set_status(400)
            self.write({"ok": False, "error": "missing job_id"})
            return

        try:
            max_bytes = int(max_bytes_str)
        except ValueError:
            max_bytes = 4096
        max_bytes = max(512, min(max_bytes, 1024 * 256))

        with _daily_jobs_lock:
            job = _daily_jobs.get(job_id)
        if not job:
            self.set_status(404)
            self.write({"ok": False, "error": "job not found"})
            return

        proc = job.get("proc")
        exit_code = None
        running = False
        if proc is not None:
            exit_code = proc.poll()
            running = exit_code is None

        tail = _read_log_tail(job.get("log_path"), max_bytes=max_bytes)

        self.write({
            "ok": True,
            "job": {
                "job_id": job_id,
                "pid": job.get("pid"),
                "running": running,
                "exit_code": exit_code,
                "log_path": job.get("log_path"),
                "started_at": job.get("started_at"),
                "log_tail": tail,
            }
        })
