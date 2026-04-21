from __future__ import annotations

import pandas as pd
import pytest

from instock.monitoring import runner as rn
from instock.monitoring.status import StatusCheck, StatusRow


class _FixedStatus(StatusCheck):
    def __init__(self, name, status):
        self.name = name
        self._status = status
    def run(self) -> StatusRow:
        return StatusRow(
            name=self.name, status=self._status, message="",
            metric_value=None, as_of=pd.Timestamp.now(),
        )


@pytest.fixture(autouse=True)
def _reg():
    rn.clear_registry()
    yield
    rn.clear_registry()


def test_first_red_fires_webhook(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "http://x")
    rn.register_check(_FixedStatus("c1", "RED"))
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append((url, payload)) or True,
    )
    job.run_once()
    assert len(posted) == 1
    assert posted[0][1]["name"] == "c1"


def test_consecutive_red_does_not_fire(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "http://x")
    rn.register_check(_FixedStatus("c1", "RED"))
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append(1) or True,
    )
    job.run_once()
    job.run_once()
    assert len(posted) == 1


def test_green_then_red_fires(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "http://x")
    chk = _FixedStatus("c1", "GREEN")
    rn.register_check(chk)
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append(1) or True,
    )
    job.run_once()
    chk._status = "RED"
    job.run_once()
    assert len(posted) == 1


def test_empty_url_no_post(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    monkeypatch.setenv("INSTOCK_WEBHOOK_URL", "")
    rn.register_check(_FixedStatus("c1", "RED"))
    posted = []
    from instock.job import monitoring_check_job as job
    monkeypatch.setattr(
        job, "post_webhook",
        lambda url, payload, timeout=5.0: posted.append(1) or True,
    )
    job.run_once()
    assert posted == []
