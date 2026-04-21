from __future__ import annotations

import os
from pathlib import Path

from instock.monitoring import get_monitoring_root


def test_default_root(monkeypatch):
    monkeypatch.delenv("INSTOCK_MONITORING_ROOT", raising=False)
    assert get_monitoring_root() == Path("data/monitoring")


def test_override_root(monkeypatch, tmp_path):
    monkeypatch.setenv("INSTOCK_MONITORING_ROOT", str(tmp_path))
    assert get_monitoring_root() == tmp_path
