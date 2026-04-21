"""Tests for factor report HTML renderer."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from instock.factors.evaluator import FactorReport
from instock.factors.report import render_factor_report, write_factor_report


def _sample_report() -> FactorReport:
    gr = pd.DataFrame(
        {"mean_return": [0.01, 0.02, 0.03]},
        index=pd.Index([0, 1, 2], name="grp"),
    )
    return FactorReport(
        ic_series=pd.Series([0.05, 0.07], index=pd.to_datetime(["2026-01-02", "2026-01-03"])),
        rank_ic_series=pd.Series([0.06, 0.08], index=pd.to_datetime(["2026-01-02", "2026-01-03"])),
        ic_mean=0.06,
        rank_ic_mean=0.07,
        ic_ir=1.25,
        group_returns=gr,
        decay={1: 0.06},
        turnover=0.35,
    )


def test_render_contains_name_and_metrics() -> None:
    html = render_factor_report("momentum_20d", _sample_report())
    assert "momentum_20d" in html
    assert "0.0600" in html  # ic_mean formatted to 4dp
    assert "1.2500" in html  # ic_ir
    assert "35.00%" in html  # turnover
    assert "<table" in html  # from group_returns.to_html()


def test_write_report_creates_file(tmp_path: Path) -> None:
    out = write_factor_report("momentum_20d", _sample_report(), root=tmp_path)
    assert out == tmp_path / "momentum_20d.html"
    assert out.exists()
    content = out.read_text(encoding="utf-8")
    assert "momentum_20d" in content
    assert len(content) > 100


def test_write_report_honors_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INSTOCK_FACTOR_REPORTS_ROOT", str(tmp_path))
    out = write_factor_report("f1", _sample_report())
    assert out == tmp_path / "f1.html"
    assert out.exists()
