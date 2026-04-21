from __future__ import annotations

import pandas as pd
import pytest

from instock.monitoring.status import ArtifactFreshness, StatusRow


def _loader_returning(ts: pd.Timestamp | None):
    def _loader() -> pd.Timestamp | None:
        return ts
    return _loader


def test_row_dataclass_frozen():
    r = StatusRow(
        name="x", status="GREEN", message="ok",
        metric_value=1.0, as_of=pd.Timestamp("2026-04-21"),
    )
    with pytest.raises(Exception):
        r.name = "y"  # frozen


def test_fresh_returns_green():
    now = pd.Timestamp.now()
    chk = ArtifactFreshness(
        name="f.mom_5d",
        loader=_loader_returning(now - pd.Timedelta(hours=1)),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    row = chk.run()
    assert row.status == "GREEN"
    assert row.name == "f.mom_5d"


def test_stale_red():
    old = pd.Timestamp.now() - pd.Timedelta(days=10)
    chk = ArtifactFreshness(
        name="f.mom_5d", loader=_loader_returning(old),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    assert chk.run().status == "RED"


def test_mid_yellow():
    mid = pd.Timestamp.now() - pd.Timedelta(days=2)
    chk = ArtifactFreshness(
        name="f.mom_5d", loader=_loader_returning(mid),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    assert chk.run().status == "YELLOW"


def test_none_loader_yellow():
    chk = ArtifactFreshness(
        name="f.mom_5d", loader=_loader_returning(None),
        max_age_days_yellow=1, max_age_days_red=3,
    )
    row = chk.run()
    assert row.status == "YELLOW"
    assert "no data" in row.message.lower()


def test_loader_exception_red():
    def boom():
        raise RuntimeError("kaboom")
    chk = ArtifactFreshness(
        name="f.x", loader=boom,
        max_age_days_yellow=1, max_age_days_red=3,
    )
    row = chk.run()
    assert row.status == "RED"
    assert "kaboom" in row.message


def test_invalid_status_raises():
    with pytest.raises(ValueError, match="invalid status"):
        StatusRow(
            name="x", status="BOGUS", message="", metric_value=None,
            as_of=pd.Timestamp("2026-04-21"),
        )
