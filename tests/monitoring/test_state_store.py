from __future__ import annotations

from pathlib import Path

import pandas as pd

from instock.monitoring.state import AlertStateStore
from instock.monitoring.status import StatusRow


def _row(name: str, status: str) -> StatusRow:
    return StatusRow(
        name=name, status=status, message="",
        metric_value=None, as_of=pd.Timestamp.now(),
    )


def test_empty_last_status_none(tmp_path):
    st = AlertStateStore(tmp_path)
    assert st.last_status("x") is None


def test_record_roundtrip(tmp_path):
    st = AlertStateStore(tmp_path)
    r = _row("chk", "GREEN")
    st.record(r)
    got = st.last_status("chk")
    assert got is not None
    assert got.name == "chk"
    assert got.status == "GREEN"


def test_last_status_returns_latest(tmp_path):
    st = AlertStateStore(tmp_path)
    st.record(_row("chk", "GREEN"))
    st.record(_row("chk", "RED"))
    assert st.last_status("chk").status == "RED"


def test_history_limits(tmp_path):
    st = AlertStateStore(tmp_path)
    for s in ["GREEN"] * 5 + ["RED"] * 5:
        st.record(_row("chk", s))
    hist = st.history("chk", n=3)
    assert len(hist) == 3
    assert list(hist["status"]) == ["RED", "RED", "RED"]


def test_other_check_isolated(tmp_path):
    st = AlertStateStore(tmp_path)
    st.record(_row("a", "GREEN"))
    st.record(_row("b", "RED"))
    assert st.last_status("a").status == "GREEN"
    assert st.last_status("b").status == "RED"
