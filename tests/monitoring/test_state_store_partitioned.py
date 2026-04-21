"""Tests for AlertStateStore partitioned layout + monotonic ordering."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from instock.monitoring.state import AlertStateStore
from instock.monitoring.status import StatusRow


def _row(name: str, status: str, msg: str = "") -> StatusRow:
    return StatusRow(
        name=name, status=status, message=msg,
        metric_value=None, as_of=pd.Timestamp.now(),
    )


def test_same_millisecond_records_ordered_deterministically(
    tmp_path: Path,
) -> None:
    """Two records in tight succession must be ordered by insertion order."""
    st = AlertStateStore(tmp_path)
    # Record many in a tight loop; time_ns() guarantees strict monotonicity.
    for i in range(50):
        st.record(_row("chk", "RED" if i == 49 else "GREEN"))
    assert st.last_status("chk").status == "RED"

    hist = st.history("chk", n=50)
    # recorded_at is strictly increasing (descending in history view)
    ras = list(hist["recorded_at"])
    assert ras == sorted(ras, reverse=True)
    assert len(set(ras)) == 50  # all distinct


def test_partitioned_files_created(tmp_path: Path) -> None:
    st = AlertStateStore(tmp_path)
    st.record(_row("chk", "GREEN"))
    part_dir = tmp_path / "_alerts"
    assert part_dir.exists()
    files = list(part_dir.glob("*.parquet"))
    assert len(files) == 1
    # Filename matches YYYY-MM.parquet
    name = files[0].stem
    assert len(name) == 7 and name[4] == "-"


def test_legacy_single_file_migrated(tmp_path: Path) -> None:
    """If <root>/_alerts.parquet exists, constructor migrates into <root>/_alerts/*.parquet."""
    legacy = tmp_path / "_alerts.parquet"
    legacy_df = pd.DataFrame([{
        "check_name": "old",
        "status": "GREEN",
        "message": "legacy",
        "metric_value": 1.5,
        "as_of": pd.Timestamp("2026-03-15"),
        "recorded_at": pd.Timestamp("2026-03-15 09:00:00"),
    }])
    legacy_df.to_parquet(legacy, index=False)
    st = AlertStateStore(tmp_path)
    # Legacy file gone, partitioned layout created
    assert not legacy.exists()
    part_dir = tmp_path / "_alerts"
    assert any(part_dir.glob("*.parquet"))
    got = st.last_status("old")
    assert got is not None
    assert got.status == "GREEN"
    assert got.metric_value == 1.5


def test_record_does_not_rewrite_unrelated_partitions(
    tmp_path: Path,
) -> None:
    """Appending to current month's partition must not touch other months."""
    st = AlertStateStore(tmp_path)
    # Seed an old partition manually.
    old_part = tmp_path / "_alerts" / "2025-01.parquet"
    seed = pd.DataFrame([{
        "check_name": "old",
        "status": "GREEN",
        "message": "",
        "metric_value": None,
        "as_of": pd.Timestamp("2025-01-15"),
        "recorded_at": int(pd.Timestamp("2025-01-15 09:00:00").value),
    }])
    seed = seed.astype({"metric_value": "float64", "recorded_at": "int64"})
    seed.to_parquet(old_part, index=False)
    mtime_before = old_part.stat().st_mtime_ns

    # Record something today — should go to today's partition only.
    st.record(_row("new", "RED"))
    mtime_after = old_part.stat().st_mtime_ns
    assert mtime_before == mtime_after  # old partition untouched

    # But history still reads across partitions.
    assert st.last_status("old") is not None
    assert st.last_status("new").status == "RED"
