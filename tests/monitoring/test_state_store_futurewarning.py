"""Regression: AlertStateStore.record must not emit FutureWarning when
mixing numeric and None metric_value across successive rows."""
from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd

from instock.monitoring.state import AlertStateStore
from instock.monitoring.status import StatusRow


def test_record_none_after_numeric_no_futurewarning(tmp_path: Path) -> None:
    store = AlertStateStore(tmp_path)
    store.record(StatusRow(
        name="c1", status="GREEN", message="ok",
        metric_value=1.5, as_of=pd.Timestamp.now(),
    ))
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        store.record(StatusRow(
            name="c1", status="YELLOW", message="no data yet",
            metric_value=None, as_of=pd.Timestamp.now(),
        ))
