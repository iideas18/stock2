from __future__ import annotations

import pandas as pd
import pytest

from instock.monitoring import runner as rn
from instock.monitoring.status import StatusCheck, StatusRow


class _Good(StatusCheck):
    name = "good"
    def run(self) -> StatusRow:
        return StatusRow(
            name=self.name, status="GREEN", message="ok",
            metric_value=1.0, as_of=pd.Timestamp.now(),
        )


class _Bad(StatusCheck):
    name = "bad"
    def run(self) -> StatusRow:
        raise RuntimeError("kaboom")


@pytest.fixture(autouse=True)
def _clear():
    rn.clear_registry()
    yield
    rn.clear_registry()


def test_empty_registry_empty_list():
    assert rn.run_all_checks() == []


def test_good_check_runs():
    rn.register_check(_Good())
    rows = rn.run_all_checks()
    assert len(rows) == 1
    assert rows[0].status == "GREEN"


def test_bad_check_becomes_red():
    rn.register_check(_Bad())
    rows = rn.run_all_checks()
    assert len(rows) == 1
    assert rows[0].status == "RED"
    assert "kaboom" in rows[0].message


def test_one_bad_does_not_block_others():
    rn.register_check(_Good())
    rn.register_check(_Bad())
    rows = rn.run_all_checks()
    assert {r.status for r in rows} == {"GREEN", "RED"}
