from datetime import date
import pandas as pd

from instock.factors.fundamental.valuation import (
    PEFactor, PBFactor, ROEFactor,
)


def _fake_pit_rows():
    return pd.DataFrame({
        "code": ["600519", "600519"],
        "report_period":     pd.to_datetime(["2023-06-30", "2023-09-30"]),
        "announcement_date": pd.to_datetime(["2023-08-29", "2023-10-28"]),
        "pe":  [30.0, 28.0],
        "pb":  [8.0,  7.5],
        "roe": [25.0, 28.0],
    })


def test_pe_factor_respects_pit(monkeypatch):
    monkeypatch.setattr(
        "instock.factors.fundamental.valuation._fetch_pit",
        lambda codes, fields: _fake_pit_rows(),
    )
    f = PEFactor()
    out = f.compute(["600519"], date(2023, 10, 1), date(2023, 10, 1))
    assert len(out) == 1
    assert out["value"].iloc[0] == 30.0

    out2 = f.compute(["600519"], date(2023, 11, 1), date(2023, 11, 1))
    assert out2["value"].iloc[0] == 28.0


def test_factor_names():
    assert PEFactor().name == "pe_ttm"
    assert PBFactor().name == "pb"
    assert ROEFactor().name == "roe_ttm"
