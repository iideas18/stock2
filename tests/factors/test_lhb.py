from datetime import date
import pandas as pd

from instock.factors.lhb.heat import LhbHeatFactor


def _fake_lhb():
    return pd.DataFrame({
        "trade_date": pd.to_datetime(
            ["2024-01-02", "2024-01-10", "2024-01-15"]
        ),
        "code":       ["600519", "600519", "000001"],
        "seat":       ["S1", "S2", "S3"],
        "buy_amount": [1e8, 2e8, 3e8],
        "sell_amount":[5e7, 1e8, 2e8],
        "reason":     ["r"] * 3,
    })


def test_lhb_heat(monkeypatch):
    monkeypatch.setattr(
        "instock.factors.lhb.heat._fetch_lhb", lambda s, e: _fake_lhb()
    )
    f = LhbHeatFactor(window=30)
    out = f.compute(["600519", "000001"], date(2024, 1, 20), date(2024, 1, 20))
    val = out.set_index("code")["value"]
    assert val["600519"] == 2
    assert val["000001"] == 1
