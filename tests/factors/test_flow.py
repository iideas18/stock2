from datetime import date
import pandas as pd

from instock.factors.flow.north import NorthHoldingChgFactor


def _fake_north():
    return pd.DataFrame({
        "trade_date": pd.to_datetime(
            ["2024-01-08", "2024-01-12", "2024-01-08", "2024-01-12"]
        ),
        "code":       ["600519", "600519", "000001", "000001"],
        "hold_shares":[1.0e8, 1.1e8, 2.0e8, 1.9e8],
        "hold_ratio": [0.10,  0.11,  0.05,  0.048],
    })


def test_north_holding_chg_5d(monkeypatch):
    monkeypatch.setattr(
        "instock.factors.flow.north._fetch_north", lambda s, e: _fake_north()
    )
    f = NorthHoldingChgFactor(window=5)
    out = f.compute(
        ["600519", "000001"], date(2024, 1, 12), date(2024, 1, 12)
    )
    val = out.set_index("code")["value"]
    assert abs(val["600519"] - 0.10) < 1e-9
    assert abs(val["000001"] - (-0.05)) < 1e-9
