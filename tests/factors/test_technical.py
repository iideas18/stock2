from datetime import date
import pandas as pd

from instock.factors.technical.momentum import MomentumFactor


def _fake_ohlcv(code: str, closes: list[float]) -> pd.DataFrame:
    n = len(closes)
    return pd.DataFrame({
        "date": pd.date_range("2024-01-02", periods=n, freq="B"),
        "code": [code] * n,
        "open": closes, "high": closes, "low": closes, "close": closes,
        "volume": [1e6] * n, "amount": [1e9] * n,
    })


def test_momentum_20d_returns_expected(monkeypatch):
    f = MomentumFactor(window=5)

    def fake_fetch(codes, start, end):
        return pd.concat(
            [_fake_ohlcv("600519", [10, 11, 12, 13, 14, 15])] +
            [_fake_ohlcv("000001", [5, 5, 5, 5, 5, 5])],
            ignore_index=True,
        )

    monkeypatch.setattr(
        "instock.factors.technical.momentum._fetch_ohlcv", fake_fetch
    )
    df = f.compute(["600519", "000001"], date(2024, 1, 2), date(2024, 1, 10))
    row_519 = df[df["code"] == "600519"].iloc[-1]
    row_001 = df[df["code"] == "000001"].iloc[-1]
    assert abs(row_519["value"] - (15 / 10 - 1)) < 1e-9
    assert abs(row_001["value"] - 0.0) < 1e-9


def test_momentum_factor_metadata():
    f = MomentumFactor(window=20)
    assert f.name == "mom_20d"
    assert f.category == "technical"
