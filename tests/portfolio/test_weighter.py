import pandas as pd

from instock.portfolio.weighter import EqualWeighter, WeighterContext


def test_equal_weighter_sums_to_one():
    ctx = WeighterContext(
        price_panel=pd.DataFrame(),
        mcap_panel=None,
        scores=pd.Series(dtype=float),
    )
    w = EqualWeighter().weigh(["A00001", "B00002", "C00003"], ctx)
    assert abs(w.sum() - 1.0) < 1e-12
    assert set(w.index) == {"A00001", "B00002", "C00003"}


def test_equal_weighter_empty_returns_empty_series():
    ctx = WeighterContext(
        price_panel=pd.DataFrame(),
        mcap_panel=None,
        scores=pd.Series(dtype=float),
    )
    w = EqualWeighter().weigh([], ctx)
    assert w.empty
