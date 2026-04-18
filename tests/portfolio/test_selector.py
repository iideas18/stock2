import pandas as pd

from instock.portfolio.selector import TopNSelector, TopQuantileSelector


def _scores(pairs):
    return pd.Series({c: s for c, s in pairs})


def test_top_quantile_picks_top_fraction():
    s = _scores([("A00001", 0.1), ("B00002", 0.2), ("C00003", 0.9),
                 ("D00004", 0.8), ("E00005", 0.5)])
    out = TopQuantileSelector(quantile=0.4).select(
        s, ["A00001", "B00002", "C00003", "D00004", "E00005"]
    )
    assert set(out) == {"C00003", "D00004"}


def test_top_quantile_guarantees_at_least_one():
    s = _scores([("A00001", 0.1), ("B00002", 0.2)])
    out = TopQuantileSelector(quantile=0.0001).select(
        s, ["A00001", "B00002"]
    )
    assert len(out) == 1
    assert out == ["B00002"]


def test_top_quantile_respects_universe_filter():
    s = _scores([("A00001", 0.9), ("B00002", 0.8), ("C00003", 0.1)])
    out = TopQuantileSelector(quantile=0.5).select(
        s, ["B00002", "C00003"]
    )
    assert out == ["B00002"]


def test_top_n_returns_top_n():
    s = _scores([("A00001", 0.1), ("B00002", 0.9), ("C00003", 0.5)])
    out = TopNSelector(n=2).select(s, ["A00001", "B00002", "C00003"])
    assert out == ["B00002", "C00003"]


def test_top_n_returns_all_if_universe_smaller():
    s = _scores([("A00001", 0.1), ("B00002", 0.9)])
    out = TopNSelector(n=5).select(s, ["A00001", "B00002"])
    assert set(out) == {"A00001", "B00002"}


def test_selector_empty_universe_returns_empty():
    s = _scores([("A00001", 0.9)])
    assert TopNSelector(n=5).select(s, []) == []
    assert TopQuantileSelector(0.1).select(s, []) == []
