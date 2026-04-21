from __future__ import annotations

from pathlib import Path

import pytest

from instock.backtest.fingerprint import compute_fingerprint


def _make_file(tmp_path: Path, name: str, content: bytes) -> Path:
    p = tmp_path / name
    p.write_bytes(content)
    return p


def test_fingerprint_stable_same_inputs(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"data1")
    f2 = _make_file(tmp_path, "b.parquet", b"data2")
    cfg = {"strategy": "x", "rng_seed": 42}
    h1 = compute_fingerprint([f1, f2], cfg)
    h2 = compute_fingerprint([f1, f2], cfg)
    assert h1 == h2
    assert len(h1) == 64


def test_fingerprint_changes_on_file_content(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"data1")
    cfg = {"strategy": "x"}
    h1 = compute_fingerprint([f1], cfg)
    f1.write_bytes(b"data1-modified")
    h2 = compute_fingerprint([f1], cfg)
    assert h1 != h2


def test_fingerprint_changes_on_config(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"data1")
    h1 = compute_fingerprint([f1], {"rng_seed": 42})
    h2 = compute_fingerprint([f1], {"rng_seed": 43})
    assert h1 != h2


def test_fingerprint_order_invariant(tmp_path):
    f1 = _make_file(tmp_path, "a.parquet", b"x")
    f2 = _make_file(tmp_path, "b.parquet", b"y")
    h1 = compute_fingerprint([f1, f2], {})
    h2 = compute_fingerprint([f2, f1], {})
    assert h1 == h2


def test_fingerprint_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        compute_fingerprint([tmp_path / "missing.parquet"], {})
