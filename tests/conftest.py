import pytest


@pytest.fixture
def tmp_factor_root(tmp_path, monkeypatch):
    """Point the factor Parquet root at a per-test tmp dir."""
    root = tmp_path / "factors"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_FACTOR_ROOT", str(root))
    return root


@pytest.fixture
def tmp_holding_root(tmp_path, monkeypatch):
    """Point the holding Parquet root at a per-test tmp dir."""
    root = tmp_path / "holdings"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_HOLDING_ROOT", str(root))
    return root


@pytest.fixture
def tmp_refdata_root(tmp_path, monkeypatch):
    """Point the refdata Parquet root at a per-test tmp dir."""
    root = tmp_path / "refdata"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_REFDATA_ROOT", str(root))
    return root


@pytest.fixture
def tmp_ohlcv_root(tmp_path, monkeypatch):
    """Point the OHLCV Parquet cache root at a per-test tmp dir."""
    root = tmp_path / "ohlcv"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_OHLCV_ROOT", str(root))
    return root
