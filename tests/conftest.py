import os
import tempfile
import pytest


@pytest.fixture
def tmp_factor_root(tmp_path, monkeypatch):
    """将 Parquet 因子根目录临时指向 tmp_path 子目录。"""
    root = tmp_path / "factors"
    root.mkdir()
    monkeypatch.setenv("INSTOCK_FACTOR_ROOT", str(root))
    return root
