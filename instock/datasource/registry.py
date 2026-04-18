"""DataSource factory. First phase registers only AkShareSource."""
from __future__ import annotations

from functools import lru_cache

from .akshare_source import AkShareSource
from .base import IDataSource


_REGISTRY = {"akshare": AkShareSource}


@lru_cache(maxsize=None)
def get_source(name: str = "akshare") -> IDataSource:
    if name not in _REGISTRY:
        raise ValueError(
            f"unknown data source '{name}'. Registered: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]()
