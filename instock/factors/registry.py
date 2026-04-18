from __future__ import annotations
from .base import Factor

_REGISTRY: dict[str, Factor] = {}


def register(factor: Factor) -> None:
    if factor.name in _REGISTRY:
        raise ValueError(f"factor '{factor.name}' already registered")
    _REGISTRY[factor.name] = factor


def get(name: str) -> Factor:
    return _REGISTRY[name]


def get_all() -> dict[str, Factor]:
    return dict(_REGISTRY)


def clear_registry() -> None:
    """Test helper."""
    _REGISTRY.clear()
