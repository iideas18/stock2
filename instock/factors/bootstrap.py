"""Idempotent default-factor registration helper.

Centralises knowledge of which factor classes ship in MVP so both
Sub-1's factor_compute_daily_job and Sub-2's generate_holdings_daily_job
share the same registration path (fixes the silent-no-op bug in which
side-effect imports were relied on but no module called register()).
"""
from __future__ import annotations

from . import registry

_REGISTERED = False


def register_default_factors() -> None:
    """Register the six MVP factors. Safe to call multiple times."""
    global _REGISTERED
    if _REGISTERED:
        return
    from .technical.momentum import MomentumFactor
    from .fundamental.valuation import PEFactor, PBFactor, ROEFactor
    from .lhb.heat import LhbHeatFactor
    from .flow.north import NorthHoldingChgFactor
    for cls in (
        MomentumFactor,
        PEFactor,
        PBFactor,
        ROEFactor,
        LhbHeatFactor,
        NorthHoldingChgFactor,
    ):
        try:
            registry.register(cls())
        except ValueError:
            # Already registered (test isolation etc.).
            pass
    _REGISTERED = True
