"""BacktestConfig: single source of truth for run parameters.

Serialized into fingerprint input so config changes propagate to run_id.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from typing import Tuple


@dataclass(frozen=True)
class BacktestConfig:
    strategy: str
    start: date
    end: date

    initial_capital: float = 1_000_000.0
    lot_size: int = 100
    price_adjust: str = "qfq"

    # Fees
    enable_fees: bool = True
    commission_rate: float = 0.00025
    commission_min: float = 5.0
    stamp_tax_rate: float = 0.0005          # SELL only
    transfer_fee_rate: float = 0.00001

    # Slippage
    enable_slippage: bool = True
    slippage_bps: float = 5.0

    # Benchmarks (order preserved for report)
    benchmarks: Tuple[str, ...] = (
        "000300.SH", "000905.SH", "000852.SH"
    )

    # Reproducibility
    rng_seed: int = 42

    # Constraint tuning
    limit_tolerance: float = 1e-3
    max_suspended_defer_days: int = 5

    def to_dict(self) -> dict:
        d = asdict(self)
        d["start"] = self.start.isoformat()
        d["end"] = self.end.isoformat()
        d["benchmarks"] = list(self.benchmarks)
        return d
