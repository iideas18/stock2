"""PortfolioState: the mutable core of the backtest loop.

Tracks cash and shares per code; avg_cost is cost-basis of remaining
shares (simple weighted mean including fees+slippage).

SELL uses FIFO: no separate lot accounting — avg_cost stays constant on
SELL (cost basis of remaining shares unchanged), matching common practice.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

import pandas as pd


@dataclass
class Position:
    code: str
    shares: int
    avg_cost: float


@dataclass
class PortfolioState:
    initial_cash: float
    cash: float = field(init=False)
    positions: Dict[str, Position] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.cash = float(self.initial_cash)

    def apply_trade(
        self, code: str, side: str, shares: int,
        fill_price: float, total_fees: float, slippage_value: float,
    ) -> None:
        gross = shares * fill_price
        if side == "BUY":
            total_out = gross + total_fees + slippage_value
            self.cash -= total_out
            pos = self.positions.get(code)
            if pos is None:
                self.positions[code] = Position(
                    code=code, shares=shares,
                    avg_cost=total_out / shares if shares > 0 else 0.0,
                )
            else:
                new_shares = pos.shares + shares
                new_cost_basis = pos.avg_cost * pos.shares + total_out
                pos.shares = new_shares
                pos.avg_cost = (
                    new_cost_basis / new_shares if new_shares > 0 else 0.0
                )
        elif side == "SELL":
            proceeds = gross - total_fees - slippage_value
            self.cash += proceeds
            pos = self.positions.get(code)
            if pos is None:
                raise ValueError(f"SELL on empty position {code}")
            pos.shares -= shares
            if pos.shares <= 0:
                del self.positions[code]
        else:
            raise ValueError(f"unknown side {side}")

    def snapshot(
        self, at: pd.Timestamp, prices: Dict[str, float],
    ) -> dict:
        position_value = 0.0
        rows = []
        for code, pos in self.positions.items():
            price = prices.get(code, pos.avg_cost)
            mv = pos.shares * price
            position_value += mv
            rows.append({
                "date": at, "code": code,
                "shares": int(pos.shares),
                "avg_cost": float(pos.avg_cost),
                "market_value": float(mv),
                "unrealized_pnl": float(
                    (price - pos.avg_cost) * pos.shares
                ),
            })
        total_value = self.cash + position_value
        for r in rows:
            r["weight"] = (
                r["market_value"] / total_value if total_value > 0 else 0.0
            )
        return {
            "date": at,
            "cash": float(self.cash),
            "position_value": float(position_value),
            "total_value": float(total_value),
            "positions": rows,
            "n_holdings": len(rows),
        }
