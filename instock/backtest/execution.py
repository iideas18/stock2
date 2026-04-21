"""Executor: turn Order objects into TradeRecord dicts + mutate portfolio.

Order contract:
  BUY:  target_value = cash to spend (upper bound); shares = floor(value / fill_price)
         then floor to lot_size. Residual cash stays in portfolio (lot drag).
  SELL: target_value = value to sell (gross); shares = floor(value / fill_price)
         then floor to lot_size, capped by held shares.

Executor does NOT check constraints — that is the engine's job before it
even creates the Order.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .costs import FeeModel, SlippageModel
from .portfolio_state import PortfolioState


@dataclass
class Order:
    code: str
    side: str           # "BUY" or "SELL"
    target_value: float
    target_w: float     # for trade record audit


class Executor:
    def __init__(
        self,
        fee_model: FeeModel,
        slippage_model: SlippageModel,
        lot_size: int = 100,
    ) -> None:
        self.fee_model = fee_model
        self.slippage_model = slippage_model
        self.lot_size = lot_size

    def execute(
        self,
        order: Order,
        row_today: dict,
        portfolio: PortfolioState,
        at: pd.Timestamp,
        reason: str,
    ) -> dict:
        open_price = float(row_today["open"])
        fill_price = self.slippage_model.fill_price(open_price, order.side)

        if fill_price <= 0:
            return self._noop_trade(order, at, fill_price, reason)

        raw_shares = int(order.target_value // fill_price)
        lots = raw_shares // self.lot_size
        shares = lots * self.lot_size

        if order.side == "SELL":
            held = portfolio.positions.get(order.code)
            if held is not None:
                shares = min(shares, held.shares)
                if held.shares - shares < self.lot_size:
                    shares = held.shares
            else:
                shares = 0

        if shares <= 0:
            return self._noop_trade(order, at, fill_price, reason)

        gross = shares * fill_price
        fees = self.fee_model.compute(value=gross, side=order.side)
        total_fees = fees["commission"] + fees["stamp_tax"] + fees["transfer_fee"]
        slippage_value = abs(fill_price - open_price) * shares

        portfolio.apply_trade(
            code=order.code, side=order.side, shares=shares,
            fill_price=fill_price, total_fees=total_fees,
            slippage_value=slippage_value,
        )

        net = gross + total_fees + slippage_value
        net_cash_change = -net if order.side == "BUY" else (gross - total_fees - slippage_value)
        return {
            "date": at, "code": order.code, "side": order.side,
            "target_w": float(order.target_w),
            "filled_shares": int(shares),
            "fill_price": float(fill_price),
            "fill_value": float(gross),
            "commission": float(fees["commission"]),
            "stamp_tax": float(fees["stamp_tax"]),
            "transfer_fee": float(fees["transfer_fee"]),
            "slippage_value": float(slippage_value),
            "gross_value": float(net),
            "net_cash_change": float(net_cash_change),
            "reason": reason,
        }

    @staticmethod
    def _noop_trade(order: Order, at: pd.Timestamp,
                    fill_price: float, reason: str) -> dict:
        return {
            "date": at, "code": order.code, "side": order.side,
            "target_w": float(order.target_w),
            "filled_shares": 0,
            "fill_price": float(max(fill_price, 0.0)),
            "fill_value": 0.0, "commission": 0.0,
            "stamp_tax": 0.0, "transfer_fee": 0.0,
            "slippage_value": 0.0, "gross_value": 0.0,
            "net_cash_change": 0.0, "reason": "NO_OP",
        }
