"""FeeModel + SlippageModel.

FeeModel: commission (min floored), A-share stamp tax (sell only),
transfer fee (simplified: applied all markets).

SlippageModel: bps proportional drift on fill price; side-signed.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class FeeModel(ABC):
    @abstractmethod
    def compute(self, value: float, side: str) -> dict:
        """Return {commission, stamp_tax, transfer_fee}, all >= 0."""


class StandardFeeModel(FeeModel):
    def __init__(
        self,
        commission_rate: float = 0.00025,
        commission_min: float = 5.0,
        stamp_tax_rate: float = 0.0005,
        transfer_fee_rate: float = 0.00001,
    ) -> None:
        self.commission_rate = commission_rate
        self.commission_min = commission_min
        self.stamp_tax_rate = stamp_tax_rate
        self.transfer_fee_rate = transfer_fee_rate

    def compute(self, value: float, side: str) -> dict:
        commission = max(value * self.commission_rate, self.commission_min)
        stamp_tax = value * self.stamp_tax_rate if side == "SELL" else 0.0
        transfer_fee = value * self.transfer_fee_rate
        return {
            "commission": float(commission),
            "stamp_tax": float(stamp_tax),
            "transfer_fee": float(transfer_fee),
        }


class ZeroFeeModel(FeeModel):
    def compute(self, value: float, side: str) -> dict:
        return {"commission": 0.0, "stamp_tax": 0.0, "transfer_fee": 0.0}
