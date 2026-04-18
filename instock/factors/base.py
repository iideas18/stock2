from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
import pandas as pd


class Factor(ABC):
    name: str
    category: str                # technical | fundamental | lhb | flow
    description: str
    frequency: str = "daily"
    universe: str = "all_a"
    dependencies: list[str] = []

    @abstractmethod
    def compute(
        self, universe: list[str], start: date, end: date
    ) -> pd.DataFrame:
        """Return long-format DataFrame with columns: date, code, value."""

    def preprocess(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Default preprocess: winsorize -> industry/mcap neutralize -> zscore.
        Subclasses may override. Imported lazily to avoid cycles."""
        from . import preprocess
        return preprocess.default_pipeline(raw)
