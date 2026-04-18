"""Portfolio-level constraints applied after weighing.

MVP:
  - MaxWeightConstraint: iterative clip + proportional redistribution
  - IndustryExposureConstraint: no-op if industry_map is None, else
    per-industry proportional scaling
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

log = logging.getLogger(__name__)


@dataclass
class ConstraintContext:
    industry_map: dict[str, str] | None = None


class PortfolioConstraint(ABC):
    @abstractmethod
    def apply(
        self, weights: pd.Series, context: ConstraintContext
    ) -> pd.Series:
        """Return new weights. Must still sum to ~1.0."""


class MaxWeightConstraint(PortfolioConstraint):
    """Cap every name at `max_weight`, redistribute overflow to unsaturated
    names in proportion to their current weights. Iterates to fixed point.

    Raises ValueError if n * max_weight < 1.0 (infeasible).
    """

    def __init__(self, max_weight: float, max_iter: int = 100) -> None:
        if not (0.0 < max_weight <= 1.0):
            raise ValueError(f"max_weight must be in (0,1], got {max_weight}")
        self.max_weight = max_weight
        self.max_iter = max_iter

    def apply(self, weights, context):
        if weights.empty:
            return weights
        if len(weights) * self.max_weight < 1.0 - 1e-9:
            raise ValueError(
                f"infeasible: {len(weights)} names * "
                f"max_weight {self.max_weight} < 1.0"
            )

        w = weights.copy().astype(float)
        for _ in range(self.max_iter):
            over = w > self.max_weight + 1e-12
            if not over.any():
                break
            overflow = (w[over] - self.max_weight).sum()
            w[over] = self.max_weight
            unsat = ~over & (w < self.max_weight - 1e-12)
            if not unsat.any():
                break
            w.loc[unsat] += overflow * (
                w.loc[unsat] / w.loc[unsat].sum()
            )
        return w


class IndustryExposureConstraint(PortfolioConstraint):
    """Cap per-industry total weight; scale offending industries down and
    redistribute freed weight pro-rata to non-capped industries.

    If industry_map is None (or ConstraintContext.industry_map is None),
    this is a no-op and emits a warning once per instance.
    """

    def __init__(
        self,
        max_industry_weight: float,
        industry_map: dict[str, str] | None = None,
    ) -> None:
        if not (0.0 < max_industry_weight <= 1.0):
            raise ValueError(
                f"max_industry_weight must be in (0,1], got {max_industry_weight}"
            )
        self.max_industry_weight = max_industry_weight
        self.industry_map = industry_map
        self._warned = False

    def apply(self, weights, context):
        imap = context.industry_map or self.industry_map
        if imap is None:
            if not self._warned:
                log.warning(
                    "IndustryExposureConstraint skipped: industry_map is None"
                )
                self._warned = True
            return weights
        if weights.empty:
            return weights

        industries = pd.Series(
            {c: imap.get(c, "_unknown") for c in weights.index}
        )
        w = weights.copy().astype(float)
        ind_totals = w.groupby(industries).sum()
        over = ind_totals[ind_totals > self.max_industry_weight + 1e-12]
        if over.empty:
            return w

        freed = 0.0
        for ind, total in over.items():
            codes_in_ind = industries[industries == ind].index
            scale = self.max_industry_weight / total
            freed += w.loc[codes_in_ind].sum() * (1.0 - scale)
            w.loc[codes_in_ind] *= scale

        capped_ind = set(over.index)
        non_capped = w.index[~industries.isin(capped_ind)]
        if len(non_capped) > 0 and w.loc[non_capped].sum() > 0:
            w.loc[non_capped] += freed * (
                w.loc[non_capped] / w.loc[non_capped].sum()
            )
        return w
