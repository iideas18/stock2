"""BacktestEngine: event-driven daily loop over trade_calendar.

Loop per day d:
  1. Apply pending orders scheduled for d (from d-1 rebalance):
     - Constraint check: suspended/limit/delist
     - On block -> defer to next trading day unless max_defer exceeded
     - On pass -> Executor.execute mutates portfolio
  2. Mark-to-market on close, emit NAV row.
  3. If d appears in holding_schedule: compute orders (deltas), schedule
     them for next trade day.

T+1 semantics: holding[d] produces orders that execute at d+1 open.
"""
from __future__ import annotations

import logging
from collections import defaultdict

import pandas as pd

from instock.datasource.base import IDataSource
from instock.refdata.ohlcv_store import OhlcvPanelStore
from instock.refdata import st as refdata_st
from instock.refdata import listing as refdata_listing
from instock.refdata.schemas import RefdataNotAvailable

from .config import BacktestConfig
from .constraints import (
    ConstraintChain, DelistConstraint, LimitDownConstraint,
    LimitUpConstraint, SuspendedConstraint,
)
from .costs import (
    BpsSlippage, StandardFeeModel, ZeroFeeModel, ZeroSlippage,
)
from .execution import Executor, Order
from .portfolio_state import PortfolioState

log = logging.getLogger(__name__)


class BacktestEngine:
    def __init__(
        self,
        source: IDataSource,
        ohlcv_store: OhlcvPanelStore,
    ) -> None:
        self.source = source
        self.ohlcv_store = ohlcv_store

    def run(
        self, holding_schedule: pd.DataFrame, config: BacktestConfig,
    ) -> dict:
        cal_dates = [
            pd.Timestamp(d) for d in self.source.get_trade_calendar(
                config.start, config.end
            )
        ]
        if not cal_dates:
            return self._empty_result(config)

        codes = (
            sorted(holding_schedule["code"].unique().tolist())
            if not holding_schedule.empty else []
        )

        panel = (
            self.ohlcv_store.get_panel(
                codes, config.start, config.end,
                adjust=config.price_adjust,
            ) if codes else pd.DataFrame(columns=[
                "date", "code", "open", "high", "low", "close",
                "volume", "amount"])
        )
        panel_by_day = {
            pd.Timestamp(ts): grp.set_index("code").to_dict("index")
            for ts, grp in panel.groupby("date")
        }

        try:
            listing_dates = refdata_listing.read_listing_dates()
        except RefdataNotAvailable:
            listing_dates = None
        try:
            st_flags = refdata_st.read_st_flags(config.end)
        except RefdataNotAvailable:
            st_flags = set()

        portfolio = PortfolioState(initial_cash=config.initial_capital)
        fee_model = (
            StandardFeeModel(
                commission_rate=config.commission_rate,
                commission_min=config.commission_min,
                stamp_tax_rate=config.stamp_tax_rate,
                transfer_fee_rate=config.transfer_fee_rate,
            ) if config.enable_fees else ZeroFeeModel()
        )
        slip_model = (
            BpsSlippage(bps=config.slippage_bps)
            if config.enable_slippage else ZeroSlippage()
        )
        executor = Executor(
            fee_model=fee_model, slippage_model=slip_model,
            lot_size=config.lot_size,
        )
        chain = ConstraintChain([
            SuspendedConstraint(),
            LimitUpConstraint(tolerance=config.limit_tolerance),
            LimitDownConstraint(tolerance=config.limit_tolerance),
            DelistConstraint(),
        ])

        holding_dates = set()
        holding_by_day: dict = {}
        if not holding_schedule.empty:
            for ts, grp in holding_schedule.groupby("date"):
                holding_dates.add(pd.Timestamp(ts))
                holding_by_day[pd.Timestamp(ts)] = grp.set_index("code")[
                    "weight"
                ].to_dict()

        pending: dict = defaultdict(list)
        trades: list = []
        positions: list = []
        nav_rows: list = []
        prev_total_value = config.initial_capital

        for i, d in enumerate(cal_dates):
            day_orders = pending.pop(d, [])
            panel_today = panel_by_day.get(d, {})
            panel_prev = panel_by_day.get(
                cal_dates[i - 1], {}
            ) if i > 0 else {}
            for order, defer_count in day_orders:
                row_today = panel_today.get(order.code)
                row_prev = panel_prev.get(order.code, {"close": 0.0})
                delisted = (
                    row_today is None or
                    (listing_dates is not None
                     and order.code not in listing_dates)
                )
                if row_today is None:
                    defer_count += 1
                    if defer_count > config.max_suspended_defer_days:
                        trades.append(executor._noop_trade(
                            order, d, 0.0, "NO_OP"
                        ))
                        continue
                    next_day = cal_dates[i + 1] if i + 1 < len(cal_dates) else None
                    if next_day is not None:
                        pending[next_day].append((order, defer_count))
                    continue
                is_st = order.code in (st_flags or set())
                ok, reason = chain.check(
                    code=order.code, is_st=is_st, side=order.side,
                    row_today=row_today, row_yesterday=row_prev,
                    delisted=delisted,
                )
                if not ok:
                    defer_count += 1
                    if defer_count > config.max_suspended_defer_days:
                        trades.append(executor._noop_trade(
                            order, d, row_today.get("open", 0.0),
                            reason or "NO_OP",
                        ))
                        continue
                    next_day = cal_dates[i + 1] if i + 1 < len(cal_dates) else None
                    if next_day is not None:
                        pending[next_day].append((order, defer_count))
                    continue
                trade = executor.execute(
                    order=order, row_today=row_today,
                    portfolio=portfolio, at=d,
                    reason=reason or "REBALANCE",
                )
                trades.append(trade)

            close_prices = {
                c: r["close"] for c, r in panel_today.items()
            }
            snap = portfolio.snapshot(d, close_prices)
            total = snap["total_value"]
            ret_daily = (
                total / prev_total_value - 1.0 if prev_total_value > 0 else 0.0
            )
            nav_rows.append({
                "date": d,
                "nav": total / config.initial_capital,
                "cash": snap["cash"],
                "position_value": snap["position_value"],
                "total_value": total,
                "ret_daily": ret_daily,
                "ret_cum": total / config.initial_capital - 1.0,
                "turnover_daily": sum(
                    abs(t["net_cash_change"]) for t in trades
                    if t["date"] == d
                ) / max(total, 1.0),
                "n_holdings": snap["n_holdings"],
            })
            positions.extend(snap["positions"])
            prev_total_value = total

            if d in holding_dates:
                target_weights = holding_by_day[d]
                orders = self._compute_orders(
                    portfolio=portfolio,
                    target_weights=target_weights,
                    close_prices=close_prices,
                    total_value=total,
                )
                next_day = cal_dates[i + 1] if i + 1 < len(cal_dates) else None
                if next_day is not None:
                    for o in orders:
                        pending[next_day].append((o, 0))

        return {
            "trades": pd.DataFrame(trades),
            "positions": pd.DataFrame(positions),
            "nav": pd.DataFrame(nav_rows),
        }

    @staticmethod
    def _compute_orders(
        portfolio: PortfolioState,
        target_weights: dict,
        close_prices: dict,
        total_value: float,
    ) -> list:
        current = {
            c: pos.shares * close_prices.get(c, pos.avg_cost)
            for c, pos in portfolio.positions.items()
        }
        codes = set(current) | set(target_weights)
        orders_sell = []
        orders_buy = []
        for c in sorted(codes):
            tgt_val = target_weights.get(c, 0.0) * total_value
            cur_val = current.get(c, 0.0)
            delta = tgt_val - cur_val
            if delta < 0:
                orders_sell.append(Order(
                    code=c, side="SELL",
                    target_value=abs(delta),
                    target_w=target_weights.get(c, 0.0),
                ))
            elif delta > 0:
                orders_buy.append(Order(
                    code=c, side="BUY",
                    target_value=delta,
                    target_w=target_weights.get(c, 0.0),
                ))
        return orders_sell + orders_buy

    @staticmethod
    def _empty_result(config: BacktestConfig) -> dict:
        return {
            "trades": pd.DataFrame(),
            "positions": pd.DataFrame(),
            "nav": pd.DataFrame(),
        }
