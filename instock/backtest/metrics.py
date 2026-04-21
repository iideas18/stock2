"""Performance metrics.

Standard quant formulas; 252 trading days/year. All return dicts are
flat (not nested) so they can go straight into METRICS_SCHEMA.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

_TD_PER_YEAR = 252


def annualize_return(rets: pd.Series) -> float:
    if rets.empty:
        return 0.0
    total = (1 + rets).prod()
    n = len(rets)
    return float(total ** (_TD_PER_YEAR / n) - 1)


def annualize_volatility(rets: pd.Series) -> float:
    if len(rets) < 2:
        return 0.0
    v = float(rets.std(ddof=1) * np.sqrt(_TD_PER_YEAR))
    return 0.0 if v < 1e-12 else v


def sharpe_ratio(rets: pd.Series, rf: float = 0.0) -> float:
    vol = annualize_volatility(rets)
    if vol == 0.0:
        return float("nan")
    excess = rets - rf / _TD_PER_YEAR
    return float(excess.mean() / rets.std(ddof=1) * np.sqrt(_TD_PER_YEAR))


def sortino_ratio(rets: pd.Series, rf: float = 0.0) -> float:
    downside = rets[rets < 0]
    if len(downside) < 2:
        return float("nan")
    dd_std = downside.std(ddof=1) * np.sqrt(_TD_PER_YEAR)
    if dd_std == 0.0:
        return float("nan")
    return float((rets.mean() - rf / _TD_PER_YEAR) / dd_std * _TD_PER_YEAR)


def max_drawdown(nav: pd.Series) -> tuple[float, int]:
    """Return (max_dd, duration_days). max_dd <= 0."""
    if nav.empty:
        return 0.0, 0
    peaks = nav.cummax()
    dd = nav / peaks - 1.0
    mdd = float(dd.min())
    if mdd == 0.0:
        return 0.0, 0
    end = int(dd.idxmin())
    peak_idx = int(nav.iloc[:end + 1].idxmax())
    return mdd, end - peak_idx


def _alpha_beta_ir(rets: pd.Series, bench: pd.Series):
    df = pd.concat([rets, bench], axis=1).dropna()
    if len(df) < 2:
        return float("nan"), float("nan"), float("nan")
    r, b = df.iloc[:, 0], df.iloc[:, 1]
    cov = np.cov(r, b, ddof=1)
    beta = cov[0, 1] / cov[1, 1] if cov[1, 1] > 0 else float("nan")
    alpha_daily = r.mean() - beta * b.mean()
    alpha = alpha_daily * _TD_PER_YEAR
    active = r - b
    ir = (
        active.mean() / active.std(ddof=1) * np.sqrt(_TD_PER_YEAR)
        if active.std(ddof=1) > 0 else float("nan")
    )
    return float(alpha), float(beta), float(ir)


def compute_metrics(
    nav_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    benchmarks_df: pd.DataFrame,
) -> dict:
    """Return a flat dict for METRICS_SCHEMA."""
    if nav_df.empty:
        return {}
    nav_df = nav_df.sort_values("date").reset_index(drop=True)
    rets = nav_df["ret_daily"]
    nav = nav_df["nav"]

    ret_total = float(nav.iloc[-1] - 1.0)
    ret_ann = annualize_return(rets)
    vol_ann = annualize_volatility(rets)
    sr = sharpe_ratio(rets)
    sor = sortino_ratio(rets)
    mdd, mdd_dur = max_drawdown(nav)
    calmar = ret_ann / abs(mdd) if mdd < 0 else float("nan")

    win_daily = float((rets > 0).sum() / max(len(rets), 1))
    monthly = (
        nav_df.assign(m=nav_df["date"].dt.to_period("M"))
        .groupby("m")["ret_daily"].apply(lambda s: (1 + s).prod() - 1)
    )
    win_monthly = (
        float((monthly > 0).sum() / len(monthly))
        if len(monthly) else float("nan")
    )

    turnover_ann = (
        float(nav_df["turnover_daily"].sum() * _TD_PER_YEAR / max(len(nav_df), 1))
    )

    if not trades_df.empty:
        total_cost = float(
            (trades_df["commission"] + trades_df["stamp_tax"]
             + trades_df["transfer_fee"]
             + trades_df["slippage_value"]).sum()
        )
        total_trade_value = float(trades_df["fill_value"].sum())
        total_cost_bps = (
            total_cost / total_trade_value * 1e4
            if total_trade_value > 0 else 0.0
        )
    else:
        total_cost_bps = 0.0

    bench_stats = {}
    if not benchmarks_df.empty:
        bench_rets = benchmarks_df.pct_change().iloc[1:]
        nav_rets_indexed = rets.copy()
        nav_rets_indexed.index = nav_df["date"]
        for col in bench_rets.columns:
            a, b, ir = _alpha_beta_ir(nav_rets_indexed, bench_rets[col])
            bench_stats[f"alpha_vs_{col}"] = a
            bench_stats[f"beta_vs_{col}"] = b
            bench_stats[f"ir_vs_{col}"] = ir

    out = {
        "ret_annual": ret_ann, "ret_total": ret_total,
        "vol_annual": vol_ann, "sharpe": sr, "sortino": sor,
        "max_drawdown": mdd, "max_dd_duration_days": int(mdd_dur),
        "calmar": calmar,
        "win_rate_daily": win_daily, "win_rate_monthly": win_monthly,
        "turnover_annual": turnover_ann,
        "total_cost_bps": total_cost_bps,
        "lot_drag_bps": 0.0,
    }
    out.update(bench_stats)
    return out
