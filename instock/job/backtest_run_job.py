"""CLI entry for Sub-3 backtest run.

    python -m instock.job.backtest_run_job \
        --strategy default_topq \
        --start 2022-01-04 --end 2023-12-29 \
        --benchmarks 000300.SH,000905.SH,000852.SH \
        --report
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from typing import Optional, Sequence

import pandas as pd

from instock.backtest.benchmarks import load_benchmarks
from instock.backtest.config import BacktestConfig
from instock.backtest.engine import BacktestEngine
from instock.backtest.fingerprint import compute_fingerprint
from instock.backtest.metrics import compute_metrics
from instock.backtest.report import render_report
from instock.backtest import storage as bt_storage

log = logging.getLogger(__name__)


def run_once(
    *,
    strategy: str,
    start: date,
    end: date,
    source=None,
    store=None,
    holding: Optional[pd.DataFrame] = None,
    benchmarks: Sequence[str] = (
        "000300.SH", "000905.SH", "000852.SH"
    ),
    enable_fees: bool = True,
    enable_slippage: bool = True,
    slippage_bps: float = 5.0,
    write_report: bool = True,
) -> str:
    """Run one backtest; return run_id."""
    if source is None:
        from instock.datasource.registry import get_source
        source = get_source()
    if store is None:
        from instock.refdata.ohlcv_store import OhlcvPanelStore
        store = OhlcvPanelStore(source=source)
    if holding is None:
        from instock.portfolio.storage import read_holding
        holding = read_holding(
            strategy, pd.Timestamp(start), pd.Timestamp(end)
        )

    cfg = BacktestConfig(
        strategy=strategy, start=start, end=end,
        enable_fees=enable_fees, enable_slippage=enable_slippage,
        slippage_bps=slippage_bps,
        benchmarks=tuple(benchmarks),
    )

    fingerprint = compute_fingerprint(
        input_files=[],
        config_dict=cfg.to_dict(),
    )
    run_id = (
        f"{strategy}_{start.isoformat()}_{end.isoformat()}_{fingerprint[:8]}"
    )

    engine = BacktestEngine(source=source, ohlcv_store=store)
    result = engine.run(holding_schedule=holding, config=cfg)

    bench_df = load_benchmarks(source, list(benchmarks), start, end)
    metrics = compute_metrics(
        nav_df=result["nav"], trades_df=result["trades"],
        benchmarks_df=bench_df,
    )
    metrics.update({
        "run_id": run_id, "strategy": strategy,
        "start": pd.Timestamp(start), "end": pd.Timestamp(end),
        "fingerprint_sha": fingerprint,
        "refdata_as_of": datetime.now().strftime("%Y-%m-%d"),
    })
    bt_storage.write_run(
        run_id=run_id, trades=result["trades"],
        positions=result["positions"], nav=result["nav"],
    )
    bt_storage.append_metrics(metrics)

    if write_report:
        html = render_report(
            run_id=run_id, nav=result["nav"],
            trades=result["trades"], metrics=metrics,
            benchmarks=bench_df,
            refdata_as_of=metrics["refdata_as_of"],
        )
        (bt_storage._run_dir(run_id) / "report.html").write_text(
            html, encoding="utf-8",
        )
    return run_id


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> None:
    p = argparse.ArgumentParser(description="Sub-3 backtest runner")
    p.add_argument("--strategy", required=True)
    p.add_argument("--start", required=True, type=_parse_date)
    p.add_argument("--end", required=True, type=_parse_date)
    p.add_argument("--benchmarks", default="000300.SH,000905.SH,000852.SH")
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--no-fees", action="store_true")
    p.add_argument("--no-slippage", action="store_true")
    p.add_argument("--report", action="store_true")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO)
    run_id = run_once(
        strategy=args.strategy,
        start=args.start, end=args.end,
        benchmarks=tuple(args.benchmarks.split(",")),
        enable_fees=not args.no_fees,
        enable_slippage=not args.no_slippage,
        slippage_bps=args.slippage_bps,
        write_report=args.report,
    )
    print(run_id)


if __name__ == "__main__":
    main()
