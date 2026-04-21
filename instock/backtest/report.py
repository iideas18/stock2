"""HTML report rendering.

Renders NAV + drawdown charts to base64-PNG and stuffs into Jinja2
template. Tolerates missing trades/benchmarks.
"""
from __future__ import annotations

import base64
import io
from pathlib import Path
from typing import Optional

import pandas as pd

_TEMPLATE_PATH = Path(__file__).parent / "template.html"


def _chart_png_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
    import matplotlib.pyplot as plt
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _nav_chart(nav: pd.DataFrame, benchmarks: pd.DataFrame) -> str:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(nav["date"], nav["nav"], label="strategy", linewidth=1.5)
    if not benchmarks.empty:
        start_vals = benchmarks.iloc[0]
        for col in benchmarks.columns:
            ax.plot(
                benchmarks.index, benchmarks[col] / start_vals[col],
                label=col, linewidth=1.0, alpha=0.7,
            )
    ax.set_title("NAV vs Benchmarks")
    ax.legend(loc="best")
    ax.grid(alpha=0.3)
    return _chart_png_b64(fig)


def _drawdown_chart(nav: pd.DataFrame) -> str:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    dd = nav["nav"] / nav["nav"].cummax() - 1.0
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.fill_between(nav["date"], dd, 0, color="red", alpha=0.3)
    ax.set_title("Drawdown")
    ax.grid(alpha=0.3)
    return _chart_png_b64(fig)


def render_report(
    run_id: str,
    nav: pd.DataFrame,
    trades: pd.DataFrame,
    metrics: dict,
    benchmarks: pd.DataFrame,
    refdata_as_of: Optional[str],
) -> str:
    from jinja2 import Template
    tmpl = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))
    nav_b64 = _nav_chart(nav, benchmarks) if not nav.empty else ""
    dd_b64 = _drawdown_chart(nav) if not nav.empty else ""
    return tmpl.render(
        run_id=run_id,
        metrics=metrics,
        nav_chart=nav_b64,
        dd_chart=dd_b64,
        refdata_as_of=refdata_as_of,
        fingerprint_prefix=str(metrics.get("fingerprint_sha", ""))[:16],
    )
