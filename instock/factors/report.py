"""Render FactorReport to HTML for the research portal iframe."""
from __future__ import annotations

import os
from pathlib import Path

from instock.factors.evaluator import FactorReport

_TEMPLATE_PATH = Path(__file__).parent / "report_template.html"


def render_factor_report(name: str, report: FactorReport) -> str:
    """Render a FactorReport to HTML using report_template.html."""
    from jinja2 import Template

    tmpl = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))
    return tmpl.render(
        name=name,
        ic_mean=report.ic_mean,
        rank_ic_mean=report.rank_ic_mean,
        ic_ir=report.ic_ir,
        turnover=report.turnover,
        group_returns_html=report.group_returns.to_html(),
    )


def write_factor_report(
    name: str,
    report: FactorReport,
    root: Path | None = None,
) -> Path:
    """Write rendered report to `<root>/<name>.html`. Returns the path.

    Root defaults to `$INSTOCK_FACTOR_REPORTS_ROOT` or `data/factor_reports`.
    """
    if root is None:
        root = Path(os.environ.get(
            "INSTOCK_FACTOR_REPORTS_ROOT", "data/factor_reports"
        ))
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    out = root / f"{name}.html"
    out.write_text(render_factor_report(name, report), encoding="utf-8")
    return out
