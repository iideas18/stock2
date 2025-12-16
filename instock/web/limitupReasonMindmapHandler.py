#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
from tornado import gen
import instock.web.base as webBase
import instock.lib.trade_time as trd
import instock.core.singleton_stock_web_module_data as sswmd


class LimitupReasonMindmapHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        # Default date: latest trade day (same behavior as other pages).
        date = self.get_argument("date", default=None, strip=False)
        if not date:
            run_date, run_date_nph = trd.get_trade_date_last()
            date = run_date_nph.strftime("%Y-%m-%d")

        # Optional: if code is provided, only show that stock.
        code = self.get_argument("code", default=None, strip=False)

        if code:
            sql = """
                SELECT `date`, `code`, `name`, `TITLE`, `reason`
                FROM `cn_stock_limitup_reason`
                WHERE `date` = %s AND `code` = %s
                ORDER BY `TITLE` ASC
            """
            rows = self.db.query(sql, date, code)
        else:
            sql = """
                SELECT `date`, `code`, `name`, `TITLE`, `reason`
                FROM `cn_stock_limitup_reason`
                WHERE `date` = %s
                ORDER BY `name` ASC, `code` ASC
            """
            rows = self.db.query(sql, date)

        def split_reason(text: str):
            if not text:
                return []
            bullets = []
            for part in str(text).replace("\r", "\n").split("\n"):
                part = part.strip()
                if not part:
                    continue
                # split by common Chinese punctuation
                for sub in part.replace("；", "\n").replace("。", "\n").split("\n"):
                    sub = sub.strip()
                    if sub:
                        bullets.append(sub)
            return bullets

        lines = [f"# 涨停原因揭密 {date}"]

        if not rows:
            lines.append("- No data")
        else:
            # Group by (code, name)
            grouped = {}
            for r in rows:
                k = (r.get("code"), r.get("name"))
                grouped.setdefault(k, []).append(r)

            for (c, n), items in grouped.items():
                name = (n or "").strip() or (c or "")
                code_str = (c or "").strip()
                # 代码超链接：跳到指标页（和表格里的代码链接一致）
                indicator_url = f"/instock/data/indicators?code={code_str}&date={date}&name={name}"
                lines.append(f"- [{name}({code_str})]({indicator_url})")

                # Second level: 原因 (TITLE)
                for it in items:
                    title = (it.get("TITLE") or "").strip() or "原因"
                    lines.append(f"  - {title}")

                    # Third level: 详因 (reason)
                    detail = (it.get("reason") or "").strip()
                    bullets = split_reason(detail)
                    if bullets:
                        for b in bullets[:50]:
                            lines.append(f"    - {b}")
                    else:
                        lines.append("    - (空)")

        markdown = "\n".join(lines)

        web_module_data = sswmd.stock_web_module_data().get_data('cn_stock_limitup_reason')
        self.render(
            "limitup_reason_mindmap.html",
            markdown=markdown,
            date_now=date,
            web_module_data=web_module_data,
            leftMenu=webBase.GetLeftMenu(self.request.uri),
        )
