"""市场日线任务的覆盖、异常与人工审核产物。"""

from __future__ import annotations

from collections import Counter
import csv
from datetime import datetime
from io import StringIO
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

REVIEW_COLUMNS = ("market", "symbol", "name", "status", "target_date", "latest_date", "listing_date", "missing_ranges", "reason", "suggested_action", "evidence", "detail")


def review_reason(detail: str) -> tuple[str, str]:
    """区分网络、配置和空行情；疑似退市必须保留人工确认。"""
    text = detail.lower()
    if any(word in text for word in ("网络", "reset", "timeout", "断连", "circuit", "consecutive_failures")):
        return "source_or_network_error", "检查上游网络/熔断状态后重跑；不要据此排除股票"
    if "api_key_missing" in text:
        return "empty_data_with_unconfigured_fallback", "核对上市日、停牌/退市公告，并检查备用源配置"
    return "missing_or_invalid_data", "核对上市日、停牌/退市、代码变更与数据源；确认后再维护排除清单"


def _safe_cell(value: object) -> str:
    text = str(value or "")
    text = re.sub(r"(?i)((?:api[_-]?key|token|password|authorization)\s*[=:]\s*)[^\s;；]+", r"\1[REDACTED]", text)
    return "'" + text if text.startswith(("=", "+", "-", "@")) else text


def write_review(directory: Path, market: str, target: str, records: dict[str, dict], *, pool_mode: str, status: str, final: bool = False, universe_warning: str = "") -> dict:
    """原子保存全覆盖CSV、待审核CSV与摘要，超时前保留最近检查点。"""
    directory.mkdir(parents=True, exist_ok=True)
    rows = [records[key] for key in sorted(records)]
    counts = dict(Counter(row["status"] for row in rows))
    summary = {"market": market, "target_date": target, "update_time": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
               "status": status, "pool_mode": pool_mode, "counts": counts, "total": len(rows), "universe_warning": universe_warning}
    prefix = directory / f"prewarm_{market}_{target}"
    for suffix, selected in (("coverage", rows), ("review", [row for row in rows if row["status"] in {"failed", "gap_partial", "pending", "dry_run_missing"}])):
        stream = StringIO()
        writer = csv.DictWriter(stream, fieldnames=REVIEW_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({key: _safe_cell(row.get(key)) for key in REVIEW_COLUMNS} for row in selected)
        destination = prefix.with_name(prefix.name + f"_{suffix}.csv")
        temp = destination.with_suffix(".tmp")
        temp.write_text(stream.getvalue(), encoding="utf-8-sig")
        temp.replace(destination)
    destination = prefix.with_name(prefix.name + "_summary.json")
    temp = destination.with_suffix(".tmp")
    temp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(destination)
    if final and os.getenv("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as out:
            out.write(f"\n### {market.upper()} 日线覆盖 · {target}\n\n状态：{status}；股票池：{pool_mode}；总数：{len(rows)}\n\n")
            if universe_warning:
                out.write(f"\n**名单警告**：{universe_warning}\n\n")
            out.write("| 状态 | 数量 |\n|---|---:|\n" + "".join(f"| {key} | {value} |\n" for key, value in sorted(counts.items())))
            out.write("\n下载本次Artifacts中的 `*_review.csv` 人工维护；`*_coverage.csv` 包含排除与未完成项。\n")
    return summary
