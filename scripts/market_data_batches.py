"""冻结 CN 日更名单与交易窗口，分批执行后严格汇总覆盖。"""
from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Mapping
import csv
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, TypedDict, cast

import structlog

from integrations.market_data_review import write_review

PLAN_VERSION = 1
DEFAULT_BATCH_SIZE = 400
MAX_ACTION_BATCHES = 256
READY_STATUSES = {"cache_ready", "gap_repaired", "manifest_cached", "suspended", "not_listed", "excluded"}
REVIEW_STATUSES = READY_STATUSES | {"pending", "failed", "gap_partial", "dry_run_missing", "dry_run_noop"}


class Security(TypedDict):
    code: str
    name: str


class BatchPart(TypedDict):
    id: int
    securities: list[Security]


class BatchPlan(TypedDict):
    version: int
    plan_id: str
    market: str
    target_date: str
    window_start: str
    trade_dates: list[str]
    trading_days: int
    pool_mode: str
    universe_warning: str
    batches: list[BatchPart]


class CoverageSummary(TypedDict):
    market: str
    target_date: str
    update_time: str
    status: str
    pool_mode: str
    counts: dict[str, int]
    total: int
    universe_warning: str


def _plan_digest(plan: Mapping[str, object]) -> str:
    payload = {key: value for key, value in plan.items() if key != "plan_id"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def create_plan(
    items: list[dict[str, str]], *, target_date: date, window_start: date,
    trade_dates: list[date], trading_days: int, batch_size: int = DEFAULT_BATCH_SIZE,
    universe_warning: str = "",
) -> BatchPlan:
    """对一次名单快照排序去重，分为互斥且完整的批次。"""
    if batch_size < 1 or trading_days < 1:
        raise ValueError("批次大小和交易日窗口必须为正数")
    by_code = {item["code"].strip(): {"code": item["code"].strip(), "name": item.get("name", "")} for item in items}
    securities = [by_code[code] for code in sorted(by_code)]
    plan: BatchPlan = {
        "version": PLAN_VERSION, "plan_id": "", "market": "cn",
        "target_date": target_date.isoformat(), "window_start": window_start.isoformat(),
        "trade_dates": [day.isoformat() for day in sorted(set(trade_dates))],
        "trading_days": trading_days, "pool_mode": "all_cn", "universe_warning": universe_warning,
        "batches": [{"id": index, "securities": securities[start:start + batch_size]}
                    for index, start in enumerate(range(0, len(securities), batch_size))],
    }
    plan["plan_id"] = _plan_digest(dict(plan))
    return validate_plan(dict(plan))


def validate_plan(payload: dict[str, Any]) -> BatchPlan:
    """拒绝空名单、重复证券、漂移日期或损坏计划，避免漏拉被报为成功。"""
    if payload.get("version") != PLAN_VERSION or payload.get("market") != "cn":
        raise ValueError("批次计划版本或市场无效")
    if payload.get("plan_id") != _plan_digest(payload):
        raise ValueError("批次计划校验失败")
    target = date.fromisoformat(payload["target_date"])
    start = date.fromisoformat(payload["window_start"])
    days = [date.fromisoformat(day) for day in payload["trade_dates"]]
    if not days or days != sorted(set(days)) or not start <= days[0] <= days[-1] == target:
        raise ValueError("批次计划交易窗口无效")
    if type(payload["trading_days"]) is not int or payload["trading_days"] < 1:
        raise ValueError("批次计划窗口长度无效")
    parts = payload["batches"]
    if not 1 <= len(parts) <= MAX_ACTION_BATCHES:
        raise ValueError("批次数量必须为1到256")
    seen: set[str] = set()
    for index, part in enumerate(parts):
        if type(part["id"]) is not int or part["id"] != index or not part["securities"]:
            raise ValueError("批次编号或证券名单无效")
        for item in part["securities"]:
            code = item["code"]
            if not re.fullmatch(r"[0-9]{6}", code) or code in seen:
                raise ValueError("批次计划包含无效或重复证券")
            seen.add(code)
    return cast(BatchPlan, payload)


def load_plan(path: Path) -> BatchPlan:
    """所有作业读取同一份不可变计划。"""
    return validate_plan(json.loads(path.read_text(encoding="utf-8")))


def merge_batches(plan: BatchPlan, source: Path, output: Path) -> CoverageSummary:
    """按计划核对各批审核文件；缺少的批次保留为pending，不假定成功。"""
    records: dict[str, dict] = {}
    warnings = [plan["universe_warning"]] if plan["universe_warning"] else []
    for part in plan["batches"]:
        folder = source / f"cn-daily-batch-{part['id']}"
        prefix = folder / f"prewarm_cn_{plan['target_date']}"
        coverage = prefix.with_name(prefix.name + "_coverage.csv")
        summary_path = prefix.with_name(prefix.name + "_summary.json")
        metadata_path = folder / "batch_metadata.json"
        expected = {item["code"]: item for item in part["securities"]}
        if not all(path.exists() for path in (coverage, summary_path, metadata_path)):
            warnings.append(f"batch={part['id']}: 缺少完整产物")
            for code, item in expected.items():
                records[code] = {"market": "cn", "symbol": code, "name": item["name"],
                                 "target_date": plan["target_date"], "status": "pending",
                                 "reason": "batch_artifact_missing", "suggested_action": "重跑未完成批次"}
            continue
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("plan_id") != plan["plan_id"] or metadata.get("batch_id") != part["id"]:
            raise ValueError(f"batch={part['id']}: 产物计划不匹配")
        with coverage.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        codes = [row["symbol"] for row in rows]
        if len(codes) != len(set(codes)) or set(codes) != set(expected):
            raise ValueError(f"batch={part['id']}: 覆盖名单不匹配计划")
        if any(row["target_date"] != plan["target_date"] or row["market"] != "cn" or row["status"] not in REVIEW_STATUSES for row in rows):
            raise ValueError(f"batch={part['id']}: 目标日或状态无效")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if (summary.get("market") != "cn" or summary.get("target_date") != plan["target_date"]
                or summary.get("counts") != dict(Counter(row["status"] for row in rows))
                or summary.get("total") != len(rows)):
            raise ValueError(f"batch={part['id']}: 摘要与覆盖记录不一致")
        if summary.get("status") != "ok" or summary.get("universe_warning"):
            warnings.append(f"batch={part['id']}: 批次未完整完成")
        records.update({row["symbol"]: row for row in rows})
    status = "ok" if not warnings and all(row["status"] in READY_STATUSES for row in records.values()) else "partial"
    return cast(CoverageSummary, write_review(output, "cn", plan["target_date"], records, pool_mode=plan["pool_mode"],
                        status=status, final=True, universe_warning="; ".join(warnings)))


def main() -> int:
    """生成批次矩阵，或汇总作业产物并以非零退出表示未完整完成。"""
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("plan")
    prepare.add_argument("--target-date", type=date.fromisoformat, required=True)
    prepare.add_argument("--trading-days", type=int, default=320)
    prepare.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    prepare.add_argument("--output", type=Path, required=True)
    merge = sub.add_parser("merge")
    merge.add_argument("--plan", type=Path, required=True)
    merge.add_argument("--source", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    logger = structlog.get_logger()
    if args.command == "merge":
        summary = merge_batches(load_plan(args.plan), args.source, args.output)
        logger.info("market_data_batches_merged", **summary)
        return 0 if summary["status"] == "ok" else 2

    # 名单、交易窗口仅加载一次；批次不再自行重取名单或按当天计算目标日。
    from scripts.funnel_prewarm import _load_cn_universe, _resolve_trading_window, _expected_trade_dates
    from scripts.prewarm_workers import bounded_call

    items, warnings = bounded_call(_load_cn_universe, timeout=180)
    window = bounded_call(_resolve_trading_window, end_calendar_day=args.target_date, trading_days=args.trading_days, timeout=180)
    dates = bounded_call(_expected_trade_dates, window, "cn", timeout=180)
    plan = create_plan(items, target_date=window.end_trade_date, window_start=window.start_trade_date,
                       trade_dates=dates, trading_days=args.trading_days, batch_size=args.batch_size,
                       universe_warning="; ".join(warnings))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    matrix = json.dumps({"batch_id": [part["id"] for part in plan["batches"]]})
    if output := os.getenv("GITHUB_OUTPUT"):
        with Path(output).open("a", encoding="utf-8") as stream:
            stream.write(f"matrix={matrix}\ntarget_date={plan['target_date']}\n")
    logger.info("market_data_batches_planned", plan_id=plan["plan_id"], target_date=plan["target_date"],
                batch_count=len(plan["batches"]), batch_size=args.batch_size,
                total=sum(len(part["securities"]) for part in plan["batches"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
