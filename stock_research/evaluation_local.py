"""CLI和worker共用受控文件边界，不加载身份或数据库配置。"""

import json
import os
from collections.abc import Callable
from pathlib import Path

from core.trade_friction import round_trip_cost_pct, slippage_bps_per_side
from stock_research.artifacts import data_lock, write_once
from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.data import read_market
from stock_research.evaluation import EVALUATION_HISTORY, EvaluationInput, EvaluationReport, evaluate_history
from stock_research.identity import digest
from stock_research.models import ClassificationVersion, UniverseVersion
from stock_research.schemas.evaluation_execution import (
    MAX_EVALUATION_INPUT,
    MAX_EVALUATION_ROWS,
    MAX_EVALUATION_VERSION,
    EvaluationFiles,
)
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current


def load_evaluation_input(files: EvaluationFiles) -> tuple[EvaluationInput, str]:
    """冻结后只重读原输入，即使最新行情已被后来修订也不能偷换。"""
    for path in (files.universe_path, files.classification_path):
        if path.stat().st_size > MAX_EVALUATION_VERSION:
            raise ValueError("评估版本文件超出上限")
    universe = UniverseVersion.model_validate_json(files.universe_path.read_text(encoding="utf-8"))
    classifications = ClassificationVersion.model_validate_json(files.classification_path.read_text(encoding="utf-8"))
    if (
        digest(universe.model_dump(mode="json")) != files.universe_hash
        or digest(classifications.model_dump(mode="json")) != files.classification_hash
    ):
        raise ValueError("评估版本文件与冻结清单不一致")
    path = files.frozen_input_path
    if path is not None and path.exists():
        if path.stat().st_size > MAX_EVALUATION_INPUT:
            raise ValueError("评估冻结输入超出上限")
        inputs = EvaluationInput.model_validate_json(path.read_text(encoding="utf-8"))
        if inputs.cutoff != files.cutoff or inputs.universe != universe or inputs.classifications != classifications:
            raise ValueError("评估冻结输入与版本清单不一致")
    else:
        source, bars, issues = read_market(
            files.market_path,
            sessions_ending(files.cutoff, EVALUATION_HISTORY)[0],
            files.cutoff,
            max_rows=MAX_EVALUATION_ROWS,
        )
        inputs = EvaluationInput(
            cutoff=files.cutoff,
            source_identity=source,
            bars=bars,
            issues=issues,
            universe=universe,
            classifications=classifications,
        )
        if path is not None:
            write_once(path, inputs.model_dump_json() + "\n")
    fingerprint = digest(inputs.model_dump(mode="json"))
    if files.expected_input_hash is not None and files.expected_input_hash != fingerprint:
        raise ValueError("评估原始行情摘要不一致")
    if path is not None:
        write_once(path.with_suffix(".sha256"), fingerprint + "\n")
    return inputs, fingerprint


def evaluation_result(root: Path, report: EvaluationReport) -> dict[str, object]:
    """内部结果含受控定位，HTTP须投影为不含路径的业务视图。"""
    return {
        "kind": report.kind,
        "path": str((root / "report.json").resolve()),
        "content_hash": report.content_hash,
        "input_hash": report.input_hash,
        "cutoff": report.cutoff.isoformat(),
        "candidate_count": report.candidate_count,
        "section_count": len(report.sections) // 2,
        "usable_sections": report.usable_sections,
        "model_selection": None,
        "manual_review": report.manual_review,
        "limitations": report.limitations,
    }


def evaluate_files(
    files: EvaluationFiles, *, checkpoint: Callable[[str, int, str], None] | None = None
) -> dict[str, object]:
    """接收纯文件/参数协议，正式API只在隔离worker调用此入口。"""
    ensure_source_current()
    if files.source_hash != LOADED_SOURCE_IDENTITY:
        raise ValueError("评估执行源码与冻结清单不一致")
    if files.cutoff > latest_complete_session():
        raise ValueError("评估截止日尚未完成正式收盘")
    if files.slippage_bps_per_side != slippage_bps_per_side():
        raise ValueError("评估成本与冻结配置不一致")
    with data_lock(files.artifacts_path / "locks", "evaluation-us", files.cutoff.isoformat()):
        inputs, fingerprint = load_evaluation_input(files)
        if checkpoint:
            checkpoint("frozen", 0, fingerprint)
        request = {
            "input_hash": fingerprint,
            "source_hash": files.source_hash,
            "martin": files.martin.model_dump(mode="json"),
            "wyckoff": files.wyckoff.model_dump(mode="json"),
            "allow_current_universe": files.allow_current_universe,
            "coverage_min": files.coverage_min,
            "cost_pct": round_trip_cost_pct(code="SPY.US"),
        }
        root = files.artifacts_path / "evaluations" / digest(request)
        write_once(root / "request.json", json.dumps(request, sort_keys=True))
        if files.frozen_input_path is None:
            write_once(root / "input.json", inputs.model_dump_json() + "\n")
        else:
            try:
                os.link(files.frozen_input_path, root / "input.json")
            except FileExistsError:
                cached = EvaluationInput.model_validate_json((root / "input.json").read_text(encoding="utf-8"))
                if inputs != cached:
                    raise ValueError("评估缓存未引用原冻结输入") from None
        report = evaluate_history(
            inputs,
            files.martin,
            files.wyckoff,
            root,
            allow_current_universe=files.allow_current_universe,
            coverage_min=files.coverage_min,
            timeout_seconds=files.timeout_seconds,
            on_section=(lambda count: checkpoint("section", count, fingerprint)) if checkpoint else None,
        )
        path = root / "report.json"
        write_once(path, report.model_dump_json() + "\n")
        verified = EvaluationReport.model_validate_json(path.read_text(encoding="utf-8"))
        ensure_source_current()
        return evaluation_result(root, verified)
