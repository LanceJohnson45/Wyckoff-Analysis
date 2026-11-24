"""历史评估CLI：只读本地行情和版本，追加发布不可变研究文件。"""

import json
from datetime import date
from pathlib import Path

from core.trade_friction import round_trip_cost_pct
from stock_research.artifacts import data_lock, write_once
from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.config import Settings
from stock_research.data import read_market
from stock_research.evaluation import EVALUATION_HISTORY, EvaluationInput, EvaluationReport, evaluate_history
from stock_research.identity import digest
from stock_research.models import ClassificationVersion, UniverseVersion
from stock_research.parameters import MartinParameters
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current
from stock_research.wyckoff import WyckoffParameters


def evaluation_result(root: Path, report: EvaluationReport) -> dict[str, object]:
    """只返回摘要，完整40份量化证据和封存规则另行读取。"""
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


def evaluate_command(
    settings: Settings, profile: str, cutoff: date, *, allow_current_universe: bool = False
) -> dict[str, object]:
    """单独CLI内计算，不占用在线API事件循环，不访问外部行情或模型。"""
    ensure_source_current()
    if cutoff > latest_complete_session():
        raise ValueError("评估截止日尚未完成正式收盘")
    martin = MartinParameters.model_validate_json(settings.parameters_path.read_text(encoding="utf-8"))
    wyckoff = WyckoffParameters.model_validate_json(settings.wyckoff_parameters_path.read_text(encoding="utf-8"))
    if martin.version != profile:
        raise ValueError("评估profile与配置参数版本不一致")
    start = sessions_ending(cutoff, EVALUATION_HISTORY)[0]
    with data_lock(settings.artifacts_path / "locks", "evaluation-us", cutoff.isoformat()):
        identity, bars, issues = read_market(settings.local_market_path, start, cutoff)
        inputs = EvaluationInput(
            cutoff=cutoff,
            source_identity=identity,
            bars=bars,
            issues=issues,
            universe=UniverseVersion.model_validate_json(settings.universe_path.read_text(encoding="utf-8")),
            classifications=ClassificationVersion.model_validate_json(
                settings.classification_path.read_text(encoding="utf-8")
            ),
        )
        request = {
            "input_hash": digest(inputs.model_dump(mode="json")),
            "source_hash": LOADED_SOURCE_IDENTITY,
            "martin": martin.model_dump(mode="json"),
            "wyckoff": wyckoff.model_dump(mode="json"),
            "allow_current_universe": allow_current_universe,
            "coverage_min": settings.coverage_min,
            "cost_pct": round_trip_cost_pct(code="SPY.US"),
        }
        root = settings.artifacts_path / "evaluations" / digest(request)
        write_once(root / "request.json", json.dumps(request, sort_keys=True))
        write_once(root / "input.json", inputs.model_dump_json() + "\n")
        report = evaluate_history(
            inputs,
            martin,
            wyckoff,
            root,
            allow_current_universe=allow_current_universe,
            coverage_min=settings.coverage_min,
            timeout_seconds=settings.run_timeout_seconds,
        )
        path = root / "report.json"
        write_once(path, report.model_dump_json() + "\n")
        verified = EvaluationReport.model_validate_json(path.read_text(encoding="utf-8"))
        ensure_source_current()
        return evaluation_result(root, verified)
