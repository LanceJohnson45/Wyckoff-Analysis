"""历史评估人工审查的只读抽样清单。"""

from collections.abc import Iterable
from pathlib import Path

from stock_research.analysis import MartinReport
from stock_research.evaluation import EvaluationReport
from stock_research.wyckoff import WyckoffReport

REVIEW_SAMPLE_SIZE = 10


def _take(items: Iterable[dict[str, object]], count: int) -> list[dict[str, object]]:
    """抽样保持产物顺序，不引入收益或排名。"""
    return list(items)[:count]


def manual_review_samples(report: EvaluationReport, root: Path) -> dict[str, object]:
    """从已验证量化证据生成每策略入选/排除审查清单。"""
    strategies = {}
    for strategy in ("martin", "wyckoff"):
        strategies[strategy] = _strategy_samples(report, root, strategy)
    return {"input_hash": report.input_hash, "strategies": strategies}


def _strategy_samples(report: EvaluationReport, root: Path, strategy: str) -> dict[str, object]:
    """只读正式分析文件，不足10条时明确失败，不能凑数。"""
    selected: list[dict[str, object]] = []
    excluded: list[dict[str, object]] = []
    for section in report.sections:
        if section.strategy != strategy:
            continue
        path = root / section.analysis_file
        model = MartinReport if strategy == "martin" else WyckoffReport
        analysis = model.model_validate_json(path.read_text(encoding="utf-8"))
        observations = {(row.symbol if hasattr(row, "symbol") else row["symbol"]): row for row in analysis.observations}
        selected.extend(
            {"section": section.section.as_of.isoformat(), **_observation(row)}
            for symbol in section.pool
            if (row := observations.get(symbol)) is not None
        )
        if section.section.partition != "sealed" and len(excluded) < REVIEW_SAMPLE_SIZE:
            excluded.extend({"section": section.section.as_of.isoformat(), **row} for row in analysis.excluded)
        if len(selected) >= REVIEW_SAMPLE_SIZE and len(excluded) >= REVIEW_SAMPLE_SIZE:
            break
    if len(selected) < REVIEW_SAMPLE_SIZE or len(excluded) < REVIEW_SAMPLE_SIZE:
        raise ValueError(f"{strategy}人工审查样本不足：入选{len(selected)}/排除{len(excluded)}")
    return {
        "selected": _take(selected, REVIEW_SAMPLE_SIZE),
        "excluded": _take(excluded, REVIEW_SAMPLE_SIZE),
    }


def _take(items: list[dict[str, object]], count: int) -> list[dict[str, object]]:
    """抽样保持产物顺序，不引入收益或排名。"""
    return items[:count]


def _observation(row: object) -> dict[str, object]:
    """Martin为模型、Wyckoff为字典，统一为只读审查载荷。"""
    return row.model_dump(mode="json") if hasattr(row, "model_dump") else row  # type: ignore[return-value]
