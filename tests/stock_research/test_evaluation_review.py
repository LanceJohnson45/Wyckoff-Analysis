from pathlib import Path

import pytest

from stock_research.evaluation import EvaluationReport
from stock_research.evaluation_review import manual_review_samples


def test_validation_manual_review_requires_evidence_files() -> None:
    """[VALIDATION] 任一策略不足10入选时不能凑数冒充人工审查完成。"""
    report = EvaluationReport.model_validate_json(
        (Path(__file__).parent / "fixtures/evaluation-report-insufficient.json").read_text(encoding="utf-8")
    )
    with pytest.raises(FileNotFoundError):
        manual_review_samples(report, Path("/unavailable"))


def test_validation_manual_review_requires_evidence_files_for_empty_pool() -> None:
    """[VALIDATION] 无入选样本时先失败，不读取不可达分析文件。"""
    report = EvaluationReport.model_validate_json(
        (Path(__file__).parent / "fixtures/evaluation-report-empty.json").read_text(encoding="utf-8")
    )
    with pytest.raises(FileNotFoundError):
        manual_review_samples(report, Path("/unavailable"))
