"""单组和有限组合参数实验的受控变体与缓存边界。"""

from pathlib import Path

import pytest

from stock_research.evaluation import EvaluationReport
from stock_research.evaluation_cli import evaluate_combined_parameter_experiment, evaluate_parameter_experiments
from stock_research.evaluation_local import evaluate_files
from stock_research.identity import digest
from stock_research.parameter_experiments import combined_parameter_experiment, experiment_parameter_sets
from stock_research.parameters import MartinParameters
from stock_research.schemas.evaluation_execution import EvaluationFiles


def test_validation_parameter_experiments_use_only_plan_fields() -> None:
    """[VALIDATION] 三个计划字段之外不得进入首轮实验。"""
    with pytest.raises(ValueError, match="实验字段"):
        experiment_parameter_sets(MartinParameters(), "ema_periods", (9, 21, 50))


def test_validation_parameter_experiments_require_three_unique_values() -> None:
    """[VALIDATION] 不足、重复或超过三个值都不能冒充单组实验。"""
    base = MartinParameters()
    with pytest.raises(ValueError, match="三个不重复值"):
        experiment_parameter_sets(base, "support_atr", (0.5, 0.75))
    with pytest.raises(ValueError, match="三个不重复值"):
        experiment_parameter_sets(base, "support_atr", (0.5, 0.5, 0.75))


def test_integration_parameter_experiment_uses_separate_immutable_cache(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] baseline与变体同冻结输入分目录缓存，重跑复用原产物。"""
    baseline = evaluation_files.model_copy(update={"frozen_input_path": None})
    baseline_result = evaluate_files(baseline)
    results = evaluate_parameter_experiments(evaluation_files, "support_atr", (0.5, 0.75, 1.0))

    assert [result["experiment"]["value"] for result in results] == [0.5, 0.75, 1.0]
    assert len({Path(baseline_result["path"]), *(Path(result["path"]) for result in results)}) == 4
    first_paths = [result["path"] for result in results]
    repeated = evaluate_parameter_experiments(evaluation_files, "support_atr", (0.5, 0.75, 1.0))
    assert [result["path"] for result in repeated] == first_paths


def test_validation_combined_experiment_requires_three_planned_fields() -> None:
    """[VALIDATION] 有限组合不得开放任意字段或复用生产默认值。"""
    base = MartinParameters()
    with pytest.raises(ValueError, match="三个计划字段"):
        combined_parameter_experiment(base, ("support_atr", "support_atr", "contraction_ratio"), (0.5, 0.75, 0.7))
    with pytest.raises(ValueError, match="非生产默认值"):
        combined_parameter_experiment(base, ("support_atr", "extension_atr", "contraction_ratio"), (0.5, 3, 0.7))
    with pytest.raises(ValueError, match="三个不重复且非生产默认值"):
        combined_parameter_experiment(base, ("support_atr", "extension_atr", "contraction_ratio"), (3, 2, 0.7))


def test_integration_combined_experiment_uses_frozen_input_and_immutable_cache(
    evaluation_files: EvaluationFiles,
) -> None:
    """[INTEGRATION] 组合实验单独显式执行并独立缓存，不发布生产参数。"""
    values = (1.0, 4.0, 0.8)
    result = evaluate_combined_parameter_experiment(evaluation_files, values)
    repeated = evaluate_combined_parameter_experiment(evaluation_files, values)
    path = Path(result["path"])
    report = EvaluationReport.model_validate_json(path.read_text(encoding="utf-8"))

    assert result["experiment"] == {"kind": "combined", "values": values}
    assert result["path"] == repeated["path"]
    assert report.input_hash == EvaluationReport.model_validate_json(path.read_text(encoding="utf-8")).input_hash
    assert report.manual_review == "pending"
    assert report.parameter_hashes["martin"] != digest(evaluation_files.martin.model_dump(mode="json"))
