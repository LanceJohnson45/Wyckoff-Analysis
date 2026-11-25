"""单组参数实验的受控变体和缓存边界。"""

from pathlib import Path

import pytest

from stock_research.evaluation_cli import evaluate_parameter_experiments
from stock_research.evaluation_local import evaluate_files
from stock_research.parameter_experiments import experiment_parameter_sets
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
