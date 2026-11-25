"""将operator配置转换为CLI/worker共享的纯文件和参数协议。"""

from datetime import date
from pathlib import Path

from core.trade_friction import slippage_bps_per_side
from stock_research.config import Settings
from stock_research.evaluation_local import evaluate_files
from stock_research.identity import digest
from stock_research.models import ClassificationVersion, UniverseVersion
from stock_research.parameter_experiments import ParameterExperiment, experiment_parameter_sets
from stock_research.parameters import MartinParameters
from stock_research.schemas.evaluation_execution import EvaluationFiles
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current
from stock_research.wyckoff import WyckoffParameters


def evaluate_command(
    settings: Settings,
    profile: str,
    cutoff: date,
    *,
    allow_current_universe: bool = False,
    frozen_input_path: Path | None = None,
) -> dict[str, object]:
    """CLI使用operator文件，API另在接单时冻结实际发布版本。"""
    ensure_source_current()
    return evaluate_files(evaluation_files(settings, profile, cutoff, allow_current_universe, frozen_input_path))


def evaluate_parameter_experiments_command(
    settings: Settings,
    profile: str,
    cutoff: date,
    field: str,
    values: tuple[float, float, float],
    *,
    allow_current_universe: bool,
) -> list[dict[str, object]]:
    """CLI冻结一份输入后顺序运行三个显式变体。"""
    frozen_input_path = settings.artifacts_path / "parameter-experiments" / field / "input.json"
    files = evaluation_files(settings, profile, cutoff, allow_current_universe, frozen_input_path)
    return evaluate_parameter_experiments(files, field, values)


def evaluation_files(
    settings: Settings, profile: str, cutoff: date, allow_current_universe: bool, frozen_input_path: Path | None
) -> EvaluationFiles:
    """参数与实验共享同一份冻结输入定位。"""
    martin = MartinParameters.model_validate_json(settings.parameters_path.read_text(encoding="utf-8"))
    wyckoff = WyckoffParameters.model_validate_json(settings.wyckoff_parameters_path.read_text(encoding="utf-8"))
    if martin.version != profile:
        raise ValueError("评估profile与配置参数版本不一致")
    universe = UniverseVersion.model_validate_json(settings.universe_path.read_text(encoding="utf-8"))
    classifications = ClassificationVersion.model_validate_json(
        settings.classification_path.read_text(encoding="utf-8")
    )
    return EvaluationFiles(
        cutoff=cutoff,
        market_path=settings.local_market_path.resolve(),
        universe_path=settings.universe_path.resolve(),
        classification_path=settings.classification_path.resolve(),
        artifacts_path=settings.artifacts_path.resolve(),
        frozen_input_path=frozen_input_path,
        source_hash=LOADED_SOURCE_IDENTITY,
        universe_hash=digest(universe.model_dump(mode="json")),
        classification_hash=digest(classifications.model_dump(mode="json")),
        martin=martin,
        wyckoff=wyckoff,
        allow_current_universe=allow_current_universe,
        coverage_min=settings.coverage_min,
        timeout_seconds=settings.run_timeout_seconds,
        slippage_bps_per_side=slippage_bps_per_side(),
    )


def evaluate_parameter_experiments(
    files: EvaluationFiles, field: str, values: tuple[float, float, float]
) -> list[dict[str, object]]:
    """同输入运行三个不发布变体，产物按参数摘要独立缓存。"""
    experiments = experiment_parameter_sets(files.martin, field, values)
    return [evaluate_experiment(files, experiment) for experiment in experiments]


def evaluate_experiment(files: EvaluationFiles, experiment: ParameterExperiment) -> dict[str, object]:
    """变体报告显式带实验字段和值，不修改生产发布。"""
    result = evaluate_files(files.model_copy(update={"martin": experiment.parameters}))
    return {**result, "experiment": {"field": experiment.field, "value": experiment.value}}
