"""工作台评估冻结实际已发布参数/输入，不把Settings送入worker。"""

import asyncio

from core.trade_friction import slippage_bps_per_side
from stock_research.config import Settings
from stock_research.errors import ResearchInputError
from stock_research.identity import digest
from stock_research.input_configuration import resolve_run_inputs
from stock_research.models import ClassificationVersion, UniverseVersion
from stock_research.parameter_config import resolve_run_parameters
from stock_research.parameters import MartinParameters
from stock_research.run_store import RunStore
from stock_research.schemas.evaluation_execution import EvaluationFiles
from stock_research.schemas.evaluations import CreateEvaluation
from stock_research.wyckoff import WyckoffParameters


async def freeze_evaluation(store: RunStore, settings: Settings, row: dict[str, object]) -> dict[str, object]:
    """冻结后恢复只消费原清单，参数发布和人工输入另有版本出处。"""
    request = CreateEvaluation.model_validate(row["payload"]["request"])
    config = row["payload"]["configuration"]
    settings, parameter_sources = await resolve_run_parameters(store, settings)
    settings, input_sources = await resolve_run_inputs(store, settings, request.cutoff)
    root = (settings.artifacts_path / "evaluation-jobs" / row["uuid"]).resolve()
    paths = (
        settings.parameters_path,
        settings.wyckoff_parameters_path,
        settings.universe_path,
        settings.classification_path,
    )
    raw = await asyncio.gather(*(asyncio.to_thread(path.read_text, encoding="utf-8") for path in paths))
    martin, wyckoff = MartinParameters.model_validate_json(raw[0]), WyckoffParameters.model_validate_json(raw[1])
    universe, classifications = (
        UniverseVersion.model_validate_json(raw[2]),
        ClassificationVersion.model_validate_json(raw[3]),
    )
    if martin.version != request.profile:
        raise ResearchInputError("评估profile与当前发布参数版本不一致")
    files = EvaluationFiles(
        cutoff=request.cutoff,
        market_path=settings.local_market_path.resolve(),
        universe_path=paths[2].resolve(),
        classification_path=paths[3].resolve(),
        artifacts_path=root / "artifacts",
        frozen_input_path=root / "input.json",
        universe_hash=digest(universe.model_dump(mode="json")),
        classification_hash=digest(classifications.model_dump(mode="json")),
        source_hash=config["source_hash"],
        martin=martin,
        wyckoff=wyckoff,
        allow_current_universe=request.allow_current_universe,
        coverage_min=settings.coverage_min,
        timeout_seconds=settings.run_timeout_seconds,
        slippage_bps_per_side=slippage_bps_per_side(),
    )
    return {
        **config,
        "files": files.model_dump(mode="json"),
        "parameter_sources": parameter_sources,
        "input_sources": input_sources.model_dump(mode="json"),
    }
