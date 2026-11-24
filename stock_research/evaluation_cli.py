"""将operator配置转换为CLI/worker共享的纯文件和参数协议。"""

from datetime import date

from core.trade_friction import slippage_bps_per_side
from stock_research.config import Settings
from stock_research.evaluation_local import evaluate_files
from stock_research.identity import digest
from stock_research.models import ClassificationVersion, UniverseVersion
from stock_research.parameters import MartinParameters
from stock_research.schemas.evaluation_execution import EvaluationFiles
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current
from stock_research.wyckoff import WyckoffParameters


def evaluate_command(
    settings: Settings, profile: str, cutoff: date, *, allow_current_universe: bool = False
) -> dict[str, object]:
    """CLI使用operator文件，API另在接单时冻结实际发布版本。"""
    ensure_source_current()
    martin = MartinParameters.model_validate_json(settings.parameters_path.read_text(encoding="utf-8"))
    wyckoff = WyckoffParameters.model_validate_json(settings.wyckoff_parameters_path.read_text(encoding="utf-8"))
    if martin.version != profile:
        raise ValueError("评估profile与配置参数版本不一致")
    universe = UniverseVersion.model_validate_json(settings.universe_path.read_text(encoding="utf-8"))
    classifications = ClassificationVersion.model_validate_json(
        settings.classification_path.read_text(encoding="utf-8")
    )
    files = EvaluationFiles(
        cutoff=cutoff,
        market_path=settings.local_market_path.resolve(),
        universe_path=settings.universe_path.resolve(),
        classification_path=settings.classification_path.resolve(),
        artifacts_path=settings.artifacts_path.resolve(),
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
    return evaluate_files(files)
