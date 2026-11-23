"""同步CLI与隔离worker共用生产分派、缓存身份和绑定校验。"""

from pathlib import Path

from stock_research.analysis import MartinReport, compute_martin
from stock_research.config import Settings
from stock_research.identity import digest
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.source_guard import LOADED_SOURCE_IDENTITY
from stock_research.wyckoff import WyckoffParameters, WyckoffReport, compute_wyckoff, engine_hash

QuantParameters = MartinParameters | WyckoffParameters
QuantReport = MartinReport | WyckoffReport


def quant_parameters(settings: Settings, strategy: str) -> QuantParameters:
    """只读取operator受控版本文件，未知策略不能选择路径。"""
    if strategy == "martin":
        return MartinParameters.model_validate_json(settings.parameters_path.read_text(encoding="utf-8"))
    if strategy == "wyckoff":
        return WyckoffParameters.model_validate_json(settings.wyckoff_parameters_path.read_text(encoding="utf-8"))
    raise ValueError("未知量化策略")


def analysis_path(
    settings: Settings, snapshot: Snapshot, parameters: QuantParameters, *, source_hash: str = LOADED_SOURCE_IDENTITY
) -> Path:
    """缓存身份保持原源码/快照/参数及Wyckoff引擎定义。"""
    identity = {
        "schema_version": 1,
        "source_hash": source_hash,
        "snapshot": snapshot.content_hash,
        "parameters": parameters.model_dump(mode="json"),
    }
    strategy = "martin" if isinstance(parameters, MartinParameters) else "wyckoff"
    if strategy == "wyckoff":
        identity["engine_hash"] = engine_hash()
    return settings.artifacts_path / "analyses" / f"{strategy}-{digest(identity)}.json"


def verify_analysis(raw: str, snapshot: Snapshot, parameters: QuantParameters) -> QuantReport:
    """缓存和子进程结果都必须回到同一快照/参数/引擎绑定。"""
    model = MartinReport if isinstance(parameters, MartinParameters) else WyckoffReport
    report = model.model_validate_json(raw)
    if (
        report.snapshot_hash != snapshot.content_hash
        or report.parameters != parameters
        or (isinstance(report, WyckoffReport) and report.engine_hash != engine_hash())
    ):
        raise ValueError("量化产物的输入或参数与缓存键不一致")
    return report


def compute_analysis(snapshot: Snapshot, parameters: QuantParameters) -> QuantReport:
    """仅分派正式实现，不复制任何策略指标或信号逻辑。"""
    if isinstance(parameters, MartinParameters):
        return compute_martin(snapshot, parameters)
    return compute_wyckoff(snapshot, parameters)
