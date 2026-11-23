"""量化研究CLI编排；冻结输入与参数后再发布不可变产物。"""

from datetime import date
from pathlib import Path

from stock_research.analysis import MartinReport, compute_martin
from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.identity import digest
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters


def analyze_command(settings: Settings, snapshot_path: Path, as_of: date) -> dict[str, object]:
    """本地文件和缓存均重读校验，参数摘要不同另生成产物。"""
    snapshot = Snapshot.model_validate_json(snapshot_path.read_text(encoding="utf-8"))
    if snapshot.as_of != as_of:
        raise ValueError("指定快照与分析截止日不一致")
    parameters = MartinParameters.model_validate_json(settings.parameters_path.read_text(encoding="utf-8"))
    key = digest(
        {"schema_version": 1, "snapshot": snapshot.content_hash, "parameters": parameters.model_dump(mode="json")}
    )
    path = settings.artifacts_path / "analyses" / f"martin-{key}.json"
    if path.exists():
        report = MartinReport.model_validate_json(path.read_text(encoding="utf-8"))
        if report.snapshot_hash != snapshot.content_hash or report.parameters != parameters:
            raise ValueError("量化产物的输入或参数与缓存键不一致")
    else:
        report = compute_martin(snapshot, parameters)
        write_once(path, report.model_dump_json(indent=2) + "\n")
    return {
        "path": str(path.resolve()),
        "content_hash": report.content_hash,
        "snapshot_id": report.snapshot_id,
        "snapshot_hash": report.snapshot_hash,
        "parameter_hash": report.parameter_hash,
        "observed_count": len(report.observations),
        "pool": report.pool,
        "pool_size": len(report.pool),
        "model_selection": None,
        "limitations": report.limitations,
    }
