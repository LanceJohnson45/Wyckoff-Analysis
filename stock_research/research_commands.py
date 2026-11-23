"""量化研究CLI编排；冻结输入与参数后再发布不可变产物。"""

from datetime import date
from pathlib import Path

from stock_research.analysis import MartinReport, compute_martin
from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.identity import digest
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.pipeline import run_preview
from stock_research.run_store import RunStore
from stock_research.store import close_engine, database_engine
from stock_research.wyckoff import WyckoffParameters, WyckoffReport, compute_wyckoff, engine_hash


def analyze_command(
    settings: Settings, snapshot_path: Path, as_of: date, *, strategy: str = "martin"
) -> dict[str, object]:
    """本地文件和缓存均重读校验，参数摘要不同另生成产物。"""
    snapshot = Snapshot.model_validate_json(snapshot_path.read_text(encoding="utf-8"))
    if snapshot.as_of != as_of:
        raise ValueError("指定快照与分析截止日不一致")
    if strategy not in {"martin", "wyckoff"}:
        raise ValueError("未知量化策略")
    parameter_model, report_model, compute, parameter_path = (
        (MartinParameters, MartinReport, compute_martin, settings.parameters_path)
        if strategy == "martin"
        else (WyckoffParameters, WyckoffReport, compute_wyckoff, settings.wyckoff_parameters_path)
    )
    parameters = parameter_model.model_validate_json(parameter_path.read_text(encoding="utf-8"))
    identity = {
        "schema_version": 1,
        "snapshot": snapshot.content_hash,
        "parameters": parameters.model_dump(mode="json"),
    }
    if strategy == "wyckoff":
        identity["engine_hash"] = engine_hash()
    path = settings.artifacts_path / "analyses" / f"{strategy}-{digest(identity)}.json"
    if path.exists():
        report = report_model.model_validate_json(path.read_text(encoding="utf-8"))
        if (
            report.snapshot_hash != snapshot.content_hash
            or report.parameters != parameters
            or (strategy == "wyckoff" and report.engine_hash != identity["engine_hash"])
        ):
            raise ValueError("量化产物的输入或参数与缓存键不一致")
    else:
        report = compute(snapshot, parameters)
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


async def run_command(
    settings: Settings, snapshot_path: Path, target_session: date, *, parent_id: str | None = None, force: bool = False
) -> dict[str, object]:
    """CLI预览走相同账本与执行器；不发送通知或创建订单。"""
    import asyncio

    snapshot = Snapshot.model_validate_json(snapshot_path.read_text(encoding="utf-8"))
    analyses = {}
    for strategy, model in (("martin", MartinReport), ("wyckoff", WyckoffReport)):
        output = await asyncio.to_thread(analyze_command, settings, snapshot_path, snapshot.as_of, strategy=strategy)
        analyses[strategy] = model.model_validate_json(Path(output["path"]).read_text(encoding="utf-8"))
    engine = database_engine(settings)
    try:
        return await run_preview(engine, settings, snapshot, analyses, target_session, parent_id=parent_id, force=force)
    finally:
        await close_engine(engine)


async def read_run_command(settings: Settings, run_id: str) -> dict[str, object]:
    """独立读连接查询已提交结果，不启动Harness或读取模型凭据。"""
    engine = database_engine(settings)
    try:
        return await RunStore(engine, settings.workspace).summary(run_id)
    finally:
        await close_engine(engine)


async def track_run_command(settings: Settings, run_id: str) -> dict[str, object]:
    """显式接入已有终态运行；采用当日来源与日期顺序规则，不执行模型。"""
    from stock_research.watch_queries import watch_page

    engine = database_engine(settings)
    try:
        store = RunStore(engine, settings.workspace)
        body = await store.summary(run_id)
        if body["status"] in {"pending", "running"}:
            raise ValueError("研究尚未结束，请等待原执行者完成")
        await store.publish(run_id, body["status"], body["error_code"], track=True)
        page = await watch_page(store, 1, 100)
        return {"processed_run_id": run_id, "watchlists": page.model_dump(mode="json")}
    finally:
        await close_engine(engine)
