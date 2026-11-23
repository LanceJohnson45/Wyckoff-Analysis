"""量化研究CLI编排；冻结输入与参数后再发布不可变产物。"""

from datetime import date
from pathlib import Path

from stock_research.analysis import compute_martin
from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.errors import ResearchQualityError
from stock_research.input_configuration import describe_inputs, resolve_run_inputs
from stock_research.models import Snapshot
from stock_research.parameter_config import resolve_run_parameters
from stock_research.parameters import MartinParameters
from stock_research.pipeline import run_preview
from stock_research.preparations import record_preparation
from stock_research.quant_compute import analysis_path, verify_analysis
from stock_research.quant_execution import analyze_isolated
from stock_research.run_store import RunStore
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current
from stock_research.store import close_engine, database_engine
from stock_research.wyckoff import WyckoffParameters, compute_wyckoff


def analyze_command(
    settings: Settings, snapshot_path: Path, as_of: date, *, strategy: str = "martin"
) -> dict[str, object]:
    """本地文件和缓存均重读校验，参数摘要不同另生成产物。"""
    ensure_source_current()
    snapshot = Snapshot.model_validate_json(snapshot_path.read_text(encoding="utf-8"))
    if snapshot.as_of != as_of:
        raise ValueError("指定快照与分析截止日不一致")
    if strategy not in {"martin", "wyckoff"}:
        raise ValueError("未知量化策略")
    parameter_model, compute, parameter_path = (
        (MartinParameters, compute_martin, settings.parameters_path)
        if strategy == "martin"
        else (WyckoffParameters, compute_wyckoff, settings.wyckoff_parameters_path)
    )
    parameters = parameter_model.model_validate_json(parameter_path.read_text(encoding="utf-8"))
    path = analysis_path(settings, snapshot, parameters, source_hash=LOADED_SOURCE_IDENTITY)
    if path.exists():
        report = verify_analysis(path.read_text(encoding="utf-8"), snapshot, parameters)
    else:
        report = compute(snapshot, parameters)
        write_once(path, report.model_dump_json(indent=2) + "\n")
    ensure_source_current()
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
    settings: Settings,
    snapshot_path: Path | None,
    target_session: date,
    *,
    parent_id: str | None = None,
    force: bool = False,
) -> dict[str, object]:
    """CLI预览走相同账本与执行器；不发送通知或创建订单。"""
    import asyncio

    ensure_source_current()
    engine = database_engine(settings)
    try:
        async with record_preparation(RunStore(engine, settings.workspace), target_session, "cli") as attempt:
            settings, sources = await resolve_run_parameters(RunStore(engine, settings.workspace), settings)
            await attempt.checkpoint("snapshot")
            input_sources = None
            if snapshot_path is None:
                from stock_research.cli import snapshot_command

                settings, input_sources = await resolve_run_inputs(
                    RunStore(engine, settings.workspace), settings, attempt.state.as_of
                )
                output, _ = await asyncio.to_thread(snapshot_command, settings, attempt.state.as_of, offline=True)
                snapshot_path = Path(output["path"])
            snapshot = Snapshot.model_validate_json(await asyncio.to_thread(snapshot_path.read_text, encoding="utf-8"))
            input_sources = input_sources or describe_inputs(
                snapshot.universe, snapshot.classifications, snapshot.as_of, source="frozen_snapshot"
            )
            await attempt.checkpoint("snapshot", snapshot.quality)
            if not snapshot.quality.passed:
                raise ResearchQualityError("快照未通过研究质量门槛")
            await attempt.checkpoint("analysis")
            analyses = {}
            for strategy in ("martin", "wyckoff"):
                analyses[strategy] = await analyze_isolated(settings, snapshot_path, snapshot, strategy)
            await attempt.checkpoint("freeze")
            return await run_preview(
                engine,
                settings,
                snapshot,
                analyses,
                target_session,
                parent_id=parent_id,
                force=force,
                parameter_sources=sources,
                input_sources=input_sources.model_dump(mode="json"),
                on_prepared=attempt.ready,
            )
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
