"""固定输入的独立初选、提交后互评及仅补失败阶段的执行编排。"""

import asyncio
import hashlib
from collections.abc import Coroutine
from datetime import date, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from stock_research.analysis import MartinReport
from stock_research.calendar import session_times
from stock_research.config import Settings
from stock_research.decisions import ResearchPoint, ReviewDecision, SelectionDecision
from stock_research.evidence import EvidenceContext, build_context
from stock_research.executor import StageResult, execute_stage, verify_runtime
from stock_research.identity import digest, new_id, now
from stock_research.model_connection import ModelConnection, load_model_connection, load_profile_connection
from stock_research.models import Snapshot
from stock_research.prompts import prompt_identity
from stock_research.run_store import RunStore
from stock_research.schema import FACTOR_RESULTS, RESEARCH_RUNS
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current, source_identity
from stock_research.store import load_snapshot, save_snapshot
from stock_research.wyckoff import WyckoffReport

STRATEGIES = ("martin", "wyckoff")
logger = structlog.get_logger(__name__)


def model_connections(settings: Settings) -> dict[str, ModelConnection]:
    """连接在执行前冻结，每个策略/角色可独立选已注册模型。"""
    # lint-redline:ignore SQL-02 固定策略/角色配置键，无SQL标识符或外部输入。
    keys = {f"{strategy}.{role}" for strategy in STRATEGIES for role in ("select", "review")}
    if settings.models_by_role.keys() - keys or bool(settings.model_env_path) == bool(settings.model_profile_path):
        raise ValueError("须选择一个可信模型配置来源，角色映射不能包含未知键")
    connection = (
        load_profile_connection(
            settings.model_profile_path,
            settings.model_credentials_path,
            provider=settings.model_provider,
            model=settings.model,
        )
        if settings.model_profile_path
        else load_model_connection(settings.model_env_path, model=settings.model, api=settings.model_api)
    )
    result = {}
    for key in sorted(keys):
        model = settings.models_by_role.get(key, connection.model)
        if model not in connection.available_models:
            raise ValueError("角色模型未在可信连接中注册")
        result[key] = connection.model_copy(update={"model": model, "output_tokens": settings.model_output_tokens})
    return result


def program_result(context: EvidenceContext, target: dict[str, object] | None) -> StageResult:
    """空池是有证据的成功状态，不依赖可用模型或伪造模型原文。"""
    if context.role == "select":
        decision = SelectionDecision(
            strategy=context.strategy,
            context_hash=context.content_hash,
            items=(),
            empty_reason=ResearchPoint(text="本策略量化合格池为空，保留空初选", evidence_refs=("scope/pool_size",)),
        )
        source = "empty_pool"
    else:
        decision = ReviewDecision(
            strategy=context.strategy,
            context_hash=context.content_hash,
            target_selection_id=target["selection_id"],
            items=(),
        )
        source = "empty_target"
    return StageResult(
        context_hash=context.content_hash,
        status="completed",
        source=source,
        decision=decision,
        error=None,
        receipt_path="",
    )


async def run_stage(
    store: RunStore,
    settings: Settings,
    snapshot: Snapshot,
    analysis: MartinReport | WyckoffReport,
    run_id: str,
    deadline: datetime,
    connection: ModelConnection | None,
    *,
    target: dict[str, object] | None = None,
) -> None:
    """相同stage_key定位成功结果；失败只重试当前阶段一次。"""
    strategy = "martin" if isinstance(analysis, MartinReport) else "wyckoff"
    role = "review" if target is not None else "select"
    summary = await store.summary(run_id)
    stage_id = await store.ensure_stage(run_id, strategy, role, str(target["selection_id"]) if target else None)
    if any(stage["id"] == stage_id and stage["status"] == "completed" for stage in summary["stages"]):
        return
    targets = tuple(item["symbol"] for item in target["decision"]["items"]) if target else ()
    for attempt in range(2):
        if now() >= deadline:
            raise TimeoutError("运行期限已耗尽")
        context = build_context(
            snapshot,
            analysis,
            workspace=settings.workspace,
            run_id=run_id,
            stage_id=stage_id,
            role=role,
            targets=targets,
            deadline=min(deadline, now() + timedelta(seconds=settings.stage_timeout_seconds)),
        )
        await store.start_stage(stage_id, context)
        try:
            if not context.symbols:
                result = program_result(context, target)
            else:
                if connection is None:
                    raise ValueError("该阶段需要模型连接")
                result = await execute_stage(
                    settings.dsh_executable,
                    context,
                    connection,
                    artifacts=settings.artifacts_path,
                    home_root=settings.dsh_home,
                    agents_root=settings.dsh_agents_home,
                    target=target,
                )
        except asyncio.CancelledError:
            await store.finish_stage(
                stage_id,
                context,
                StageResult(
                    context_hash=context.content_hash,
                    status="cancelled",
                    decision=None,
                    error="cancelled",
                    receipt_path="",
                ),
            )
            raise
        await store.finish_stage(stage_id, context, result)
        if (
            result.status == "completed"
            or result.error not in {"deadline_exceeded", "transient_provider_error", "transport_error"}
            or attempt
        ):
            return


async def execute_run(
    store: RunStore,
    settings: Settings,
    snapshot: Snapshot,
    analyses: dict[str, MartinReport | WyckoffReport],
    run_id: str,
    connections: dict[str, ModelConnection],
) -> dict[str, object]:
    """两个初选各自隔离；全部尝试结束并提交后才生成互评上下文。"""
    run = await store.row(RESEARCH_RUNS, run_id)
    deadline = run["create_time"] + timedelta(seconds=settings.run_timeout_seconds)
    await store.publish(run_id, "running")
    try:
        async with asyncio.timeout(max(0, (deadline - now()).total_seconds())):
            await parallel_stages(
                [
                    run_stage(
                        store,
                        settings,
                        snapshot,
                        analyses[strategy],
                        run_id,
                        deadline,
                        connections.get(f"{strategy}.select"),
                    )
                    for strategy in STRATEGIES
                ]
            )
            selected = await store.publish(run_id, "running")
            reviews = []
            for selection in selected["selections"]:
                target = {"selection_id": selection["id"], **selection["payload"]}
                for reviewer in STRATEGIES:
                    if reviewer != selection["strategy"]:
                        reviews.append(
                            run_stage(
                                store,
                                settings,
                                snapshot,
                                analyses[reviewer],
                                run_id,
                                deadline,
                                connections.get(f"{reviewer}.review"),
                                target=target,
                            )
                        )
            await parallel_stages(reviews)
    except TimeoutError:
        return await failed_run(store, run_id, "run_deadline_exceeded")
    except (ValueError, SQLAlchemyError, OSError):
        logger.exception("研究阶段执行失败", run_id=run_id)
        return await failed_run(store, run_id, "stage_execution_failed")
    except asyncio.CancelledError:
        await store.publish(run_id, "cancelled", track=True)
        raise
    result = await store.summary(run_id)
    success = len(result["selections"]) == len(STRATEGIES) and all(
        stage["status"] == "completed" for stage in result["stages"]
    )
    return await store.publish(
        run_id, "completed" if success else "partial" if result["selections"] else "failed", track=True
    )


async def failed_run(store: RunStore, run_id: str, error: str) -> dict[str, object]:
    """失败收尾统一回收阶段并保留已成功初选及观察状态。"""
    await store.recover_stages(run_id, error)
    return await store.publish(
        run_id, "partial" if (await store.summary(run_id))["selections"] else "failed", error, track=True
    )


async def parallel_stages(coroutines: list[Coroutine[Any, Any, None]]) -> None:
    """一个阶段失败时先取消并收尾兄弟任务，再释放数据库和运行锁。"""
    tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def prepare_preview(
    engine: AsyncEngine,
    settings: Settings,
    snapshot: Snapshot,
    analyses: dict[str, MartinReport | WyckoffReport],
    target_session: date,
    *,
    parent_id: str | None = None,
    force: bool = False,
    request_id: str | None = None,
    parameter_sources: dict[str, object] | None = None,
) -> tuple[str, dict[str, ModelConnection]]:
    """先冻结并保存输入，调用方取得run_id后才能调度执行。"""
    ensure_source_current()
    _opening, report_due, previous = session_times(target_session)
    if snapshot.as_of != previous or set(analyses) != set(STRATEGIES) or not snapshot.quality.passed:
        raise ValueError("运行需要前一正式交易日的合格快照和两个策略产物")
    if any(value.snapshot_hash != snapshot.content_hash for value in analyses.values()):
        raise ValueError("两个策略没有使用同一快照")
    store = RunStore(engine, settings.workspace)
    if force != bool(parent_id):
        raise ValueError("强制重分析须同时指定force和父运行")
    connections = {}
    config = {
        "workspace": settings.workspace,
        "source_hash": LOADED_SOURCE_IDENTITY,
        "prompt_hash": prompt_identity(),
        "target_session": str(target_session),
        "report_due": report_due.isoformat(),
        "universe_hash": digest(snapshot.universe.model_dump(mode="json")),
        "classification_hash": digest(snapshot.classifications.model_dump(mode="json")),
        "parameters": {key: value.parameters.model_dump(mode="json") for key, value in analyses.items()},
        "parameter_sources": parameter_sources or {},
        "algorithms": {"wyckoff": analyses["wyckoff"].engine_hash},
        "model_preferences": model_preferences(settings),
        "preview": True,
        "stage_timeout_seconds": settings.stage_timeout_seconds,
        "run_timeout_seconds": settings.run_timeout_seconds,
        "parent_id": parent_id,
        "manual_nonce": (request_id or new_id()) if force else None,
    }
    existing = await store.find_run(digest(config))
    if existing:
        run_id = existing["uuid"]
    else:
        if any(value.pool for value in analyses.values()):
            connections = await asyncio.to_thread(model_connections, settings)
            if model_preferences(settings) != config["model_preferences"]:
                raise ValueError("模型配置在冻结期间发生变化")
        snapshot = await save_snapshot(engine, settings.workspace, snapshot)
        factors = await store.artifacts(
            snapshot.id,
            {key: value.model_dump(mode="json") for key, value in analyses.items()},
            config["source_hash"],
            config["prompt_hash"],
        )
        manifest = {
            **config,
            "api_request_id": request_id,
            "snapshot_id": snapshot.id,
            "snapshot_hash": snapshot.content_hash,
            "factor_results": factors,
            "analysis_hashes": {key: value.content_hash for key, value in analyses.items()},
            "models": {key: value.model_dump(mode="json") for key, value in connections.items()},
        }
        ensure_source_current()
        run_id = await store.create_run(manifest, parent_id=parent_id, run_key=digest(config))
    return run_id, connections


async def resume_preview(
    engine: AsyncEngine, settings: Settings, run_id: str, connections: dict[str, ModelConnection] | None = None
) -> dict[str, object]:
    """仅从账本恢复固定输入；调用方不能替换原快照或已提交初选。"""
    ensure_source_current()
    store = RunStore(engine, settings.workspace)
    connections = connections or {}
    async with store.execution_lock(run_id):
        current = await store.summary(run_id)
        if current["status"] == "completed":
            return current
        manifest = current["manifest"]
        if manifest["source_hash"] != source_identity() or manifest["prompt_hash"] != prompt_identity():
            return await store.publish(
                run_id, "partial" if current["selections"] else "failed", "execution_version_changed", track=True
            )
        settings = settings.model_copy(
            update={key: manifest[key] for key in ("stage_timeout_seconds", "run_timeout_seconds")}
        )
        await store.recover_stages(run_id)
        snapshot = await load_snapshot(engine, settings.workspace, manifest["snapshot_id"])
        analyses = {
            key: (MartinReport if key == "martin" else WyckoffReport).model_validate(
                (await store.row(FACTOR_RESULTS, uuid))["payload"]
            )
            for key, uuid in manifest["factor_results"].items()
        }
        if any(value.pool for value in analyses.values()):
            if not connections:
                connections = await asyncio.to_thread(model_connections, settings)
            if {key: value.model_dump(mode="json") for key, value in connections.items()} != manifest["models"]:
                raise ValueError("冻结模型连接与当前配置不同，须强制重分析")
            try:
                await verify_runtime(settings.dsh_executable)
            except (ValueError, OSError):
                return await store.publish(
                    run_id, "partial" if current["selections"] else "failed", "executor_unavailable", track=True
                )
        return await execute_run(store, settings, snapshot, analyses, run_id, connections)


async def run_preview(
    engine: AsyncEngine,
    settings: Settings,
    snapshot: Snapshot,
    analyses: dict[str, MartinReport | WyckoffReport],
    target_session: date,
    *,
    parent_id: str | None = None,
    force: bool = False,
    parameter_sources: dict[str, object] | None = None,
) -> dict[str, object]:
    """CLI预览等待完整流程，HTTP可在冻结后独立调度同一恢复入口。"""
    run_id, connections = await prepare_preview(
        engine,
        settings,
        snapshot,
        analyses,
        target_session,
        parent_id=parent_id,
        force=force,
        parameter_sources=parameter_sources,
    )
    return await resume_preview(engine, settings, run_id, connections)


def model_preferences(settings: Settings) -> dict[str, object]:
    """幂等配置不依赖当前池是否为空，独立凭据文件轮换不改变初选版本。"""
    source = settings.model_profile_path or settings.model_env_path
    return {
        "profile": str(settings.model_profile_path),
        "env": str(settings.model_env_path),
        "source_hash": hashlib.sha256(source.read_bytes()).hexdigest() if source and source.is_file() else None,
        "provider": settings.model_provider,
        "model": settings.model,
        "api": settings.model_api,
        "models_by_role": settings.models_by_role,
        "output_tokens": settings.model_output_tokens,
    }
