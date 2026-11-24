"""盘前意图复用持久请求账本；独立身份只能管理其绑定的运行。"""

from pydantic import ValidationError
from sqlalchemy import select, update

from stock_research.errors import ResearchConflict, ResearchIntegrityError, ResearchNotFound, ResearchUnavailable
from stock_research.execution_budget import run_deadline
from stock_research.identity import digest, now
from stock_research.premarket import plan_premarket
from stock_research.preparations import preparation_view
from stock_research.queries import latest_report_id, report_view, run_view
from stock_research.run_manager import RunManager, prepare_from_market
from stock_research.run_store import verified_payload
from stock_research.schema import API_REQUESTS, PREPARATION_ATTEMPTS, RESEARCH_RUNS
from stock_research.schemas.premarket import PremarketBinding, PremarketInput, PremarketPlan, PremarketReceipt
from stock_research.schemas.requests import CreateRun
from stock_research.source_guard import ensure_source_current


def request_identity(data: PremarketInput) -> tuple[str, dict[str, object]]:
    """认领、恢复和取消共同使用规范化后的同一个原事件身份。"""
    payload = data.model_dump(mode="json")
    return "premarket:" + digest(payload), {"premarket": payload}


def job_input(manager: RunManager, request: dict[str, object]) -> PremarketInput:
    """普通API请求和其他来源工作区都不属于这个能力。"""
    payload = verified_payload(request)
    if "premarket" not in payload["request"]:
        raise ResearchNotFound("盘前任务不存在")
    try:
        data = PremarketInput.model_validate(payload["request"]["premarket"])
    except ValidationError:
        raise ResearchIntegrityError("盘前原意图无效") from None
    if (
        not manager.settings.premarket_source_workspace
        or data.schedule.workspace_id != manager.settings.premarket_source_workspace
    ):
        raise ResearchNotFound("盘前任务不存在")
    return data


async def run_origin(
    manager: RunManager, run_id: str, data: PremarketInput
) -> tuple[PremarketBinding, dict[str, object]]:
    """别名只能引用同一能力/日期/触发类别的首次意图，不能替换其期限。"""
    row = await manager.store.row(RESEARCH_RUNS, run_id)
    try:
        binding = PremarketBinding.model_validate(row["payload"]["premarket"])
    except (KeyError, ValueError):
        raise ResearchIntegrityError("盘前运行绑定无效") from None
    if binding.schedule.research_scope() != data.schedule.research_scope():
        raise ResearchIntegrityError("盘前任务与运行范围不一致")
    origin = await manager.store.row(API_REQUESTS, str(binding.job_id))
    if job_input(manager, origin).schedule != binding.schedule or await manager.requests.frozen_run(origin) != run_id:
        raise ResearchIntegrityError("盘前首次意图与运行绑定不一致")
    return binding, origin


async def job_receipt(manager: RunManager, job_id: str) -> PremarketReceipt:
    """准备失败、未确认和已有报告均从持久数据读回，不把内存任务当成功。"""
    request = await manager.store.row(API_REQUESTS, job_id)
    data = job_input(manager, request)
    plan = PremarketPlan.model_validate(plan_premarket(data, request["create_time"]))
    async with manager.store.engine.connect() as connection:
        row = (
            (
                await connection.execute(
                    select(PREPARATION_ATTEMPTS)
                    .where(
                        PREPARATION_ATTEMPTS.c.workspace == manager.store.workspace,
                        PREPARATION_ATTEMPTS.c.request_uuid == job_id,
                        PREPARATION_ATTEMPTS.c.is_deleted.is_(False),
                    )
                    .order_by(PREPARATION_ATTEMPTS.c.create_time.desc(), PREPARATION_ATTEMPTS.c.uuid.desc())
                    .limit(1)
                )
            )
            .mappings()
            .one_or_none()
        )
    preparation = preparation_view(dict(row)) if row else None
    run_id = await manager.requests.frozen_run(request)
    run = report = binding = None
    status = "unconfirmed"
    if run_id:
        body = await manager.store.summary(run_id)
        binding, origin = await run_origin(manager, run_id, data)
        run = await run_view(manager.store, body)
        report = await report_view(manager.store, await latest_report_id(manager.store, run_id=run_id))
        cancelled = request["payload"].get("premarket_cancelled") or origin["payload"].get("premarket_cancelled")
        status = "cancelled" if cancelled else run.status
    elif request["payload"].get("premarket_cancelled"):
        status = "cancelled"
    elif plan.budget_seconds == 0:
        status = "skipped"
    elif now() >= plan.execution_deadline:
        status = "expired"
    elif preparation:
        status = (
            "failed_preparation"
            if preparation.status == "failed"
            else "cancelled"
            if preparation.status == "cancelled"
            else "unconfirmed"
        )
    return PremarketReceipt(
        job_id=job_id, plan=plan, origin=binding, status=status, preparation=preparation, run=run, report=report
    )


async def admit_job(manager: RunManager, data: PremarketInput) -> PremarketReceipt:
    """原事件只认领一次；同一请求失败可重试准备，原期限和意图不变。"""
    ensure_source_current()
    if (
        data.schedule.workspace_id != manager.settings.premarket_source_workspace
        or not manager.settings.premarket_source_workspace
    ):
        raise ResearchNotFound("盘前来源未授权")
    key, body = request_identity(data)
    async with manager.admission:
        if manager.closing:
            raise ResearchUnavailable("研究服务正在关闭")
        async with manager.requests.claim(key, body) as request:
            job_id = request["uuid"]
            job_input(manager, request)
            plan = PremarketPlan.model_validate(plan_premarket(data, request["create_time"]))
            run_id = await manager.requests.frozen_run(request)
            if (
                run_id is None
                and not request["payload"].get("premarket_cancelled")
                and plan.budget_seconds
                and now() < plan.execution_deadline
            ):
                if len(manager.tasks) + len(manager.peer_tasks) >= 2:
                    raise ResearchConflict("研究运行容量已满")
                binding = PremarketBinding(
                    job_id=job_id, schedule=data.schedule, execution_deadline=plan.execution_deadline
                )
                try:
                    run_id = await prepare_from_market(
                        manager.store,
                        manager.settings,
                        CreateRun(target_session=plan.target_session),
                        job_id,
                        premarket=binding,
                    )
                except ResearchUnavailable:
                    return await job_receipt(manager, job_id)
            if run_id:
                if request["run_uuid"] is None:
                    await manager.requests.bind(request, run_id)
                row = await manager.store.row(RESEARCH_RUNS, run_id)
                _, origin = await run_origin(manager, run_id, data)
                cancelled = request["payload"].get("premarket_cancelled") or origin["payload"].get(
                    "premarket_cancelled"
                )
                if row["status"] in {"pending", "running"} and not cancelled:
                    manager.start(run_id)
    return await job_receipt(manager, job_id)


async def retry_job(manager: RunManager, job_id: str) -> PremarketReceipt:
    """请求级锁序列化取消/恢复，原期限和成功阶段不重置。"""
    receipt = await job_receipt(manager, job_id)
    if receipt.run is None:
        raise ResearchConflict("盘前任务尚未冻结运行")
    key, body = request_identity(PremarketInput(schedule=receipt.origin.schedule))
    async with manager.requests.claim(key, body) as request:
        receipt = await job_receipt(manager, job_id)
        if (
            request["payload"].get("premarket_cancelled")
            or receipt.status == "cancelled"
            or receipt.run is None
            or now() >= receipt.plan.execution_deadline
        ):
            raise ResearchConflict("盘前任务不能重试")
        row = await manager.store.row(RESEARCH_RUNS, str(receipt.run.run_id))
        if now() >= run_deadline(row, row["payload"]["run_timeout_seconds"]):
            raise ResearchConflict("盘前运行的原预算已耗尽")
        await manager.retry(str(receipt.run.run_id))
    return await job_receipt(manager, job_id)


async def cancel_job(manager: RunManager, job_id: str) -> PremarketReceipt:
    """请求级锁及取消标记阻止另一个API拥有者复活已取消意图。"""
    async with manager.admission:
        previous = await manager.store.row(API_REQUESTS, job_id)
        data = job_input(manager, previous)
        run_id = await manager.requests.frozen_run(previous)
        if run_id:
            binding, _ = await run_origin(manager, run_id, data)
            data = PremarketInput(schedule=binding.schedule)
        key, body = request_identity(data)
        async with manager.requests.claim(key, body) as request:
            run_id = await manager.requests.frozen_run(request)
            if run_id:
                binding, _ = await run_origin(manager, run_id, data)
                if str(binding.job_id) != request["uuid"]:
                    raise ResearchConflict("盘前任务刚绑定共享运行，请重试取消")
                row = await manager.store.row(RESEARCH_RUNS, run_id)
                if row["status"] == "completed":
                    return await job_receipt(manager, job_id)
                if row["status"] in {"pending", "running"}:
                    await manager.cancel(run_id)
            if not request["payload"].get("premarket_cancelled"):
                payload = {**request["payload"], "premarket_cancelled": True}
                async with manager.store.engine.begin() as connection:
                    result = await connection.execute(
                        update(API_REQUESTS)
                        .where(
                            API_REQUESTS.c.uuid == request["uuid"],
                            API_REQUESTS.c.workspace == manager.store.workspace,
                            API_REQUESTS.c.content_hash == request["content_hash"],
                        )
                        .values(payload=payload, content_hash=digest(payload))
                    )
                    if result.rowcount != 1:
                        raise ResearchConflict("盘前任务在取消时已变化")
    return await job_receipt(manager, job_id)
