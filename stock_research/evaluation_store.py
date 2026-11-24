"""评估作业持久化：原意图幂等、workspace隔离和有界状态转换。"""

from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from stock_research.errors import ResearchConflict, ResearchIntegrityError, ResearchNotFound
from stock_research.identity import digest, new_id, now
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import EVALUATION_JOBS
from stock_research.schemas.evaluation_execution import EvaluationFiles, EvaluationFrame
from stock_research.schemas.evaluations import CreateEvaluation, EvaluationJobPage, EvaluationJobView, EvaluationStatus


def evaluation_view(row: dict[str, object]) -> EvaluationJobView:
    """状态列必须与完整payload一致，不让损坏状态驱动恢复。"""
    payload = verified_payload(row)
    if row["status"] != payload["status"]:
        raise ResearchIntegrityError("评估状态与账本不一致")
    return EvaluationJobView(
        id=row["uuid"],
        **payload["request"],
        status=payload["status"],
        completed_sections=payload["completed_sections"],
        error_code=payload["error_code"],
        result_id=payload["result_id"],
        cancel_requested=payload.get("cancel_requested", False),
        attempt_count=payload.get("attempt_count", 0),
        create_time=row["create_time"],
        update_time=row["update_time"],
    )


async def locked_evaluation(connection: AsyncConnection, store: RunStore, identifier: str) -> dict[str, object]:
    """评估变更都在同一租户/删除/摘要/状态校验后的行锁内执行。"""
    row = (
        (
            await connection.execute(
                select(EVALUATION_JOBS)
                .where(
                    EVALUATION_JOBS.c.workspace == store.workspace,
                    EVALUATION_JOBS.c.uuid == identifier,
                    EVALUATION_JOBS.c.is_deleted.is_(False),
                )
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise ResearchNotFound("评估对象不存在")
    result = dict(row)
    evaluation_view(result)
    return result


async def save_evaluation(
    connection: AsyncConnection, store: RunStore, row: dict[str, object], payload: dict[str, object]
) -> None:
    """行锁中的统一写入同时更新查询状态和完整正文摘要。"""
    fingerprint = digest(payload)
    evaluation_view({**row, "payload": payload, "content_hash": fingerprint, "status": payload["status"]})
    await connection.execute(
        update(EVALUATION_JOBS)
        .where(
            EVALUATION_JOBS.c.uuid == row["uuid"],
            EVALUATION_JOBS.c.workspace == store.workspace,
        )
        .values(status=payload["status"], payload=payload, content_hash=fingerprint)
    )


class EvaluationStore:
    """只读API读取小账本，不加载行情全文或启动计算。"""

    def __init__(self, store: RunStore) -> None:
        self.store = store

    async def request(self, key: str, body: CreateEvaluation) -> dict[str, object] | None:
        """原请求恢复先读账本，不能依赖当前配置文件仍然存在。"""
        key_hash = digest({"workspace": self.store.workspace, "evaluation": key})
        async with self.store.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(EVALUATION_JOBS).where(
                            EVALUATION_JOBS.c.workspace == self.store.workspace,
                            EVALUATION_JOBS.c.key_hash == key_hash,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        result = dict(row)
        evaluation_view(result)
        if result["is_deleted"] or result["request_hash"] != digest(body.model_dump(mode="json")):
            raise ResearchConflict("评估幂等键载荷已改变或任务已删除")
        return result

    async def create(self, key: str, body: CreateEvaluation, configuration: dict[str, object]) -> dict[str, object]:
        """同一workspace一次只接收一批评估；原请求重放不重消费配置。"""
        key_hash = digest({"workspace": self.store.workspace, "evaluation": key})
        request = body.model_dump(mode="json")
        async with self.store.engine.begin() as connection:
            lock = int(digest({"evaluation_admission": self.store.workspace})[:16], 16) - (1 << 63)
            await connection.execute(select(func.pg_advisory_xact_lock(lock)))
            existing = (
                (
                    await connection.execute(
                        select(EVALUATION_JOBS).where(
                            EVALUATION_JOBS.c.workspace == self.store.workspace,
                            EVALUATION_JOBS.c.key_hash == key_hash,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing:
                row = dict(existing)
                evaluation_view(row)
                if row["request_hash"] != digest(request) or row["is_deleted"]:
                    raise ResearchConflict("同一评估幂等键不能更换载荷或重建已删除任务")
                return row
            active = await connection.scalar(
                select(func.count())
                .select_from(EVALUATION_JOBS)
                .where(
                    EVALUATION_JOBS.c.workspace == self.store.workspace,
                    EVALUATION_JOBS.c.is_deleted.is_(False),
                    EVALUATION_JOBS.c.status.in_(("pending", "running")),
                )
            )
            if active:
                raise ResearchConflict("当前工作区已有未完成批量评估")
            identifier = new_id()
            payload = dict(
                request=request,
                configuration=configuration,
                status="pending",
                completed_sections=0,
                error_code=None,
                result_id=None,
                cancel_requested=False,
                attempt_count=0,
            )
            await connection.execute(
                insert(EVALUATION_JOBS).values(
                    uuid=identifier,
                    workspace=self.store.workspace,
                    key_hash=key_hash,
                    request_hash=digest(request),
                    status="pending",
                    payload=payload,
                    content_hash=digest(payload),
                )
            )
        return await self.read(identifier)

    async def read(self, identifier: str) -> dict[str, object]:
        """通用账本读取校验摘要，评估视图再校验状态列。"""
        row = await self.store.row(EVALUATION_JOBS, identifier)
        evaluation_view(row)
        return row

    async def page(self, page: int, page_size: int) -> EvaluationJobPage:
        """COUNT和LIMIT/OFFSET使用相同workspace及软删除过滤。"""
        base = select(EVALUATION_JOBS).where(
            EVALUATION_JOBS.c.workspace == self.store.workspace,
            EVALUATION_JOBS.c.is_deleted.is_(False),
        )
        async with self.store.engine.connect() as connection:
            total = await connection.scalar(select(func.count()).select_from(base.subquery()))
            rows = (
                (
                    await connection.execute(
                        base.order_by(
                            EVALUATION_JOBS.c.create_time.desc(),
                            EVALUATION_JOBS.c.uuid.desc(),
                        )
                        .limit(page_size)
                        .offset((page - 1) * page_size)
                    )
                )
                .mappings()
                .all()
            )
        return EvaluationJobPage(
            items=tuple(evaluation_view(dict(row)) for row in rows), total=total, page=page, page_size=page_size
        )

    async def transition(
        self,
        identifier: str,
        status: EvaluationStatus,
        *,
        expected: tuple[str, ...],
        completed_sections: int | None = None,
        error_code: str | None = None,
    ) -> bool:
        """取消与晚到worker竞争由事务CAS决定，终态不能被进度反转。"""
        async with self.store.engine.begin() as connection:
            row = await locked_evaluation(connection, self.store, identifier)
            payload = row["payload"]
            if row["status"] not in expected:
                return False
            if status == "completed" or row["status"] not in {"pending", "running"}:
                raise ResearchConflict("评估转换不允许覆盖终态或跳过结果提交")
            payload = {**payload, "status": status, "error_code": error_code}
            if completed_sections is not None:
                if not payload["completed_sections"] <= completed_sections <= 20:
                    raise ResearchConflict("评估进度不能倒退或越界")
                payload["completed_sections"] = completed_sections
            await save_evaluation(connection, self.store, row, payload)
        return True

    async def configure(self, identifier: str, configuration: dict[str, object]) -> None:
        """冻结清单只写一次，恢复不能把后来参数和输入拼入原任务。"""
        async with self.store.engine.begin() as connection:
            row = await locked_evaluation(connection, self.store, identifier)
            payload = row["payload"]
            if row["status"] not in {"pending", "running"}:
                raise ResearchConflict("终态评估不能重新冻结")
            current = payload["configuration"]
            if current.get("files") is not None:
                if current != configuration:
                    raise ResearchConflict("评估已经冻结其他清单")
                return
            if current.get("source_hash") != configuration.get("source_hash") or current.get(
                "deadline"
            ) != configuration.get("deadline"):
                raise ResearchConflict("评估源码和期限不能重新开始")
            EvaluationFiles.model_validate(configuration.get("files"))
            await save_evaluation(connection, self.store, row, {**payload, "configuration": configuration})

    async def start_attempt(self, identifier: str) -> dict[str, object]:
        """保留总尝试数，重启和人工续跑共用原预算而非无限重试。"""
        async with self.store.engine.begin() as connection:
            row = await locked_evaluation(connection, self.store, identifier)
            payload = row["payload"]
            if (
                row["status"] not in {"pending", "running"}
                or payload.get("cancel_requested")
                or payload.get("attempt_count", 0) >= 2
            ):
                raise ResearchConflict("评估状态或剩余尝试数不允许执行")
            payload = {
                **payload,
                "status": "running",
                "error_code": None,
                "attempt_count": payload.get("attempt_count", 0) + 1,
            }
            await save_evaluation(connection, self.store, row, payload)
        return await self.read(identifier)

    async def progress(self, identifier: str, frame: EvaluationFrame) -> None:
        """冻结行情摘要先落库；恢复重放的已完成截面不让总进度倒退。"""
        if frame.status not in {"started", "frozen", "section"}:
            return
        async with self.store.engine.begin() as connection:
            row = await locked_evaluation(connection, self.store, identifier)
            payload = row["payload"]
            if row["status"] != "running":
                return
            configuration = payload["configuration"]
            files = EvaluationFiles.model_validate(configuration.get("files"))
            if frame.status == "started":
                if files.source_hash != frame.source_hash:
                    raise ResearchIntegrityError("评估worker源码不一致")
                await save_evaluation(connection, self.store, row, {**payload, "worker_pid": frame.pid})
                return
            if files.source_hash != frame.source_hash or files.expected_input_hash not in {None, frame.input_hash}:
                raise ResearchIntegrityError("评估进度引用了另一份输入")
            frozen = files.model_copy(update={"expected_input_hash": frame.input_hash})
            payload = {
                **payload,
                "configuration": {**configuration, "files": frozen.model_dump(mode="json")},
                "completed_sections": max(payload["completed_sections"], frame.completed_sections),
            }
            await save_evaluation(connection, self.store, row, payload)

    async def request_cancel(self, identifier: str) -> None:
        """先记录取消意图，只有执行者确实回收进程后才提交cancelled。"""
        async with self.store.engine.begin() as connection:
            row = await locked_evaluation(connection, self.store, identifier)
            if row["status"] in {"pending", "running"}:
                await save_evaluation(connection, self.store, row, {**row["payload"], "cancel_requested": True})

    async def retry(self, identifier: str, source_hash: str) -> None:
        """显式续跑保持原输入/期限；失败不可重建无限预算。"""
        async with self.store.engine.begin() as connection:
            row = await locked_evaluation(connection, self.store, identifier)
            payload = row["payload"]
            if row["status"] not in {"failed", "cancelled"} or payload.get("attempt_count", 0) >= 2:
                raise ResearchConflict("评估不在可续跑状态或尝试数已用完")
            if payload.get("error_code") in {"evaluation_history_insufficient", "evaluation_quality_failed"}:
                raise ResearchConflict("冻结输入资料不足或质量未通过，请补齐资料后创建新评估")
            configuration = payload["configuration"]
            if (
                configuration.get("source_hash") != source_hash
                or datetime.fromisoformat(configuration["deadline"]) <= now()
            ):
                raise ResearchConflict("评估源码或原期限不允许续跑，请创建新实验")
            await save_evaluation(
                connection,
                self.store,
                row,
                {**payload, "status": "pending", "cancel_requested": False, "error_code": None},
            )
