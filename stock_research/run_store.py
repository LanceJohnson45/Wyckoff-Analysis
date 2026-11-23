"""研究执行账本：workspace隔离、提交后初选引用及并发恢复。"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import Table, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from stock_research.analysis import MartinReport
from stock_research.decisions import ReviewDecision, SelectionDecision, validate_decision
from stock_research.errors import ResearchConflict, ResearchIntegrityError, ResearchNotFound
from stock_research.evidence import EvidenceContext, build_context
from stock_research.executor import StageResult
from stock_research.identity import digest, new_id
from stock_research.schema import (
    CROSS_REVIEWS,
    FACTOR_RESULTS,
    PARAMETER_SETS,
    RESEARCH_REPORTS,
    RESEARCH_RUNS,
    SELECTION_ITEMS,
    STAGE_RUNS,
    STRATEGY_SELECTIONS,
    STRATEGY_VERSIONS,
)
from stock_research.store import load_snapshot
from stock_research.wyckoff import WyckoffReport


def verified_payload(row: dict[str, object]) -> dict[str, object]:
    """查询和恢复均校验payload，不信任单独的状态或文件存在性。"""
    if digest(row["payload"]) != row["content_hash"]:
        raise ResearchIntegrityError("研究账本payload摘要不匹配")
    return row["payload"]


async def put_artifact(
    connection: AsyncConnection, table: Table, workspace: str, payload: dict[str, object], **fields: object
) -> str:
    """不可变内容按摘要复用，调用者的业务唯一约束冲突须由事务拒绝。"""
    content_hash = digest(payload)
    statement = insert(table).values(
        uuid=new_id(), workspace=workspace, payload=payload, content_hash=content_hash, **fields
    )
    created = await connection.scalar(
        statement.on_conflict_do_nothing(index_elements=["workspace", "content_hash"]).returning(table.c.uuid)
    )
    if created:
        return created
    return await connection.scalar(
        select(table.c.uuid).where(table.c.workspace == workspace, table.c.content_hash == content_hash)
    )


class RunStore:
    """所有查询保留workspace约束；成功初选和互评只可追加。"""

    def __init__(self, engine: AsyncEngine, workspace: str) -> None:
        self.engine, self.workspace = engine, workspace

    async def row(self, table: Table, uuid: str) -> dict[str, object]:
        """未知或跨workspace对象均返回同一种缺失错误。"""
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(table).where(
                            table.c.uuid == uuid, table.c.workspace == self.workspace, table.c.is_deleted.is_(False)
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise ResearchNotFound("研究对象不存在或不属于当前workspace")
        result = dict(row)
        verified_payload(result)
        return result

    async def artifacts(
        self, snapshot_id: str, analyses: dict[str, dict[str, object]], source_hash: str, prompt_hash: str
    ) -> dict[str, str]:
        """完整参数、策略出处和量化产物在同一事务内注册。"""
        result = {}
        async with self.engine.begin() as connection:
            for strategy, payload in analyses.items():
                strategy_id = await put_artifact(
                    connection,
                    STRATEGY_VERSIONS,
                    self.workspace,
                    {"strategy": strategy, "source_hash": source_hash, "prompt_hash": prompt_hash},
                )
                parameter_id = await put_artifact(
                    connection,
                    PARAMETER_SETS,
                    self.workspace,
                    {"strategy": strategy, "parameters": payload["parameters"]},
                )
                result[strategy] = await put_artifact(
                    connection,
                    FACTOR_RESULTS,
                    self.workspace,
                    payload,
                    snapshot_uuid=snapshot_id,
                    strategy=strategy,
                    strategy_uuid=strategy_id,
                    parameter_uuid=parameter_id,
                )
        return result

    async def create_run(
        self, manifest: dict[str, object], *, parent_id: str | None = None, run_key: str | None = None
    ) -> str:
        """同一冻结输入只创建一次，重分析由显式nonce产生新run_key。"""
        if parent_id:
            await self.row(RESEARCH_RUNS, parent_id)
        key = run_key or digest(manifest)
        async with self.engine.begin() as connection:
            statement = insert(RESEARCH_RUNS).values(
                uuid=new_id(),
                workspace=self.workspace,
                snapshot_uuid=manifest["snapshot_id"],
                run_key=key,
                status="pending",
                parent_run_uuid=parent_id,
                payload=manifest,
                content_hash=digest(manifest),
            )
            created = await connection.scalar(
                statement.on_conflict_do_nothing(index_elements=["workspace", "run_key"]).returning(
                    RESEARCH_RUNS.c.uuid
                )
            )
            if created:
                return created
            row = (
                (
                    await connection.execute(
                        select(RESEARCH_RUNS).where(
                            RESEARCH_RUNS.c.workspace == self.workspace, RESEARCH_RUNS.c.run_key == key
                        )
                    )
                )
                .mappings()
                .one()
            )
            verified_payload(dict(row))
            if row["parent_run_uuid"] != parent_id:
                raise ValueError("研究运行父版本冲突")
            return row["uuid"]

    async def find_run(self, run_key: str) -> dict[str, object] | None:
        """按日期及配置幂等键找首个已冻结输入，不以新行情替换旧初选。"""
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(RESEARCH_RUNS).where(
                            RESEARCH_RUNS.c.workspace == self.workspace,
                            RESEARCH_RUNS.c.run_key == run_key,
                            RESEARCH_RUNS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is not None:
            verified_payload(dict(row))
        return dict(row) if row else None

    async def recover_stages(self, run_id: str, error: str = "previous_executor_stopped") -> None:
        """持有运行锁后回收遗留running；已提交初选的completed阶段不改变。"""
        await self.row(RESEARCH_RUNS, run_id)
        async with self.engine.begin() as connection:
            rows = (
                (
                    await connection.execute(
                        select(STAGE_RUNS)
                        .where(
                            STAGE_RUNS.c.run_uuid == run_id,
                            STAGE_RUNS.c.workspace == self.workspace,
                            STAGE_RUNS.c.status == "running",
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .all()
            )
            for row in rows:
                payload = verified_payload(dict(row))
                result = StageResult(
                    context_hash=payload["context"]["content_hash"],
                    status="failed",
                    decision=None,
                    error=error,
                    receipt_path="",
                )
                body = {**payload, "result": result.model_dump(mode="json")}
                await connection.execute(
                    update(STAGE_RUNS)
                    .where(STAGE_RUNS.c.uuid == row["uuid"], STAGE_RUNS.c.workspace == self.workspace)
                    .values(status="failed", payload=body, content_hash=digest(body))
                )

    @asynccontextmanager
    async def execution_lock(self, run_id: str) -> AsyncIterator[None]:
        """session级锁跨阶段事务持有，进程死亡后由PostgreSQL自动释放。"""
        await self.row(RESEARCH_RUNS, run_id)
        key = int(digest({"workspace": self.workspace, "run": run_id})[:16], 16) - (1 << 63)
        connection = await self.engine.connect()
        acquired = False
        try:
            acquired = bool(await connection.scalar(select(func.pg_try_advisory_lock(key))))
            await connection.commit()
            if not acquired:
                raise ResearchConflict("当前研究运行已有执行者")
            yield
        finally:
            try:
                if acquired:
                    await connection.execute(select(func.pg_advisory_unlock(key)))
                    await connection.commit()
            finally:
                await asyncio.wait_for(connection.close(), 5)

    async def ensure_stage(self, run_id: str, strategy: str, kind: str, target_id: str | None = None) -> str:
        """稳定stage_key保证重启重试定位同一阶段，成功版本保持不变。"""
        if (
            strategy not in {"martin", "wyckoff"}
            or kind not in {"select", "review"}
            or (kind == "review") != bool(target_id)
        ):
            raise ValueError("阶段策略、类型或目标无效")
        await self.row(RESEARCH_RUNS, run_id)
        if target_id:
            target = await self.row(STRATEGY_SELECTIONS, target_id)
            if target["run_uuid"] != run_id or target["strategy"] == strategy:
                raise ValueError("互评目标不属于当前运行的另一策略")
        key = digest({"strategy": strategy, "kind": kind, "target": target_id})
        async with self.engine.begin() as connection:
            statement = insert(STAGE_RUNS).values(
                uuid=new_id(),
                workspace=self.workspace,
                run_uuid=run_id,
                stage_key=key,
                strategy=strategy,
                kind=kind,
                status="pending",
                attempt_count=0,
                target_selection_uuid=target_id,
                payload={},
                content_hash=digest({}),
            )
            created = await connection.scalar(
                statement.on_conflict_do_nothing(index_elements=["run_uuid", "stage_key"]).returning(STAGE_RUNS.c.uuid)
            )
            return created or await connection.scalar(
                select(STAGE_RUNS.c.uuid).where(
                    STAGE_RUNS.c.run_uuid == run_id,
                    STAGE_RUNS.c.stage_key == key,
                    STAGE_RUNS.c.workspace == self.workspace,
                )
            )

    async def start_stage(self, stage_id: str, context: EvidenceContext) -> None:
        """attempt先提交，再启动外部进程；中断后running可被显式恢复。"""
        row = await self.row(STAGE_RUNS, stage_id)
        if (
            row["status"] not in {"pending", "failed", "cancelled"}
            or context.workspace != self.workspace
            or context.run_id != row["run_uuid"]
            or context.stage_id != stage_id
            or context.strategy != row["strategy"]
            or context.role != row["kind"]
        ):
            raise ValueError("阶段身份冲突或已成功，不可覆盖")
        await self.validate_context(row, context)
        payload = {
            "context": context.model_dump(mode="json"),
            "attempt_receipts": row["payload"].get("attempt_receipts", []),
        }
        async with self.engine.begin() as connection:
            changed = await connection.execute(
                update(STAGE_RUNS)
                .where(
                    STAGE_RUNS.c.uuid == stage_id,
                    STAGE_RUNS.c.workspace == self.workspace,
                    STAGE_RUNS.c.attempt_count == row["attempt_count"],
                    STAGE_RUNS.c.content_hash == row["content_hash"],
                    STAGE_RUNS.c.status == row["status"],
                )
                .values(
                    status="running",
                    attempt_count=STAGE_RUNS.c.attempt_count + 1,
                    payload=payload,
                    content_hash=digest(payload),
                )
            )
            if changed.rowcount != 1:
                raise ValueError("阶段已被另一个执行者领取")

    async def validate_context(self, stage: dict[str, object], context: EvidenceContext) -> None:
        """从独立存储重建授权上下文，不能仅凭一个自报摘要扩展范围。"""
        run = await self.row(RESEARCH_RUNS, stage["run_uuid"])
        manifest = run["payload"]
        if context.snapshot_id != run["snapshot_uuid"] or context.snapshot_hash != manifest["snapshot_hash"]:
            raise ValueError("阶段引用了其他快照")
        artifact = await self.row(FACTOR_RESULTS, manifest["factor_results"][context.strategy])
        model = MartinReport if context.strategy == "martin" else WyckoffReport
        analysis = model.model_validate(artifact["payload"])
        if context.role == "select":
            targets = ()
        else:
            target = await self.row(STRATEGY_SELECTIONS, stage["target_selection_uuid"])
            targets = tuple(item["symbol"] for item in target["payload"]["decision"]["items"])
        snapshot = await load_snapshot(self.engine, self.workspace, context.snapshot_id)
        expected = build_context(
            snapshot,
            analysis,
            workspace=self.workspace,
            run_id=context.run_id,
            stage_id=context.stage_id,
            role=context.role,
            deadline=context.deadline,
            targets=targets,
        )
        if (
            expected.content_hash != context.content_hash
            or digest(context.model_dump(mode="json", exclude={"content_hash"})) != expected.content_hash
        ):
            raise ValueError("阶段证据与已提交快照、策略或目标不一致")

    async def finish_stage(self, stage_id: str, context: EvidenceContext, result: StageResult) -> str | None:
        """初选/互评及完成状态同事务提交，失败不会产生空初选。"""
        row = await self.row(STAGE_RUNS, stage_id)
        if (
            row["status"] != "running"
            or row["payload"]["context"] != context.model_dump(mode="json")
            or result.context_hash != context.content_hash
        ):
            raise ValueError("阶段提交与当前执行身份不一致")
        payload = {
            **row["payload"],
            "result": result.model_dump(mode="json"),
            "attempt_receipts": [*row["payload"].get("attempt_receipts", []), result.receipt_path],
        }
        committed = None
        async with self.engine.begin() as connection:
            if result.status == "completed":
                if result.decision is None:
                    raise ValueError("完成阶段缺少结构化结论")
                decision = validate_decision(
                    context, result.decision.model_dump_json(), target_selection_id=row["target_selection_uuid"]
                )
                committed = await self.commit_decision(connection, row, decision, result)
            elif result.decision is not None:
                raise ValueError("失败阶段不能携带正式结论")
            changed = await connection.execute(
                update(STAGE_RUNS)
                .where(
                    STAGE_RUNS.c.uuid == stage_id,
                    STAGE_RUNS.c.workspace == self.workspace,
                    STAGE_RUNS.c.status == "running",
                    STAGE_RUNS.c.content_hash == row["content_hash"],
                    STAGE_RUNS.c.attempt_count == row["attempt_count"],
                )
                .values(status=result.status, payload=payload, content_hash=digest(payload))
            )
            if changed.rowcount != 1:
                raise ValueError("阶段提交已失去领取权")
        return committed

    async def commit_decision(
        self,
        connection: AsyncConnection,
        stage: dict[str, object],
        decision: SelectionDecision | ReviewDecision,
        result: StageResult,
    ) -> str:
        """目标外键仅指向已提交初选，数据库唯一约束拒绝第二份初选。"""
        uuid = new_id()
        body = {
            "decision": decision.model_dump(mode="json"),
            "receipt_path": result.receipt_path,
            "context_hash": result.context_hash,
            "source": result.source,
        }
        fields = {
            "uuid": uuid,
            "workspace": self.workspace,
            "run_uuid": stage["run_uuid"],
            "stage_uuid": stage["uuid"],
            "payload": body,
            "content_hash": digest(body),
        }
        if isinstance(decision, SelectionDecision):
            await connection.execute(insert(STRATEGY_SELECTIONS).values(**fields, strategy=decision.strategy))
            for rank, item in enumerate(decision.items, 1):
                value = item.model_dump(mode="json")
                await connection.execute(
                    insert(SELECTION_ITEMS).values(
                        uuid=new_id(),
                        workspace=self.workspace,
                        selection_uuid=uuid,
                        symbol=item.symbol,
                        rank=rank,
                        payload=value,
                        content_hash=digest(value),
                    )
                )
        else:
            target = (
                (
                    await connection.execute(
                        select(STRATEGY_SELECTIONS).where(
                            STRATEGY_SELECTIONS.c.uuid == decision.target_selection_id,
                            STRATEGY_SELECTIONS.c.workspace == self.workspace,
                            STRATEGY_SELECTIONS.c.run_uuid == stage["run_uuid"],
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if target is None or target["strategy"] == decision.strategy:
                raise ValueError("互评目标不存在或不属于另一策略")
            await connection.execute(
                insert(CROSS_REVIEWS).values(
                    **fields, reviewer_strategy=decision.strategy, target_selection_uuid=decision.target_selection_id
                )
            )
        return uuid

    async def summary(self, run_id: str) -> dict[str, object]:
        """独立查询恢复所有已提交版本，未完成阶段继续显式展示。"""
        run = await self.row(RESEARCH_RUNS, run_id)
        result = {
            "run_id": run_id,
            "status": run["status"],
            "error_code": run["error_code"],
            "manifest": run["payload"],
            "selections": [],
            "reviews": [],
            "stages": [],
        }
        async with self.engine.connect() as connection:
            for key, table in (("selections", STRATEGY_SELECTIONS), ("reviews", CROSS_REVIEWS), ("stages", STAGE_RUNS)):
                rows = (
                    (
                        await connection.execute(
                            select(table)
                            .where(
                                table.c.run_uuid == run_id,
                                table.c.workspace == self.workspace,
                                table.c.is_deleted.is_(False),
                            )
                            .order_by(table.c.create_time, table.c.uuid)
                        )
                    )
                    .mappings()
                    .all()
                )
                for row in rows:
                    payload = verified_payload(dict(row))
                    if key == "stages":
                        payload = {
                            "context_hash": payload.get("context", {}).get("content_hash"),
                            "result": {
                                key: value for key, value in payload.get("result", {}).items() if key != "decision"
                            },
                        }
                    result[key].append(
                        {
                            "id": row["uuid"],
                            "strategy": row.get("strategy", row.get("reviewer_strategy")),
                            "payload": payload,
                            **(
                                {
                                    "kind": row["kind"],
                                    "status": row["status"],
                                    "attempt_count": row["attempt_count"],
                                    "target_selection_id": row["target_selection_uuid"],
                                }
                                if key == "stages"
                                else {}
                            ),
                        }
                    )
        return result

    async def publish(self, run_id: str, status: str, error_code: str | None = None) -> dict[str, object]:
        """报告修订只追加，内容相同的补读不创建重复版本。"""
        if status not in {"pending", "running", "completed", "partial", "failed", "cancelled"}:
            raise ValueError("未知研究运行状态")
        await self.row(RESEARCH_RUNS, run_id)
        async with self.engine.begin() as connection:
            await connection.execute(
                update(RESEARCH_RUNS)
                .where(RESEARCH_RUNS.c.uuid == run_id, RESEARCH_RUNS.c.workspace == self.workspace)
                .values(status=status, error_code=error_code)
            )
        body = await self.summary(run_id)
        async with self.engine.begin() as connection:
            revision = await connection.scalar(
                select(func.coalesce(func.max(RESEARCH_REPORTS.c.revision), 0) + 1).where(
                    RESEARCH_REPORTS.c.run_uuid == run_id, RESEARCH_REPORTS.c.workspace == self.workspace
                )
            )
            await put_artifact(connection, RESEARCH_REPORTS, self.workspace, body, run_uuid=run_id, revision=revision)
        return body
