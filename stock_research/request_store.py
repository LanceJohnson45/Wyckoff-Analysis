"""HTTP幂等请求持久化；冻结中断后恢复映射，不重复强制重分析。"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from stock_research.errors import ResearchConflict
from stock_research.identity import digest, new_id
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import API_REQUESTS, RESEARCH_RUNS


class RequestStore:
    """请求身份只保存摘要；一个请求不能在重放时更换业务载荷。"""

    def __init__(self, store: RunStore) -> None:
        self.store = store

    @asynccontextmanager
    async def claim(self, key: str, body: dict[str, object]) -> AsyncIterator[dict[str, object]]:
        """会话锁覆盖冻结与绑定；进程崩溃后锁自动释放，pending映射仍可恢复。"""
        key_hash = digest({"workspace": self.store.workspace, "key": key})
        lock = int(key_hash[:16], 16) - (1 << 63)
        connection = await self.store.engine.connect()
        owned = False
        try:
            owned = bool(await connection.scalar(select(func.pg_try_advisory_lock(lock))))
            await connection.commit()
            if not owned:
                raise ResearchConflict("相同幂等请求正在冻结输入")
            payload = {"request": body, "run_id": None}
            async with connection.begin():
                await connection.execute(
                    insert(API_REQUESTS)
                    .values(
                        uuid=new_id(),
                        workspace=self.store.workspace,
                        key_hash=key_hash,
                        request_hash=digest(body),
                        run_uuid=None,
                        payload=payload,
                        content_hash=digest(payload),
                    )
                    .on_conflict_do_nothing(index_elements=["workspace", "key_hash"])
                )
                row = (
                    (
                        await connection.execute(
                            select(API_REQUESTS).where(
                                API_REQUESTS.c.workspace == self.store.workspace,
                                API_REQUESTS.c.key_hash == key_hash,
                                API_REQUESTS.c.is_deleted.is_(False),
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
            verified_payload(dict(row))
            if row["request_hash"] != digest(body):
                raise ResearchConflict("相同Idempotency-Key不能更换请求载荷")
            yield dict(row)
        finally:
            try:
                if owned:
                    await connection.execute(select(func.pg_advisory_unlock(lock)))
                    await connection.commit()
            finally:
                await asyncio.wait_for(connection.close(), 5)

    async def frozen_run(self, request: dict[str, object]) -> str | None:
        """原子绑定前崩溃时，按冻结清单中的请求UUID找回原运行。"""
        if request["run_uuid"]:
            await self.store.row(RESEARCH_RUNS, request["run_uuid"])
            return request["run_uuid"]
        async with self.store.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(RESEARCH_RUNS)
                        .where(
                            RESEARCH_RUNS.c.workspace == self.store.workspace,
                            RESEARCH_RUNS.c.is_deleted.is_(False),
                            RESEARCH_RUNS.c.payload["api_request_id"].as_string() == request["uuid"],
                        )
                        .order_by(RESEARCH_RUNS.c.create_time, RESEARCH_RUNS.c.uuid)
                        .limit(1)
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row:
            verified_payload(dict(row))
            return row["uuid"]
        return None

    async def bind(self, request: dict[str, object], run_id: str) -> None:
        """首次冻结结果只绑定一次，内容与外键同事务更新。"""
        await self.store.row(RESEARCH_RUNS, run_id)
        payload = {**request["payload"], "run_id": run_id}
        async with self.store.engine.begin() as connection:
            result = await connection.execute(
                update(API_REQUESTS)
                .where(
                    API_REQUESTS.c.uuid == request["uuid"],
                    API_REQUESTS.c.workspace == self.store.workspace,
                    API_REQUESTS.c.run_uuid.is_(None),
                )
                .values(run_uuid=run_id, payload=payload, content_hash=digest(payload))
            )
            if result.rowcount != 1 and request["run_uuid"] != run_id:
                raise ResearchConflict("幂等请求已绑定其他运行")
