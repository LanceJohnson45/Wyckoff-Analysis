"""按不可变报告修订续读的有限SSE流，不依赖进程内事件缓冲。"""

import asyncio
from collections.abc import AsyncIterator
from time import monotonic
from typing import Annotated

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import UUID4
from sqlalchemy import select

from stock_research.queries import report_view
from stock_research.schema import RESEARCH_REPORTS, RESEARCH_RUNS
from stock_research.schemas.api import ApiResponse, Code, ReportView


async def report_events(request: Request, run_id: str, cursor: int) -> AsyncIterator[str]:
    """每连接最多三十秒，游标是当前run内单调递增且持久化的revision。"""
    store = request.app.state.store
    expires = monotonic() + 30
    while monotonic() < expires and not await request.is_disconnected():
        async with store.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(RESEARCH_REPORTS.c.uuid, RESEARCH_REPORTS.c.revision)
                        .where(
                            RESEARCH_REPORTS.c.workspace == store.workspace,
                            RESEARCH_REPORTS.c.run_uuid == run_id,
                            RESEARCH_REPORTS.c.is_deleted.is_(False),
                            RESEARCH_REPORTS.c.revision > cursor,
                        )
                        .order_by(RESEARCH_REPORTS.c.revision)
                        .limit(20)
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            report = await report_view(store, row["uuid"])
            packet = ApiResponse[ReportView](
                code=Code.OK, message="ok", data=report, correlation_id=request.state.correlation_id
            )
            yield f"id: {row['revision']}\nevent: report\ndata: {packet.model_dump_json()}\n\n"
            cursor = row["revision"]
        run = await store.row(RESEARCH_RUNS, run_id)
        if not rows and run["status"] in {"completed", "partial", "failed", "cancelled"}:
            return
        yield ": heartbeat\n\n"
        await asyncio.sleep(1)


def register_events(app: FastAPI) -> None:
    """认证继承应用中间件，stream打开前先校验对象归属。"""

    @app.get("/v1/research-runs/{run_id}/events", response_class=StreamingResponse)
    async def events(
        request: Request,
        run_id: UUID4,
        last_event_id: Annotated[int, Header(alias="Last-Event-ID", ge=0)] = 0,
        after: Annotated[int, Query(ge=0)] = 0,
    ) -> StreamingResponse:
        await request.app.state.store.row(RESEARCH_RUNS, str(run_id))
        return StreamingResponse(
            report_events(request, str(run_id), max(after, last_event_id)),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )
