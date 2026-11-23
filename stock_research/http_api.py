"""独立loopback研究API；只公开固定workspace的研究业务视图。"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

import structlog
from fastapi import FastAPI, Query, Request
from pydantic import UUID4
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from stock_research.config import Settings
from stock_research.http_actions import register_actions
from stock_research.http_events import register_events
from stock_research.http_observations import register_observations
from stock_research.http_reports import register_reports
from stock_research.http_security import install_security
from stock_research.http_watchlists import register_watchlists
from stock_research.queries import list_runs, run_view
from stock_research.run_manager import RunManager
from stock_research.run_store import RunStore
from stock_research.schema import API_REQUESTS, RESEARCH_RUNS, WATCHLIST_DAYS, WATCHLIST_EVENTS, WATCHLIST_ITEMS
from stock_research.schemas.api import ApiResponse, Code, HealthView, RunPage, RunView
from stock_research.store import close_engine, database_engine

RUNTIME_TABLES = (RESEARCH_RUNS, API_REQUESTS, WATCHLIST_ITEMS, WATCHLIST_EVENTS, WATCHLIST_DAYS)


def create_app(settings: Settings | None = None) -> FastAPI:
    """启动检查真实账本可读性，不在多进程启动阶段自动迁移表。"""
    config = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = database_engine(config)
        manager = None
        try:
            async with engine.connect() as connection:
                for table in RUNTIME_TABLES:
                    await connection.execute(select(table.c.uuid).limit(0))
            app.state.store = RunStore(engine, config.workspace)
            manager = app.state.manager = RunManager(app.state.store, config)
            await manager.restore()
            yield
        finally:
            try:
                if manager:
                    await asyncio.wait_for(manager.close(), 25)
            except (TimeoutError, SQLAlchemyError, OSError):
                structlog.get_logger(__name__).error("研究API关闭未完成，未提交状态待恢复")
            finally:
                await close_engine(engine)

    app = FastAPI(
        title="Stock Research",
        version="1",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        responses={status: {"model": ApiResponse[None]} for status in (401, 403, 404, 409, 422, 500, 503)},
    )
    install_security(app, config)
    register_reads(app)
    register_reports(app)
    register_observations(app)
    register_watchlists(app)
    register_events(app)
    register_actions(app)
    return app


def register_reads(app: FastAPI) -> None:
    """读取不启动模型、刷新数据或更改已保存初选。"""

    @app.get("/healthz", response_model=ApiResponse[HealthView])
    async def health(request: Request) -> ApiResponse[HealthView]:
        return ApiResponse(
            code=Code.OK, message="ok", data=HealthView(status="alive"), correlation_id=request.state.correlation_id
        )

    @app.get("/readyz", response_model=ApiResponse[HealthView])
    async def ready(request: Request) -> ApiResponse[HealthView]:
        async with request.app.state.store.engine.connect() as connection:
            for table in RUNTIME_TABLES:
                await connection.execute(select(table.c.uuid).limit(0))
        return ApiResponse(
            code=Code.OK, message="ok", data=HealthView(status="ready"), correlation_id=request.state.correlation_id
        )

    @app.get("/v1/research-runs", response_model=ApiResponse[RunPage])
    async def runs(
        request: Request, page: Annotated[int, Query(ge=1)] = 1, page_size: Annotated[int, Query(ge=1, le=100)] = 20
    ) -> ApiResponse[RunPage]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await list_runs(request.app.state.store, page, page_size),
            correlation_id=request.state.correlation_id,
        )

    @app.get("/v1/research-runs/{run_id}", response_model=ApiResponse[RunView])
    async def run(request: Request, run_id: UUID4) -> ApiResponse[RunView]:
        store = request.app.state.store
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await run_view(store, await store.summary(str(run_id))),
            correlation_id=request.state.correlation_id,
        )
