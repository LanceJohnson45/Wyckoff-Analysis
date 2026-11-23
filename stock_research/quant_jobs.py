"""运行锁内执行缺失量化，失败不阻断另一策略，成功结果不重做。"""

import asyncio
from datetime import timedelta

import structlog

from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.errors import ResearchSourceChanged
from stock_research.identity import canonical_json, digest, now
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.quant_execution import QuantExecutionError, analyze_isolated
from stock_research.quant_store import finish_job, job_rows, start_job
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_RUNS
from stock_research.schemas.quant_execution import QUANT_TIMEOUT_SECONDS
from stock_research.wyckoff import WyckoffParameters

logger = structlog.get_logger(__name__)


async def run_quant_job(
    store: RunStore, settings: Settings, run: dict[str, object], row: dict[str, object], snapshot: Snapshot
) -> None:
    """只消费原run参数和PG归并后的快照ID，原120秒预算还服从总期限。"""
    if row["status"] == "completed":
        return
    if row["status"] == "running":
        await finish_job(store, run, row, error="previous_executor_stopped")
        row = next(item for item in await job_rows(store, run) if item["strategy"] == row["strategy"])
    manifest, strategy = run["payload"], row["strategy"]
    model = MartinParameters if strategy == "martin" else WyckoffParameters
    parameters = model.model_validate_json(canonical_json(manifest["parameters"][strategy]), strict=True)
    path = (
        settings.artifacts_path / "quant-inputs" / f"{digest({'id': snapshot.id, 'hash': snapshot.content_hash})}.json"
    )
    await asyncio.to_thread(write_once, path, snapshot.model_dump_json())
    for attempt in range(2):
        row = await start_job(store, row)
        await store.publish(run["uuid"], "running")
        remaining = (run["create_time"] + timedelta(seconds=settings.run_timeout_seconds) - now()).total_seconds()
        error = None
        try:
            if remaining <= 0:
                raise QuantExecutionError("quant_timeout")
            report = await analyze_isolated(
                settings, path, snapshot, strategy, timeout=min(QUANT_TIMEOUT_SECONDS, remaining), parameters=parameters
            )
            await finish_job(store, run, row, report=report)
        except asyncio.CancelledError:
            await finish_job(store, run, row, error="cancelled", cancelled=True)
            await store.publish(run["uuid"], "cancelled", track=True)
            raise
        except ResearchSourceChanged:
            await finish_job(store, run, row, error="quant_source_changed")
            raise
        except (QuantExecutionError, OSError, ValueError) as failure:
            error = failure.code if isinstance(failure, QuantExecutionError) else "quant_calculation_failed"
            logger.error("量化任务失败", strategy=strategy, error_code=error, error_type=type(failure).__name__)
            await finish_job(store, run, row, error=error)
        await store.publish(run["uuid"], "running")
        if error not in {"quant_timeout", "quant_process_failed"} or attempt or remaining <= 0:
            return
        row = next(item for item in await job_rows(store, run) if item["strategy"] == strategy)


async def run_quant_jobs(store: RunStore, settings: Settings, run_id: str, snapshot: Snapshot) -> None:
    """调用方持有run锁；每策略成功直接复用，失败尝试完整保留。"""
    run = await store.row(RESEARCH_RUNS, run_id)
    await store.publish(run_id, "running")
    for row in await job_rows(store, run):
        await run_quant_job(store, settings, run, row, snapshot)
