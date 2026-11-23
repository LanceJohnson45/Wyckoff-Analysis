"""真实量化与PostgreSQL编排；受控执行器只用于故障注入，不代替真实模型验收。"""

import asyncio
from pathlib import Path

import pytest

from stock_research.analysis import compute_martin
from stock_research.config import Settings
from stock_research.decisions import (
    EvidenceClaim,
    ResearchPoint,
    ReviewDecision,
    ReviewItem,
    SelectionDecision,
    SelectionItem,
)
from stock_research.executor import StageResult
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.pipeline import run_preview
from stock_research.quality import build_snapshot
from stock_research.store import close_engine, database_engine, initialize_schema
from stock_research.wyckoff import WyckoffParameters, compute_wyckoff

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


def test_partial_run_resumes_failed_stage_without_replacing_selection(
    postgres_settings: Settings, snapshot: Snapshot, trending_bars, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """注入一次结构错误后续跑：已提交空初选不重算，互评只在另一初选提交后执行。"""
    from stock_research import pipeline

    bars = tuple(bar for bar in snapshot.bars if bar.kind == "benchmark") + trending_bars
    valid = build_snapshot(snapshot.universe, snapshot.classifications, bars, "synthetic", snapshot.as_of)
    analyses = {
        "martin": compute_martin(valid, MartinParameters()),
        "wyckoff": compute_wyckoff(valid, WyckoffParameters()),
    }
    assert analyses["martin"].pool == ("AAPL",)
    trusted = tmp_path / "model.env"
    trusted.write_text(
        "DSH_E2E_MODEL_PROVIDER=fixture\nDSH_E2E_MODEL_ID=fixture\nDSH_E2E_MODEL_BASE_URL=https://example.invalid/v1\nDSH_E2E_MODEL_API_KEY=fixture\n"
    )
    fail = True
    seen = []

    async def verified(_path: Path) -> None:
        return None

    async def controlled(_path, context, _connection, **kwargs) -> StageResult:
        nonlocal fail
        seen.append((context.strategy, context.role))
        if fail:
            fail = False
            return StageResult(
                context_hash=context.content_hash,
                status="failed",
                decision=None,
                error="invalid_output",
                receipt_path="fixture-failure",
            )
        if context.role == "select":
            assert kwargs["target"] is None
            ref = "martin/AAPL/factors/close"
            point = ResearchPoint(text="合成执行器验证引用提交", evidence_refs=(ref,))
            item = SelectionItem(
                symbol="AAPL",
                rationale=point,
                risks=(point,),
                confirmation_conditions=(point,),
                claims=(EvidenceClaim(ref=ref, value=context.resolve(ref).value),),
            )
            decision = SelectionDecision(strategy=context.strategy, context_hash=context.content_hash, items=(item,))
        else:
            target = kwargs["target"]
            assert target["decision"]["items"][0]["symbol"] == "AAPL"
            ref = "wyckoff/AAPL/calculation_available"
            point = ResearchPoint(text="本策略缺少结构计算，暂不支持确认", evidence_refs=(ref,))
            item = ReviewItem(
                symbol="AAPL",
                stance="insufficient_evidence",
                rationale=point,
                objections=(),
                confirmation_conditions=(),
                claims=(EvidenceClaim(ref=ref, value=False),),
                data_limitations=context.limitations,
            )
            decision = ReviewDecision(
                strategy=context.strategy,
                context_hash=context.content_hash,
                target_selection_id=target["selection_id"],
                items=(item,),
            )
        return StageResult(
            context_hash=context.content_hash,
            status="completed",
            decision=decision,
            error=None,
            receipt_path="fixture-success",
        )

    monkeypatch.setattr(pipeline, "verify_runtime", verified)
    monkeypatch.setattr(pipeline, "execute_stage", controlled)

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        settings = postgres_settings.model_copy(
            update={"workspace": "resume-" + new_id(), "artifacts_path": tmp_path, "model_env_path": trusted}
        )
        try:
            await initialize_schema(engine)
            target = valid.as_of.replace(day=23)
            partial = await run_preview(engine, settings, valid, analyses, target)
            assert partial["status"] == "partial"
            assert len(partial["selections"]) == 1
            original = partial["selections"][0]
            recovered = await run_preview(engine, settings, valid, analyses, target)
            assert recovered["run_id"] == partial["run_id"]
            assert recovered["status"] == "completed"
            assert original in recovered["selections"]
            assert len(recovered["selections"]) == len(recovered["reviews"]) == 2
            assert seen == [("martin", "select"), ("martin", "select"), ("wyckoff", "review")]
            stages = {(stage["strategy"], stage["kind"]): stage for stage in recovered["stages"]}
            assert stages["martin", "select"]["attempt_count"] == 2
            assert stages["wyckoff", "select"]["attempt_count"] == 1
            assert stages["wyckoff", "review"]["target_selection_id"] == next(
                selection["id"] for selection in recovered["selections"] if selection["strategy"] == "martin"
            )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_parallel_stage_failure_cancels_and_awaits_sibling_cleanup() -> None:
    from stock_research.pipeline import parallel_stages

    async def scenario() -> None:
        entered, cleaned = asyncio.Event(), asyncio.Event()

        async def long_stage() -> None:
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()

        async def failed_stage() -> None:
            await entered.wait()
            raise ValueError("controlled failure")

        with pytest.raises(ValueError, match="controlled failure"):
            await parallel_stages([long_stage(), failed_stage()])
        assert cleaned.is_set()

    asyncio.run(scenario())
