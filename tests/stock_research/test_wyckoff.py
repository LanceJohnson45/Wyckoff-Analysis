"""离线驱动真实生产路径，验证研究适配、诊断与不可变产物。"""

import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from stock_research.calendar import sessions_ending
from stock_research.config import Settings
from stock_research.models import Snapshot
from stock_research.quality import build_snapshot
from stock_research.research_commands import analyze_command
from stock_research.wyckoff import WyckoffParameters, WyckoffReport, compute_wyckoff, production_inputs
from workflows.market_funnel_job import run_funnel_for_ranked
from workflows.market_funnel_runtime import runtime_config_from_env


def test_normal_wyckoff_matches_actual_market_production_entry(spring_snapshot: Snapshot) -> None:
    """【正常值】实跑生产入口逐字段对照候选和指标，不用手写信号代理。"""
    symbols, frames, benchmark, names, sectors = production_inputs(spring_snapshot)
    runtime = replace(runtime_config_from_env("us", None), kline_count=320, min_avg_amount=0.0)
    ranked = [{"symbol": symbol, "name": names[symbol], "sector": sectors[symbol]} for symbol in symbols]

    metrics, candidates = run_funnel_for_ranked(ranked, frames, runtime, benchmark, "SPY")
    report = compute_wyckoff(spring_snapshot, WyckoffParameters())

    assert report.metrics == metrics
    assert list(report.candidates) == candidates
    assert report.pool == ("AAPL",)
    assert report.candidates[0]["raw_triggers"] == ["spring"]
    assert report.candidates[0]["score"] == pytest.approx(6.060606060606057)
    assert report.effective_config["require_cn_main_or_chinext"] is False
    assert report.effective_config["sos_pct_min"] == 8.0
    assert report.effective_config["min_avg_amount_wan"] == 0.0
    assert report.execution_permission == "not_evaluated" and report.model_selection is None
    assert any("金额" in text for text in report.limitations)
    assert WyckoffReport.model_validate_json(report.model_dump_json()) == report


def test_boundary_crash_regime_caps_formal_hits_without_filling(spring_snapshot: Snapshot) -> None:
    """【边界值】已有Spring命中在CRASH下仍不得进入池，保留水温排除。"""
    bars = tuple(
        bar.model_copy(update={"open": 10.5, "close": 10.0})
        if bar.kind == "benchmark" and bar.symbol == "SPY" and bar.session_date == spring_snapshot.as_of
        else bar
        for bar in spring_snapshot.bars
    )
    crashed = build_snapshot(
        spring_snapshot.universe, spring_snapshot.classifications, bars, "synthetic-crash", spring_snapshot.as_of
    )

    report = compute_wyckoff(crashed, WyckoffParameters())

    assert report.metrics["benchmark"]["regime"] == "CRASH"
    assert report.effective_config["rps_fast_min"] >= 85.0
    assert report.auxiliary["triggers"]["spring"]
    assert report.pool == () and report.candidates == ()
    assert report.auxiliary["regime_excluded"] == ["AAPL"]
    assert any(row.get("layer") == "regime" for row in report.excluded)


def test_boundary_empty_formal_candidates_keep_observations(snapshot: Snapshot) -> None:
    """【边界值】平坦样本不产生假信号，逐层保留实际排除且不补位。"""
    report = compute_wyckoff(snapshot, WyckoffParameters())

    assert report.pool == () and report.candidates == ()
    assert len(report.observations) == 1
    assert any(row.get("layer") == "layer4" for row in report.excluded)


def test_normal_production_score_order_is_preserved_before_pool_limit(spring_snapshot: Snapshot) -> None:
    """【正常值】不同收复幅度的两个真实Spring命中按上游分数排序，池大小不影响计算。"""
    universe = spring_snapshot.universe.model_copy(
        update={
            "members": (
                *spring_snapshot.universe.members,
                spring_snapshot.universe.members[0].model_copy(update={"symbol": "MSFT", "name": "Microsoft"}),
            ),
        }
    )
    classifications = spring_snapshot.classifications.model_copy(
        update={
            "records": (
                *spring_snapshot.classifications.records,
                spring_snapshot.classifications.records[0].model_copy(update={"symbol": "MSFT"}),
            ),
        }
    )
    additional = tuple(
        bar.model_copy(
            update={
                "symbol": "MSFT",
                **(
                    {"high": 10.8, "close": 10.6, "volume": 3000.0} if bar.session_date == spring_snapshot.as_of else {}
                ),
            }
        )
        for bar in spring_snapshot.bars
        if bar.kind == "stock"
    )
    combined = build_snapshot(
        universe, classifications, (*spring_snapshot.bars, *additional), "synthetic-two-springs", spring_snapshot.as_of
    )

    report = compute_wyckoff(combined, WyckoffParameters(pool_size=1))

    assert [row["symbol"] for row in report.candidates] == ["MSFT", "AAPL"]
    assert report.candidates[0]["score"] > report.candidates[1]["score"]
    assert report.pool == ("MSFT",)
    assert all(row["qualified"] for row in report.observations)


def test_boundary_layer1_rejection_does_not_disappear(spring_snapshot: Snapshot) -> None:
    """【边界值】自定义生产价格门将股票拦在L1时保留其分母和层级。"""
    report = compute_wyckoff(spring_snapshot, WyckoffParameters(overrides={"l1_min_close_price": 20.0}))

    assert report.metrics["layer1"] == 0 and report.pool == ()
    assert len(report.observations) == 1
    assert any(row.get("layer") == "layer1" for row in report.excluded)


def test_boundary_longer_snapshot_uses_same_fixed_production_window(spring_snapshot: Snapshot) -> None:
    """【边界值】快照允许更长历史，当前美股计算仍须与320根输入一致。"""
    earlier = sessions_ending(spring_snapshot.as_of, 360)[:40]
    prototypes = {(bar.kind, bar.symbol): bar for bar in spring_snapshot.bars}
    prefix = tuple(
        bar.model_copy(update={"session_date": day, "open": 999.0, "high": 1001.0, "low": 998.0, "close": 1000.0})
        for bar in prototypes.values()
        for day in earlier
    )
    extended = build_snapshot(
        spring_snapshot.universe,
        spring_snapshot.classifications,
        (*prefix, *spring_snapshot.bars),
        "synthetic-extended",
        spring_snapshot.as_of,
        history_rows=360,
    )

    _symbols, frames, benchmark, _names, _sectors = production_inputs(extended)
    original = compute_wyckoff(spring_snapshot, WyckoffParameters())
    actual = compute_wyckoff(extended, WyckoffParameters())

    assert len(benchmark) == 320 and all(len(frame) == 320 for frame in frames.values())
    assert actual.metrics == original.metrics
    assert actual.candidates == original.candidates


@pytest.mark.parametrize(
    "overrides",
    [
        {"unknown": 1},
        {"__dict__": {}},
        {"enable_rs_filter": "false"},
        {"min_avg_amount_wan": 1.0},
        {"require_cn_main_or_chinext": True},
        {"trading_days": 321},
    ],
)
def test_validation_unknown_or_unsafe_production_overrides_rejected(overrides: dict) -> None:
    """【参数校验】拼写、类型、缺金额硬门槛和不受支持窗口不能静默放行。"""
    with pytest.raises(ValueError):
        WyckoffParameters(overrides=overrides)


def test_validation_json_tuple_override_keeps_production_collection_type() -> None:
    """【参数校验】JSON数组可用于上游tuple字段，不能因严格校验拒绝合法配置。"""
    original = WyckoffParameters()
    assert original.overrides == {}
    actual = WyckoffParameters(overrides={"blocked_candidate_entry_types": ["trend_follow"]})
    assert actual.overrides["blocked_candidate_entry_types"] == ["trend_follow"]


def test_error_failed_quality_blocks_wyckoff(snapshot: Snapshot) -> None:
    """【异常值】整体数据失败不能继续生成量化结果。"""
    invalid = snapshot.model_copy(update={"quality": snapshot.quality.model_copy(update={"passed": False})})
    with pytest.raises(ValueError, match="质量未通过"):
        compute_wyckoff(invalid, WyckoffParameters())


def test_idempotent_wyckoff_artifact_reused_and_tampering_rejected(spring_snapshot: Snapshot, tmp_path: Path) -> None:
    """【幂等性】同快照和源码复用已发布产物，损坏缓存拒绝使用。"""
    source, parameters = tmp_path / "snapshot.json", tmp_path / "parameters.json"
    source.write_text(spring_snapshot.model_dump_json())
    parameters.write_text(WyckoffParameters().model_dump_json())
    settings = Settings(_env_file=None, wyckoff_parameters_path=parameters, artifacts_path=tmp_path / "artifacts")

    first = analyze_command(settings, source, spring_snapshot.as_of, strategy="wyckoff")
    assert first == analyze_command(settings, source, spring_snapshot.as_of, strategy="wyckoff")
    path = Path(first["path"])
    body = json.loads(path.read_text())
    body["candidates"][0]["score"] += 1
    path.write_text(json.dumps(body))
    with pytest.raises(ValueError, match="摘要不匹配"):
        analyze_command(settings, source, spring_snapshot.as_of, strategy="wyckoff")


def test_normal_wyckoff_cli_produces_replayable_json(spring_snapshot: Snapshot, tmp_path: Path) -> None:
    """【正常值】真实CLI子进程接线，输出可重读且无生产进度文本污染。"""
    source, parameters = tmp_path / "snapshot.json", tmp_path / "parameters.json"
    source.write_text(spring_snapshot.model_dump_json())
    parameters.write_text(WyckoffParameters().model_dump_json())
    environment = {
        **os.environ,
        "STOCK_RESEARCH_WYCKOFF_PARAMETERS_PATH": str(parameters),
        "STOCK_RESEARCH_ARTIFACTS_PATH": str(tmp_path / "artifacts"),
    }
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "stock_research.cli",
            "analyze",
            "--market",
            "us",
            "--as-of",
            str(spring_snapshot.as_of),
            "--strategy",
            "wyckoff",
            "--no-llm",
            "--snapshot-file",
            str(source),
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    body = json.loads(result.stdout)
    report = WyckoffReport.model_validate_json(Path(body["path"]).read_text())
    assert body["pool"] == ["AAPL"] and report.pool == ("AAPL",)
    assert report.snapshot_hash == spring_snapshot.content_hash
