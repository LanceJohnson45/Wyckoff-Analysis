"""分组覆盖、轮流合并、版本产物与AKQuant真正执行的对拍。"""

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from stock_research.analysis import MartinObservation, merge_pool, score_observations
from stock_research.config import Settings
from stock_research.factor_benchmark import akquant_values, benchmark, pandas_values
from stock_research.groups import group_facts
from stock_research.martin import observe_branch
from stock_research.models import Bar, ClassificationVersion, Snapshot
from stock_research.parameters import MartinParameters
from stock_research.quality import build_snapshot
from stock_research.research_commands import analyze_command


def test_normal_akquant_atr_adr_match_reference_with_warmup(trending_bars: tuple[Bar, ...], tmp_path: Path) -> None:
    """【正常值】调用实际FactorEngine逐值比较，含预热缺失，而非库存在性断言。"""
    rows = [
        {
            "symbol": bar.symbol,
            "date": bar.session_date.isoformat(),
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
        }
        for bar in trending_bars
    ]
    parameters = MartinParameters()

    reference, reference_key = pandas_values(rows, parameters)
    actual, actual_key = akquant_values(rows, parameters, tmp_path / "catalog")

    assert reference_key == actual_key
    np.testing.assert_allclose(actual, reference, rtol=0, atol=1e-10, equal_nan=True)


def test_normal_benchmark_subprocesses_read_same_snapshot(snapshot: Snapshot, tmp_path: Path) -> None:
    """【正常值】两个独立进程测量并逐值校验同一快照，可重读测量产物。"""
    path = tmp_path / "snapshot.json"
    path.write_text(snapshot.model_dump_json())

    result = benchmark(path, MartinParameters(), tmp_path / "results")
    reread = json.loads(Path(result["path"]).read_text())

    assert result["passed"]
    assert result["positions_match"] and result["warmup_missing_match"]
    assert result["ema_compared"] is False
    assert reread["measurements"]["pandas"]["rows"] == reread["measurements"]["akquant"]["rows"] == 320
    assert all(metrics["peak_rss_mib"] > 0 for metrics in reread["measurements"].values())


def test_boundary_group_denominator_includes_missing_members(snapshot: Snapshot) -> None:
    """【边界值】缺行情的组成员不从分母丢弃，低于80%不给分。"""
    members = tuple(snapshot.universe.members[0].model_copy(update={"symbol": f"A{i}"}) for i in range(4))
    universe = snapshot.universe.model_copy(update={"members": members})
    base = snapshot.classifications.records[0]
    records = tuple(base.model_copy(update={"symbol": f"A{i}", "themes": ("Repeated", "Repeated")}) for i in range(4))
    version = ClassificationVersion(version="group-test", records=records)
    grouped = snapshot.model_copy(update={"universe": universe, "classifications": version})
    factors = {f"A{i}": {"rs": float(i), "return_1": 1.0, "return_3": 1.0} for i in range(3)}

    result = group_facts(grouped, factors, {f"A{i}": True for i in range(3)}, MartinParameters())

    assert len(result) == 2
    assert all(group["coverage"] == 0.75 for group in result)
    assert all(group["score"] is None for group in result)
    assert all(len(group["members"]) == 4 for group in result)


def observation(symbol: str, branches: tuple[str, ...], scores: dict[str, float]) -> MartinObservation:
    """构造已通过独立规则执行器的分支，用于验证合并协议。"""
    return MartinObservation(
        symbol=symbol,
        factors={"rs": 1.0, "volume_ratio": 0.8, "range_ratio": 0.5},
        branches=tuple(observe_branch(branch, {"support": True}, (), {}, MartinParameters()) for branch in branches),
        anchors=(),
        branch_scores=scores,
    )


def test_normal_two_branches_alternate_without_duplicate_symbols() -> None:
    """【正常值】按分支名次轮流取，双命中仍保留两分支且股票池只出现一次。"""
    rows = (
        observation("BOTH", ("early", "mature"), {"early": 100, "mature": 100}),
        observation("EARLY", ("early",), {"early": 80}),
        observation("MATURE", ("mature",), {"mature": 90}),
    )

    result = merge_pool(rows, 3)

    assert result == ("BOTH", "EARLY", "MATURE")
    assert len(rows[0].branches) == 2
    assert merge_pool(rows, 2) == ("BOTH", "EARLY")


def test_error_no_available_score_weight_excludes_branch() -> None:
    """【异常值】只有缺失分组项被赋权时不能伪造零分并进入量化池。"""
    row = observation("AAPL", ("mature",), {"mature": 50})
    original = MartinParameters()
    parameters = original.model_copy(update={"weights": {key: float(key == "group") for key in original.weights}})

    scored = score_observations((row,), (), parameters)

    assert merge_pool(scored, 30) == ()
    assert "no_available_score_weight" in scored[0].branches[0].excluded_by


def test_idempotent_analysis_reuses_artifact_and_rejects_tampering(
    snapshot: Snapshot,
    trending_bars: tuple[Bar, ...],
    tmp_path: Path,
) -> None:
    """【幂等性】同快照同参数重用原产物，改动价格会在重读时被摘要发现。"""
    bars = tuple(bar for bar in snapshot.bars if bar.kind == "benchmark") + trending_bars
    valid = build_snapshot(snapshot.universe, snapshot.classifications, bars, "synthetic", snapshot.as_of)
    source, params = tmp_path / "snapshot.json", tmp_path / "parameters.json"
    source.write_text(valid.model_dump_json())
    params.write_text(MartinParameters().model_dump_json())
    settings = Settings(_env_file=None, parameters_path=params, artifacts_path=tmp_path / "artifacts")

    first = analyze_command(settings, source, valid.as_of)
    second = analyze_command(settings, source, valid.as_of)

    assert first == second
    result_path = Path(first["path"])
    body = json.loads(result_path.read_text())
    body["observations"][0]["factors"]["close"] += 1
    result_path.write_text(json.dumps(body))
    with pytest.raises(ValueError, match="摘要"):
        analyze_command(settings, source, valid.as_of)


def test_validation_analysis_cutoff_must_match_snapshot(snapshot: Snapshot, tmp_path: Path) -> None:
    """【参数校验】不能把另一截止日的快照解释成当前观察。"""
    path = tmp_path / "snapshot.json"
    path.write_text(snapshot.model_dump_json())

    with pytest.raises(ValueError, match="截止日不一致"):
        analyze_command(Settings(_env_file=None), path, date(2026, 11, 19))


def test_normal_analysis_cli_uses_real_snapshot_and_parameter_files(
    snapshot: Snapshot,
    trending_bars: tuple[Bar, ...],
    tmp_path: Path,
) -> None:
    """【正常值】新解释器通过正式CLI生成量化观察，不调用模型或读取当前行情。"""
    bars = tuple(bar for bar in snapshot.bars if bar.kind == "benchmark") + trending_bars
    valid = build_snapshot(snapshot.universe, snapshot.classifications, bars, "synthetic", snapshot.as_of)
    source, params = tmp_path / "snapshot.json", tmp_path / "parameters.json"
    source.write_text(valid.model_dump_json())
    params.write_text(MartinParameters().model_dump_json())
    env = {
        **os.environ,
        "STOCK_RESEARCH_PARAMETERS_PATH": str(params),
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
            "2026-11-20",
            "--strategy",
            "martin",
            "--no-llm",
            "--snapshot-file",
            str(source),
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    output = json.loads(result.stdout)
    assert output["pool"] == ["AAPL"]
    assert output["model_selection"] is None
    assert Path(output["path"]).is_file()
