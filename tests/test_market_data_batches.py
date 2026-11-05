"""冻结名单分批与完整覆盖汇总的行为回归。"""
from datetime import date
import json
from pathlib import Path

import pytest

from integrations.market_data_review import write_review
from scripts import market_data_batches as batches


def plan_for(symbols: list[str], size: int = 2) -> batches.BatchPlan:
    """生成固定目标日的真实批次计划。"""
    return batches.create_plan(
        [{"code": code, "name": code} for code in symbols],
        target_date=date(2026, 11, 4), window_start=date(2026, 11, 3),
        trade_dates=[date(2026, 11, 3), date(2026, 11, 4)],
        trading_days=320, batch_size=size,
    )


def save_part(root: Path, plan: batches.BatchPlan, batch_id: int, status: str = "gap_repaired") -> None:
    """使用生产审核写出器落盘单批覆盖记录。"""
    part = plan["batches"][batch_id]
    folder = root / f"cn-daily-batch-{batch_id}"
    folder.mkdir(parents=True)
    (folder / "batch_metadata.json").write_text(json.dumps({"plan_id": plan["plan_id"], "batch_id": batch_id}))
    records = {item["code"]: {"market": "cn", "symbol": item["code"], "status": status, "target_date": plan["target_date"]} for item in part["securities"]}
    write_review(folder, "cn", plan["target_date"], records, pool_mode="all_cn", status="ok" if status == "gap_repaired" else "partial")


def test_normal_partition_covers_4606_symbols_once() -> None:
    """【正常值】4606只分成12批，批内不超过400，乱序重复输入不重不漏。"""
    symbols = [f"{i:06d}" for i in range(4606)]
    plan = plan_for(list(reversed(symbols)) + symbols[:3], 400)
    flattened = [item["code"] for part in plan["batches"] for item in part["securities"]]

    assert len(plan["batches"]) == 12
    assert max(len(part["securities"]) for part in plan["batches"]) == 400
    assert flattened == symbols
    assert plan["target_date"] == "2026-11-04"


def test_normal_merge_all_batches_uses_whole_pool(tmp_path: Path) -> None:
    """【正常值】最终统计覆盖所有批次，而非仅最后一批。"""
    plan = plan_for(["000001", "000002", "000003"])
    for part in plan["batches"]:
        save_part(tmp_path / "parts", plan, part["id"])

    summary = batches.merge_batches(plan, tmp_path / "parts", tmp_path / "merged")

    assert summary["status"] == "ok"
    assert summary["total"] == 3
    assert summary["counts"] == {"gap_repaired": 3}


def test_error_missing_batch_stays_pending(tmp_path: Path) -> None:
    """【异常值】缺少一个批次产物也保留全池总数，并明确未完成。"""
    plan = plan_for(["000001", "000002", "000003"])
    save_part(tmp_path / "parts", plan, 0)

    summary = batches.merge_batches(plan, tmp_path / "parts", tmp_path / "merged")

    assert summary["status"] == "partial"
    assert summary["total"] == 3
    assert summary["counts"] == {"gap_repaired": 2, "pending": 1}


def test_error_wrong_target_or_plan_cannot_merge(tmp_path: Path) -> None:
    """【异常值】重跑产物来自不同计划时不能冒充本轮成功。"""
    plan = plan_for(["000001"])
    save_part(tmp_path / "parts", plan, 0)
    (tmp_path / "parts/cn-daily-batch-0/batch_metadata.json").write_text(json.dumps({"plan_id": "wrong", "batch_id": 0}))

    with pytest.raises(ValueError, match="计划"):
        batches.merge_batches(plan, tmp_path / "parts", tmp_path / "merged")


def test_error_failed_part_is_not_success(tmp_path: Path) -> None:
    """【异常值】各批均有产物但含失败股票时仍报告未完成。"""
    plan = plan_for(["000001", "000002", "000003"])
    save_part(tmp_path / "parts", plan, 0)
    save_part(tmp_path / "parts", plan, 1, "failed")

    summary = batches.merge_batches(plan, tmp_path / "parts", tmp_path / "merged")

    assert summary["status"] == "partial"
    assert summary["counts"]["failed"] == 1


@pytest.mark.parametrize("size,count", [pytest.param(0, 1, id="zero-size"), pytest.param(1, 0, id="empty-pool"), pytest.param(1, 257, id="matrix-limit")])
def test_error_invalid_plan_limits(size: int, count: int) -> None:
    """【参数校验】空池、零批大小和超过Actions矩阵上限时明确拒绝。"""
    with pytest.raises(ValueError):
        plan_for([f"{i:06d}" for i in range(count)], size)


def test_error_coverage_date_drift_is_rejected(tmp_path: Path) -> None:
    """【异常值】即使批次标识匹配，覆盖日期漂移也不能合并。"""
    plan = plan_for(["000001"])
    save_part(tmp_path / "parts", plan, 0)
    path = tmp_path / "parts/cn-daily-batch-0/prewarm_cn_2026-11-04_coverage.csv"
    path.write_text(path.read_text(encoding="utf-8-sig").replace("2026-11-04", "2026-11-05"), encoding="utf-8-sig")

    with pytest.raises(ValueError, match="目标日"):
        batches.merge_batches(plan, tmp_path / "parts", tmp_path / "merged")
