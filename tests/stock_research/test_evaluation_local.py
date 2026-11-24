"""隔离评估的文件输入冻结与后续修订边界。"""

from pathlib import Path

import pytest
from sqlalchemy import create_engine, update

from stock_research.data import DAILY_BARS
from stock_research.evaluation_local import evaluate_files, load_evaluation_input
from stock_research.schemas.evaluation_execution import EvaluationFiles


def test_integration_evaluation_frozen_input_survives_market_revision(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] 重试只读已冻结行情，不悄悄把最新修订带回原实验。"""
    first, first_hash = load_evaluation_input(evaluation_files)
    engine = create_engine("sqlite:///" + str(evaluation_files.market_path))
    try:
        with engine.begin() as connection:
            connection.execute(update(DAILY_BARS).values(volume=777))
    finally:
        engine.dispose()

    repeated, repeated_hash = load_evaluation_input(evaluation_files)

    assert repeated == first and repeated_hash == first_hash
    assert all(bar.volume == 1000 for bar in repeated.bars)
    with pytest.raises(ValueError, match="摘要"):
        load_evaluation_input(evaluation_files.model_copy(update={"expected_input_hash": "0" * 64}))


def test_integration_evaluation_checkpoints_stop_at_twenty(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] 输入冻结和20个已完成截面分别回报，未完成不先报成功。"""
    events = []

    result = evaluate_files(
        evaluation_files, checkpoint=lambda kind, count, fingerprint: events.append((kind, count, fingerprint))
    )

    assert events[0][:2] == ("frozen", 0)
    assert [count for kind, count, _ in events if kind == "section"] == list(range(1, 21))
    assert {fingerprint for _, _, fingerprint in events} == {result["input_hash"]}
    assert Path(result["path"]).is_file()
    assert (Path(result["path"]).parent / "input.json").samefile(evaluation_files.frozen_input_path)


def test_validation_evaluation_paths_and_unknown_fields(evaluation_files: EvaluationFiles) -> None:
    """[VALIDATION] 协议不猜相对路径，不接数据库连接等未知配置。"""
    body = evaluation_files.model_dump(mode="json")
    with pytest.raises(ValueError):
        EvaluationFiles.model_validate({**body, "market_path": "../source.sqlite3"})
    with pytest.raises(ValueError):
        EvaluationFiles.model_validate({**body, "database_url": "forbidden"})


def test_integration_evaluation_versions_reject_replacement(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] 物化版本后来改名同内容，仍拒绝冒充原版本。"""
    raw = evaluation_files.universe_path.read_text()
    evaluation_files.universe_path.write_text(raw.replace("fixture-v1", "fixture-v2"), encoding="utf-8")

    with pytest.raises(ValueError, match="冻结清单"):
        load_evaluation_input(evaluation_files)
