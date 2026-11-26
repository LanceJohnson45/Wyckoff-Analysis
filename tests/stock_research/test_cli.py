"""CLI 子进程、真实输入文件和脱敏失败路径。"""

import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete

from stock_research.calendar import latest_complete_session
from stock_research.cli import snapshot_command, supported_python
from stock_research.config import Settings
from stock_research.data import DAILY_BARS
from stock_research.models import ClassificationVersion, UniverseVersion


@pytest.fixture
def configured(
    tmp_path: Path, market_path: Path, universe: UniverseVersion, classifications: ClassificationVersion
) -> Settings:
    """真实CLI输入，使用临时版本文件和临时行情库。"""
    upath, cpath = tmp_path / "universe.json", tmp_path / "classifications.json"
    upath.write_text(universe.model_dump_json(), encoding="utf-8")
    cpath.write_text(classifications.model_dump_json(), encoding="utf-8")
    return Settings(
        _env_file=None,
        local_market_path=market_path,
        universe_path=upath,
        classification_path=cpath,
        artifacts_path=tmp_path / "artifacts",
    )


def test_normal_cli_offline_snapshot_reloads_in_new_process(configured: Settings) -> None:
    """【正常值】快照CLI落盘后，新解释器走读取命令校验同一摘要。"""
    env = {
        **os.environ,
        "STOCK_RESEARCH_BENCHMARK_PATH": "",
        **{
            f"STOCK_RESEARCH_{key.upper()}": str(getattr(configured, key))
            for key in (
                "local_market_path",
                "universe_path",
                "classification_path",
                "artifacts_path",
            )
        },
    }

    first = subprocess.run(
        [
            sys.executable,
            "-m",
            "stock_research.cli",
            "snapshot",
            "--market",
            "us",
            "--as-of",
            "2026-11-20",
            "--offline",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    payload = json.loads(first.stdout)
    second = subprocess.run(
        [sys.executable, "-m", "stock_research.cli", "read-snapshot", "--file", payload["path"]],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert first.returncode == second.returncode == 0, first.stderr + second.stderr
    assert payload["persisted"] is False
    assert json.loads(second.stdout)["content_hash"] == payload["content_hash"]
    assert len(json.loads(second.stdout)["bars"]) == 1280


def test_idempotent_offline_snapshot_reuses_materialization(configured: Settings) -> None:
    """【幂等性】重复同一输入保留原快照ID和文件。"""
    from datetime import date

    first, first_status = snapshot_command(configured, date(2026, 11, 20), offline=True)
    second, second_status = snapshot_command(configured, date(2026, 11, 20), offline=True)

    assert first_status == second_status == 0
    assert first["id"] == second["id"]
    assert first["path"] == second["path"]
    assert len(list((configured.artifacts_path / "snapshots").glob("*.json"))) == 1


def test_error_failed_quality_still_writes_report(configured: Settings) -> None:
    """【异常值】基准缺失返回数据未就绪退出码，失败报告仍可查看。"""
    import sqlite3
    from datetime import date

    engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(configured.local_market_path))
    try:
        with engine.begin() as connection:
            connection.execute(delete(DAILY_BARS).where(DAILY_BARS.c.symbol == "QQQ"))
    finally:
        engine.dispose()

    output, status = snapshot_command(configured, date(2026, 11, 20), offline=True)

    assert status == 2
    assert output["quality"]["passed"] is False
    assert Path(output["path"]).is_file()


def test_error_doctor_does_not_expose_database_secret(tmp_path: Path) -> None:
    """【异常值】数据库不可达时输出结构化就绪检查，不输出密码或堆栈。"""
    secret = "synthetic-doctor-secret"
    env = {
        **os.environ,
        "STOCK_RESEARCH_POSTGRES_HOST": str(tmp_path / "no-such-socket"),
        "STOCK_RESEARCH_POSTGRES_PASSWORD": secret,
    }

    result = subprocess.run(
        [sys.executable, "-m", "stock_research.cli", "doctor"], capture_output=True, text=True, env=env, timeout=30
    )

    assert result.returncode == 2
    assert json.loads(result.stdout)["checks"]["database"] is False
    assert secret not in result.stdout + result.stderr
    assert "Traceback" not in result.stdout + result.stderr


def test_doctor_accepts_release_python_floor() -> None:
    """【边界值】发布venv是3.11；doctor必须按项目下限判定，而非CI解释器。"""
    assert supported_python((3, 11, 16, "final", 0))
    assert not supported_python((3, 10, 12, "final", 0))


def test_doctor_uses_supported_python_floor(configured: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    """【接线】边界函数必须被doctor生产路径消费，而不是只被自身测试调用。"""
    from stock_research import cli

    observed: list[tuple[int, int]] = []

    def supported(version: Sequence[int]) -> bool:
        observed.append(tuple(version[:2]))
        return True

    async def unavailable_database(settings: Settings, command: str, snapshot_id: str | None = None) -> bool:
        return False

    monkeypatch.setattr(cli, "supported_python", supported)
    monkeypatch.setattr(cli, "database_command", unavailable_database)
    monkeypatch.setattr(cli.sys, "version_info", (3, 11, 16, "final", 0))

    output, status = cli.doctor(configured)

    assert observed == [(3, 11)]
    assert output["checks"]["python"] is True
    assert output["checks"]["database"] is False
    assert status == 2


def test_validation_incomplete_session_is_rejected(configured: Settings) -> None:
    """【参数校验】尚未完成收盘的未来日期不能生成完整日线快照。"""
    from datetime import timedelta

    future = latest_complete_session() + timedelta(days=7)

    with pytest.raises(ValueError, match="尚未完成"):
        snapshot_command(configured, future, offline=True)
