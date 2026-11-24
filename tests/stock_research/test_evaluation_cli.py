"""历史评估真实CLI入口、生产行情源和不可变文件验收。"""

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine, insert

from stock_research.calendar import sessions_ending
from stock_research.data import DAILY_BARS, SOURCE_CONFIG, SOURCE_METADATA
from stock_research.models import ClassificationVersion, UniverseVersion
from stock_research.parameters import MartinParameters
from stock_research.wyckoff import WyckoffParameters


def test_integration_cli_evaluate_entry_reuses_and_rejects_corrupt_evidence(
    tmp_path: Path, universe: UniverseVersion, classifications: ClassificationVersion
) -> None:
    """[INTEGRATION] 新进程从真实行情库生成40份证据并拒绝损坏重读。"""
    source = tmp_path / "source.sqlite3"
    engine = create_engine("sqlite:///" + str(source))
    stamp = "2026-11-21T08:00:00+08:00"
    try:
        SOURCE_METADATA.create_all(engine)
        with engine.begin() as connection:
            connection.execute(insert(SOURCE_CONFIG).values(key="cloud_source", value="synthetic-fixture"))
            connection.execute(
                insert(DAILY_BARS),
                [
                    dict(
                        market="us",
                        kind="stock" if symbol == "AAPL" else "benchmark",
                        symbol=symbol,
                        adjust="qfq",
                        date=day,
                        open=100,
                        high=101,
                        low=99,
                        close=100,
                        volume=1000,
                        cloud_updated_at=stamp,
                        imported_at=stamp,
                    )
                    for symbol in ("AAPL", "SPY", "QQQ", "IWM")
                    for day in sessions_ending(date(2026, 11, 20), 360)
                ],
            )
    finally:
        engine.dispose()
    versions = {
        "universe_path": universe,
        "classification_path": classifications,
        "parameters_path": MartinParameters(),
        "wyckoff_parameters_path": WyckoffParameters(),
    }
    env = {
        **os.environ,
        "STOCK_RESEARCH_LOCAL_MARKET_PATH": str(source),
        "STOCK_RESEARCH_ARTIFACTS_PATH": str(tmp_path / "artifacts"),
    }
    for key, version in versions.items():
        path = tmp_path / (key + ".json")
        path.write_text(version.model_dump_json(), encoding="utf-8")
        env["STOCK_RESEARCH_" + key.upper()] = str(path)
    command = [
        sys.executable,
        "-m",
        "stock_research.cli",
        "evaluate",
        "--profile",
        "baseline-v1",
        "--cutoff",
        "2026-11-20",
    ]

    first = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)
    second = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)

    assert first.returncode == second.returncode == 0, first.stdout + first.stderr + second.stdout + second.stderr
    summary = json.loads(first.stdout)
    assert summary == json.loads(second.stdout)
    assert summary["section_count"] == 20 and summary["model_selection"] is None
    root = Path(summary["path"]).parent
    report_before = (root / "report.json").read_bytes()
    analysis = next(root.glob("*-martin.json"))
    analysis.write_text("{}", encoding="utf-8")

    failed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)

    assert failed.returncode == 1
    assert "error" in json.loads(failed.stdout)
    assert (root / "report.json").read_bytes() == report_before
