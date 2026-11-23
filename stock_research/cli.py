"""独立研究数据命令；仅显式初始化时修改研究 schema。"""

import argparse
import asyncio
import importlib.metadata
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

from asyncpg import PostgresError
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from stock_research.artifacts import data_lock, write_once
from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.config import Settings
from stock_research.data import read_market
from stock_research.models import ClassificationVersion, Snapshot, UniverseVersion
from stock_research.quality import build_snapshot
from stock_research.store import (
    close_engine,
    database_engine,
    database_ready,
    initialize_schema,
    load_snapshot,
    save_snapshot,
)
from stock_research.versions import build_classifications, build_universe, import_version


def parser() -> argparse.ArgumentParser:
    """只注册已实现的命令，不暴露尚不存在的分析或执行入口。"""
    root = argparse.ArgumentParser(prog="stock-research")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor")
    commands.add_parser("init-schema", help="显式创建独立研究 schema v1；不更新已有表定义")
    snapshot = commands.add_parser("snapshot")
    snapshot.add_argument("--market", choices=["us"], required=True)
    snapshot.add_argument("--as-of", type=date.fromisoformat, required=True)
    snapshot.add_argument("--offline", action="store_true", help="仅输出本地物化快照，不连接研究 PostgreSQL")
    read = commands.add_parser("read-snapshot")
    read.add_argument("--id")
    read.add_argument("--file", type=Path)
    version = commands.add_parser("import-version")
    version.add_argument("--kind", choices=["universe", "classifications"], required=True)
    version.add_argument("--input", type=Path, required=True)
    build = commands.add_parser("build-versions", help="将独立指数清单与旧分类缓存转换为带出处的版本，写入指定目录")
    build.add_argument("--sp500", type=Path, required=True)
    build.add_argument("--nasdaq100", type=Path, required=True)
    build.add_argument("--manual", type=Path)
    build.add_argument("--classification-cache", type=Path, required=True)
    build.add_argument("--version", required=True)
    build.add_argument("--effective-date", type=date.fromisoformat, required=True)
    build.add_argument("--source-reference", required=True)
    build.add_argument("--output-dir", type=Path, required=True)
    return root


async def persist(settings: Settings, snapshot: Snapshot) -> Snapshot:
    """数据库提交及独立重读完成后才返回成功。"""
    engine = database_engine(settings)
    try:
        return await save_snapshot(engine, settings.workspace, snapshot)
    finally:
        await close_engine(engine)


def snapshot_command(settings: Settings, as_of: date, *, offline: bool) -> tuple[dict[str, object], int]:
    """在协作锁内读取版本和一致行情，失败质量报告也留存。"""
    if as_of > latest_complete_session():
        raise ValueError("截止日尚未完成正式收盘，不接受未完成日线")
    with data_lock(settings.artifacts_path / "locks", "us", as_of.isoformat()):
        universe = UniverseVersion.model_validate_json(settings.universe_path.read_text(encoding="utf-8"))
        classifications = ClassificationVersion.model_validate_json(
            settings.classification_path.read_text(encoding="utf-8")
        )
        start = sessions_ending(as_of, settings.history_rows)[0]
        identity, bars, issues = read_market(settings.local_market_path, start, as_of)
        snapshot = build_snapshot(
            universe,
            classifications,
            bars,
            identity,
            as_of,
            coverage_min=settings.coverage_min,
            history_rows=settings.history_rows,
            source_issues=issues,
        )
        path = settings.artifacts_path / "snapshots" / f"{snapshot.content_hash}.json"
        if path.exists():
            saved = Snapshot.model_validate_json(path.read_text(encoding="utf-8"))
            if saved.content_hash != snapshot.content_hash:
                raise ValueError("本地快照文件名与摘要不一致")
            snapshot = saved
        if not offline:
            snapshot = asyncio.run(persist(settings, snapshot))
            path = path.parent / f"{snapshot.content_hash}-{snapshot.id}.json"
        write_once(path, snapshot.model_dump_json(indent=2) + "\n")
    return {
        "id": snapshot.id,
        "content_hash": snapshot.content_hash,
        "path": str(path.resolve()),
        "persisted": not offline,
        "quality": snapshot.quality.model_dump(mode="json"),
    }, 0 if snapshot.quality.passed else 2


async def database_command(settings: Settings, command: str, snapshot_id: str | None = None) -> object:
    """异步数据库操作统一回收连接，不输出 DSN 或驱动错误。"""
    engine = database_engine(settings)
    try:
        if command == "init-schema":
            await initialize_schema(engine)
            return {"initialized": "stock_research", "schema_version": 1}
        if command == "read-snapshot":
            return (await load_snapshot(engine, settings.workspace, snapshot_id)).model_dump(mode="json")
        return await database_ready(engine)
    finally:
        await close_engine(engine)


def doctor(settings: Settings) -> tuple[dict[str, object], int]:
    """脱敏检查配置、日历、版本、行情格式和研究数据库可读性。"""
    checks: dict[str, object] = {"python": sys.version_info[:2] == (3, 12)}
    checks["dependencies"] = {
        name: importlib.metadata.version(name)
        for name in (
            "sqlalchemy",
            "asyncpg",
            "exchange-calendars",
            "akquant",
        )
    }
    complete_session = latest_complete_session()
    checks["calendar"] = bool(sessions_ending(complete_session, 320))
    for name, path, model in (
        ("universe", settings.universe_path, UniverseVersion),
        ("classifications", settings.classification_path, ClassificationVersion),
    ):
        try:
            model.model_validate_json(path.read_text(encoding="utf-8"))
            checks[name] = True
        except (OSError, ValueError):
            checks[name] = False
    try:
        _, bars, issues = read_market(settings.local_market_path, complete_session, complete_session)
        checks["local_market"] = bool(bars) and not issues
    except (OSError, ValueError, sqlite3.Error, SQLAlchemyError):
        checks["local_market"] = False
    try:
        checks["database"] = asyncio.run(database_command(settings, "doctor"))
    except (OSError, PostgresError, SQLAlchemyError, TimeoutError):
        checks["database"] = False
    ready = all(
        checks[name] is True
        for name in ("python", "calendar", "universe", "classifications", "local_market", "database")
    )
    return {"ready": ready, "checks": checks, "api": "not_implemented", "scheduler": "not_connected"}, 0 if ready else 2


def execute(args: argparse.Namespace, settings: Settings) -> tuple[object, int]:
    """CLI 与生产数据路径共用领域实现。"""
    if args.command == "doctor":
        return doctor(settings)
    if args.command == "snapshot":
        return snapshot_command(settings, args.as_of, offline=args.offline)
    if args.command == "import-version":
        destination = settings.universe_path if args.kind == "universe" else settings.classification_path
        version = import_version(args.kind, args.input, destination)
        return {"version": version, "path": str(destination.resolve())}, 0
    if args.command == "build-versions":
        universe = build_universe(
            args.sp500,
            args.nasdaq100,
            version=args.version,
            effective_date=args.effective_date,
            source_reference=args.source_reference,
            manual=args.manual,
        )
        classifications = build_classifications(
            args.classification_cache,
            universe,
            version=args.version,
            effective_date=args.effective_date,
            source_reference=args.source_reference,
        )
        for name, version in (("universe", universe), ("classifications", classifications)):
            write_once(args.output_dir / f"{name}.json", version.model_dump_json(indent=2) + "\n")
        return {
            "version": args.version,
            "universe_count": len(universe.active(args.effective_date)),
            "classified_count": len(classifications.records),
            "limitations": universe.limitations + classifications.limitations,
        }, 0
    if args.command == "read-snapshot":
        if bool(args.id) == bool(args.file):
            raise ValueError("须选择 --id 或 --file 中的一个")
        if args.file:
            return Snapshot.model_validate_json(args.file.read_text(encoding="utf-8")).model_dump(mode="json"), 0
    return asyncio.run(database_command(settings, args.command, getattr(args, "id", None))), 0


def main(argv: list[str] | None = None) -> int:
    """JSON 输出和稳定退出码：0 通过，2 数据未就绪，1 命令错误。"""
    args = parser().parse_args(argv)
    try:
        result, status = execute(args, Settings())
    except ValidationError:
        result, status = {"error": "版本、行情或配置校验失败；检查必填字段、时区和摘要"}, 1
    except (OSError, sqlite3.Error, PostgresError, SQLAlchemyError, TimeoutError) as exc:
        result, status = {"error": "文件或数据库不可用，请运行 doctor 并检查数据源配置", "type": type(exc).__name__}, 1
    except ValueError as exc:
        result, status = {"error": str(exc)}, 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
