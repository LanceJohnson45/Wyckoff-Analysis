"""独立研究数据命令；仅显式初始化时修改研究 schema。"""

import argparse
import asyncio
import importlib.metadata
import json
import sqlite3
import subprocess
import sys
from datetime import date
from pathlib import Path

import httpx
from asyncpg import PostgresError
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from stock_research.artifacts import data_lock, write_once
from stock_research.benchmarks import BenchmarkBundle, fetch_benchmarks, merge_benchmarks
from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.config import MarketSourceSettings, Settings
from stock_research.data import read_market
from stock_research.errors import ResearchSourceChanged, ResearchUnavailable
from stock_research.factor_benchmark import benchmark
from stock_research.identity import digest
from stock_research.market_sync import sync_market
from stock_research.models import ClassificationVersion, Snapshot, UniverseVersion
from stock_research.parameters import MartinParameters
from stock_research.personal_integration import configure_personal
from stock_research.public_versions import refresh_versions
from stock_research.quality import build_snapshot
from stock_research.research_commands import analyze_command, read_run_command, run_command, track_run_command
from stock_research.schema import schema_ddl
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
    commands.add_parser("serve", help="启动仅监听loopback的独立研究API，须配置独立Bearer身份")
    commands.add_parser("init-schema", help="显式创建独立研究 schema v1；不更新已有表定义")
    commands.add_parser("schema-ddl", help="从Python schema导出可审阅DDL，不连接数据库")
    personal = commands.add_parser("personal-mcp", help="预览或精确追加Personal预设的三项只读股票工具")
    personal.add_argument("--profile", type=Path, default=Path.home() / ".dsh/profiles/superagent/cordis.patch.yml")
    personal.add_argument("--workspace", type=Path, default=Path.cwd())
    personal.add_argument("--python", type=Path)
    personal.add_argument("--apply", action="store_true")
    personal.add_argument("--remove", action="store_true", help="只撤回本次股票MCP，保留其他Personal配置")
    personal.add_argument("--expected-profile-hash")
    personal.add_argument("--backup-dir", type=Path)
    run = commands.add_parser("run", help="双策略独立初选及提交后互评，持久化预览不发送通知")
    run.add_argument("--market", choices=["us"], required=True)
    run.add_argument("--target-session", type=date.fromisoformat, required=True)
    run.add_argument("--preview", action="store_true", required=True)
    run.add_argument("--snapshot-file", type=Path)
    run.add_argument("--force", action="store_true")
    run.add_argument("--parent-run")
    read_run = commands.add_parser("read-run", help="从研究PostgreSQL独立读取已提交结果，不调用模型")
    read_run.add_argument("--id", required=True)
    track_run = commands.add_parser("track-run", help="显式将已有终态研究接入观察名单，不运行模型")
    track_run.add_argument("--id", required=True)
    sync = commands.add_parser("sync-market", help="从 PG_* 授权行情只读增量同步，不执行云端写入")
    sync.add_argument("--market", choices=["us"], required=True)
    sync.add_argument("--full", action="store_true")
    refresh_market = commands.add_parser("refresh-market", help="有界只读同步及已启用基准补充，不调用模型")
    refresh_market.add_argument("--as-of", type=date.fromisoformat, required=True)
    benchmarks = commands.add_parser("fetch-benchmarks", help="显式获取带供应商出处的三基准补充文件")
    benchmarks.add_argument("--as-of", type=date.fromisoformat, required=True)
    refresh = commands.add_parser("refresh-versions", help="固定公开聚合源提交，不把当前清单冒充严格历史成员")
    refresh.add_argument("--as-of", type=date.fromisoformat, required=True)
    refresh.add_argument("--version", required=True)
    refresh.add_argument("--output-dir", type=Path, required=True)
    refresh.add_argument("--allow-current-universe", action="store_true")
    analyze = commands.add_parser("analyze", help="Martin/Wyckoff量化观察，不调用模型、不生成交易指令")
    analyze.add_argument("--market", choices=["us"], required=True)
    analyze.add_argument("--as-of", type=date.fromisoformat, required=True)
    analyze.add_argument("--strategy", choices=["martin", "wyckoff"], required=True)
    analyze.add_argument("--no-llm", action="store_true")
    analyze.add_argument("--snapshot-file", type=Path)
    compare = commands.add_parser("benchmark", help="独立进程对拍ATR/ADR和冷/热耗时，不比较未对齐EMA")
    compare.add_argument("--as-of", type=date.fromisoformat, required=True)
    compare.add_argument("--profile", required=True)
    compare.add_argument("--snapshot-file", type=Path, required=True)
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
        extra_limitations = ()
        if settings.benchmark_path is not None:
            bundle = BenchmarkBundle.model_validate_json(settings.benchmark_path.read_text(encoding="utf-8"))
            bars = merge_benchmarks(bars, bundle, as_of, settings.history_rows, settings.benchmark_price_tolerance)
            identity = "composite:" + digest({"market": identity, "benchmarks": bundle.content_hash})
            issues = tuple(issue for issue in issues if issue.kind == "stock")
            extra_limitations = bundle.limitations + (
                f"基准对拍规则v1:SPY重叠>=100根,OHLC相对容忍={settings.benchmark_price_tolerance},成交量相对容忍=0.01",
            )
        snapshot = build_snapshot(
            universe,
            classifications,
            bars,
            identity,
            as_of,
            coverage_min=settings.coverage_min,
            history_rows=settings.history_rows,
            source_issues=issues,
            extra_limitations=extra_limitations,
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
    checks["benchmark_supplement"] = True
    if settings.benchmark_path is not None:
        try:
            bundle = BenchmarkBundle.model_validate_json(settings.benchmark_path.read_text(encoding="utf-8"))
            checks["benchmark_supplement"] = (
                bundle.as_of == complete_session and bundle.history_rows == settings.history_rows
            )
        except (OSError, ValueError):
            checks["benchmark_supplement"] = False
    try:
        checks["database"] = asyncio.run(database_command(settings, "doctor"))
    except (OSError, PostgresError, SQLAlchemyError, TimeoutError):
        checks["database"] = False
    ready = all(
        checks[name] is True
        for name in (
            "python",
            "calendar",
            "universe",
            "classifications",
            "local_market",
            "database",
            "benchmark_supplement",
        )
    )
    return {
        "ready": ready,
        "checks": checks,
        "api": "query_implemented_not_probed",
        "scheduler": "not_connected",
    }, 0 if ready else 2


def execute(args: argparse.Namespace, settings: Settings) -> tuple[object, int]:
    """CLI 与生产数据路径共用领域实现。"""
    if args.command == "doctor":
        return doctor(settings)
    if args.command == "serve":
        import uvicorn

        from stock_research.http_api import create_app

        uvicorn.run(create_app(settings), host=settings.api_host, port=settings.api_port, access_log=False)
        return {"stopped": True}, 0
    if args.command == "read-run":
        return asyncio.run(read_run_command(settings, args.id)), 0
    if args.command == "track-run":
        return asyncio.run(track_run_command(settings, args.id)), 0
    if args.command == "run":
        result = asyncio.run(
            run_command(settings, args.snapshot_file, args.target_session, parent_id=args.parent_run, force=args.force)
        )
        return result, 0 if result["status"] == "completed" else 2
    if args.command == "benchmark":
        parameters = MartinParameters.model_validate_json(settings.parameters_path.read_text(encoding="utf-8"))
        snapshot = Snapshot.model_validate_json(args.snapshot_file.read_text(encoding="utf-8"))
        if parameters.version != args.profile or snapshot.as_of != args.as_of:
            raise ValueError("对拍profile或截止日与输入版本不一致")
        result = benchmark(
            args.snapshot_file,
            parameters,
            settings.artifacts_path / "benchmarks",
            timeout_seconds=settings.stage_timeout_seconds,
        )
        return result, 0 if result["passed"] else 2
    if args.command == "analyze":
        if not args.no_llm:
            raise ValueError("当前仅实现量化观察，须显式传入 --no-llm")
        path = args.snapshot_file
        if path is None:
            result, status = snapshot_command(settings, args.as_of, offline=False)
            if status:
                return result, status
            path = Path(result["path"])
        return analyze_command(settings, path, args.as_of, strategy=args.strategy), 0
    if args.command == "fetch-benchmarks":
        return fetch_benchmarks(args.as_of, settings.history_rows, settings.artifacts_path / "benchmarks"), 0
    if args.command == "refresh-market":
        from stock_research.market_refresh import refresh_command

        return asyncio.run(refresh_command(settings, args.as_of))
    if args.command == "refresh-versions":
        if not args.allow_current_universe:
            raise ValueError("公开源为当前取得清单，须显式提供 --allow-current-universe 并保留回溯限制")
        if args.as_of > latest_complete_session():
            raise ValueError("研究截止日尚未完成收盘")
        return refresh_versions(settings, args.version, args.as_of, args.output_dir), 0
    if args.command == "snapshot":
        return snapshot_command(settings, args.as_of, offline=args.offline)
    if args.command == "sync-market":
        with data_lock(settings.artifacts_path / "locks", "us", latest_complete_session().isoformat()):
            result = asyncio.run(sync_market(MarketSourceSettings(), settings.local_market_path, full=args.full))
        return result, 2 if result["issues"] or result["rejected"] else 0
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
    if args.command == "schema-ddl":
        print(schema_ddl(), end="")
        return 0
    try:
        if args.command == "personal-mcp":
            result, status = (
                configure_personal(
                    args.profile,
                    args.workspace,
                    python=args.python,
                    apply=args.apply,
                    remove=args.remove,
                    expected_hash=args.expected_profile_hash,
                    backup_dir=args.backup_dir,
                ),
                0,
            )
        else:
            result, status = execute(args, Settings())
    except ValidationError:
        result, status = {"error": "版本、行情或配置校验失败；检查必填字段、时区和摘要"}, 1
    except ResearchSourceChanged as error:
        result, status = {"error": str(error)}, 1
    except ResearchUnavailable as error:
        result, status = {"error": str(error)}, 2
    except (
        OSError,
        sqlite3.Error,
        PostgresError,
        SQLAlchemyError,
        httpx.HTTPError,
        subprocess.TimeoutExpired,
        TimeoutError,
    ) as exc:
        result, status = {"error": "文件或数据库不可用，请运行 doctor 并检查数据源配置", "type": type(exc).__name__}, 1
    except ValueError as exc:
        result, status = {"error": str(exc)}, 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
