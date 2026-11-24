"""固定Personal通知账本只读子进程；不加载或执行投递组件。"""

import argparse
import asyncio
import contextlib
import json
import os
import re
import sys
import threading
from datetime import date, datetime
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo

if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg
from sqlalchemy import column, select, table
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.stock_research_mvp_worker import load_selected, parent_closed

NATIVE_REFERENCE = r"om_[A-Za-z0-9_]+(?:,om_[A-Za-z0-9_]+)*"
WINDOW = r"us:[0-9]{4}-[0-9]{2}-[0-9]{2}:(initial|recovery)"
REPORTS = table(
    "task_reports",
    column("workspace_id"),
    column("task_id"),
    column("window_key"),
    column("state"),
    column("message_ref"),
    column("updated_at"),
    schema="personal",
)


def phase_view(row: dict[str, object] | None) -> dict[str, object]:
    """账本sent缺少可核对的原生引用时，不冒充送达成功。"""
    if row is None:
        return {"state": "not_recorded", "ledger_update_time": None, "has_native_receipt": False}
    receipt = bool(re.fullmatch(NATIVE_REFERENCE, row["message_ref"] or ""))
    state = "unconfirmed" if row["state"] == "sent" and not receipt else row["state"]
    return {"state": state, "ledger_update_time": row["updated_at"].isoformat(), "has_native_receipt": receipt}


async def read_status(payload: dict[str, object], env_file: Path) -> dict[str, object]:
    """沿既有Personal身份读取小投影；独立事务提供一致的只读视图。"""
    if set(payload) != {"workspace", "target_session"}:
        raise ValueError("invalid_status_input")
    target = date.fromisoformat(payload["target_session"]).isoformat()
    if target != payload["target_session"]:
        raise ValueError("invalid_status_date")
    load_selected(env_file, {"SA_PG_URL", "SA_PERSONAL_WORKSPACE"})
    workspace = os.environ["SA_PERSONAL_WORKSPACE"]
    if not workspace or payload["workspace"] != workspace:
        raise ValueError("personal_workspace_invalid")
    url = make_url(os.environ["SA_PG_URL"])
    if (
        url.drivername not in {"postgresql", "postgresql+psycopg"}
        or not url.database
        or not (url.host or url.query.get("host"))
    ):
        raise ValueError("personal_database_invalid")
    engine = create_async_engine(
        "postgresql+asyncpg://",
        async_creator=partial(
            asyncpg.connect,
            dsn=url.set(drivername="postgresql").render_as_string(hide_password=False),
            timeout=5,
            command_timeout=5,
        ),
        pool_size=1,
        max_overflow=0,
    )
    try:
        async with engine.connect() as connection:
            connection = await connection.execution_options(isolation_level="REPEATABLE READ", postgresql_readonly=True)
            async with connection.begin():
                scope = (REPORTS.c.workspace_id == workspace, REPORTS.c.task_id == "stock-research-premarket")
                rows = (
                    (
                        await connection.execute(
                            select(
                                REPORTS.c.window_key, REPORTS.c.state, REPORTS.c.message_ref, REPORTS.c.updated_at
                            ).where(*scope, REPORTS.c.window_key.in_((f"us:{target}:initial", f"us:{target}:recovery")))
                        )
                    )
                    .mappings()
                    .all()
                )
                latest = (
                    (
                        await connection.execute(
                            select(REPORTS.c.window_key, REPORTS.c.updated_at)
                            .where(
                                *scope,
                                REPORTS.c.state == "sent",
                                REPORTS.c.message_ref.op("~")("^" + NATIVE_REFERENCE + "$"),
                                REPORTS.c.window_key.op("~")("^" + WINDOW + "$"),
                            )
                            .order_by(REPORTS.c.updated_at.desc(), REPORTS.c.window_key.desc())
                            .limit(1)
                        )
                    )
                    .mappings()
                    .first()
                )
    finally:
        await asyncio.wait_for(engine.dispose(), 5)
    phases = {row["window_key"].rsplit(":", 1)[1]: row for row in rows}
    recent = None
    if latest:
        _, day, phase = latest["window_key"].split(":")
        recent = {
            "target_session": date.fromisoformat(day).isoformat(),
            "phase": phase,
            "ledger_update_time": latest["updated_at"].isoformat(),
        }
    return {
        "target_session": target,
        "checked_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
        "initial": phase_view(phases.get("initial")),
        "recovery": phase_view(phases.get("recovery")),
        "latest_sent": recent,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    args = parser.parse_args()
    raw = sys.stdin.buffer.readline(2049)
    threading.Thread(target=parent_closed, daemon=True).start()
    try:
        if len(raw) > 2048 or args.root.resolve(strict=True) != Path(__file__).resolve().parents[1]:
            raise ValueError("status_input_invalid")
        with open(os.devnull, "w") as quiet, contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
            result = asyncio.run(read_status(json.loads(raw), args.env_file))
    except Exception:
        sys.stdout.write('{"error_code":"delivery_status_unavailable"}\n')
        return 1
    sys.stdout.write(json.dumps(result) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
