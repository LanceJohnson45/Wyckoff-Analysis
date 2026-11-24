"""固定MVP子进程：各自读取既有行情或Personal身份，不向父进程返回凭据。"""

import argparse
import contextlib
import json
import os
import sys
import threading
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

MAX_INPUT_BYTES = 100_000


def parent_closed() -> None:
    try:
        os.read(sys.stdin.fileno(), 1)
    finally:
        os._exit(75)


def load_selected(path: Path, keys: set[str]) -> None:
    from dotenv import dotenv_values

    if not path.is_file() or path.is_symlink() or path.stat().st_mode & 0o077:
        raise ValueError("private_configuration_invalid")
    values = dotenv_values(path, interpolate=False)
    for key in keys:
        if values.get(key) is not None:
            os.environ[key] = values[key]


def refresh(payload: dict[str, object], env_file: Path) -> dict[str, object]:
    load_selected(
        env_file, {"PG_" + key for key in ("ENABLED", "HOST", "PORT", "DATABASE", "USER", "PASSWORD", "SSLMODE", "DSN")}
    )
    from scripts.us_research_refresh import refresh_research

    return refresh_research(payload["symbols"], date.fromisoformat(payload["as_of"]))


def notify(payload: dict[str, object], env_file: Path) -> dict[str, object]:
    load_selected(env_file, {"SA_PG_URL", "SA_PERSONAL_NOTIFY_TARGET", "SA_PERSONAL_WORKSPACE"})
    os.environ.update(SA_LOAD_DOTENV="0", SA_INTERNAL_TOKEN="")
    from src import personal, task_delivery

    workspace = os.environ["SA_PERSONAL_WORKSPACE"]
    target = os.environ["SA_PERSONAL_NOTIFY_TARGET"]
    if not target.startswith("ou_") or payload["workspace"] != workspace:
        raise ValueError("personal_recipient_invalid")
    task, window = "stock-research-premarket", payload["window"]
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    row = task_delivery.reserve(workspace, task, window, now)
    if row["decision"] == "prepare":
        task_delivery.ready(workspace, task, window, payload["body"], target=target)
    elif row["decision"] in {"busy", "exhausted"}:
        return {"state": row["decision"], "message_ref": ""}
    directory = Path(payload["directory"])
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    state = task_delivery.deliver(workspace, task, window, target, now, directory)
    with personal.connect() as conn:
        saved = conn.execute(
            "SELECT state,message_ref FROM personal.task_reports "
            "WHERE workspace_id=%s AND task_id=%s AND window_key=%s",
            (workspace, task, window),
        ).fetchone()
    return {"state": state, "message_ref": saved["message_ref"] or ""}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("refresh", "notify"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    raw = sys.stdin.buffer.readline(MAX_INPUT_BYTES + 1)
    threading.Thread(target=parent_closed, daemon=True).start()
    result = {"status": "failed", "error_code": "mvp_worker_failed"}
    try:
        if len(raw) > MAX_INPUT_BYTES:
            raise ValueError("worker_input_too_large")
        payload = json.loads(raw)
        sys.path.insert(0, str(args.root.resolve(strict=True)))
        # Provider and driver diagnostics can contain identities; only the bounded receipt leaves this child.
        with open(os.devnull, "w") as quiet, contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
            result = refresh(payload, args.env_file) if args.kind == "refresh" else notify(payload, args.env_file)
    except Exception as failure:
        result = {"status": "failed", "error_code": "mvp_worker_failed", "error_type": type(failure).__name__}
    sys.stdout.write(json.dumps(result) + "\n")
    return 1 if result.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
