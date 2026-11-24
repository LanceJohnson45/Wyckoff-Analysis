"""Single-user launchd tick; preserve New York slots and the original research deadline."""

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime, time
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stock_research.artifacts import data_lock, write_once
from stock_research.errors import PublicationScopeError
from stock_research.identity import canonical_json, new_id, now
from stock_research.mvp import execute_mvp
from stock_research.mvp_delivery import deliveries_sent
from stock_research.mvp_settings import load_mvp_settings
from stock_research.premarket import plan_premarket
from stock_research.schemas.premarket import NEW_YORK, PremarketInput


def daily_input(observed_at: datetime, workspace: str, *, manual: bool = False) -> PremarketInput:
    local = observed_at.astimezone(NEW_YORK)
    slot = observed_at if manual else datetime.combine(local.date(), time(8), NEW_YORK)
    stamp = slot.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return PremarketInput.model_validate(
        {
            "schedule": {
                "schedule_id": "stock-research-premarket",
                "event_id": "stock-research-premarket:" + ("manual-" if manual else "") + stamp,
                "scheduled_for": stamp,
                "workspace_id": workspace,
                "trigger_kind": "manual" if manual else "scheduled",
            }
        }
    )


def finished(result: dict[str, object]) -> bool:
    research = result.get("research") or {}
    receipt = research.get("receipt") or {}
    return deliveries_sent(result) and receipt.get("status") in {
        "completed",
        "partial",
        "failed",
        "failed_preparation",
        "cancelled",
        "expired",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--workspace", required=True, help="operator fixed Personal workspace")
    parser.add_argument("--manual", action="store_true")
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        settings = load_mvp_settings(args.config)
        data = daily_input(now(), args.workspace, manual=args.manual)
        plan = plan_premarket(data, now())
        if plan["status"] in {"before_start", "expired", "skipped_non_session"}:
            return 0
        root = settings.state_path
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        root.chmod(0o700)
        with data_lock(root, "us", str(data.schedule.target_session)):
            done = root / str(data.schedule.target_session) / ("manual-done.json" if args.manual else "done.json")
            if done.exists():
                return 0
            directory = done.parent / "executions" / new_id()
            directory.mkdir(parents=True, mode=0o700)
            write_once(directory / "input.json", data.model_dump_json() + "\n")
            result = asyncio.run(execute_mvp(settings, data))
            write_once(directory / "output.json", canonical_json(result) + "\n")
            if finished(result):
                write_once(done, canonical_json({"execution": directory.name, "initial": result["initial"]}) + "\n")
        print(
            canonical_json(
                {
                    "mode": "mvp_tick",
                    "execution": directory.name,
                    "initial": result.get("initial"),
                    "research": (result.get("research") or {}).get("error_code"),
                }
            )
        )
        return 0 if finished(result) else 1
    except PublicationScopeError:
        print(json.dumps({"mode": "mvp_tick", "error_code": "mvp_publication_scope_unconfirmed"}))
        return 1
    except (OSError, ValueError, KeyError, TimeoutError):
        print(json.dumps({"mode": "mvp_tick", "error_code": "mvp_tick_unconfirmed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
