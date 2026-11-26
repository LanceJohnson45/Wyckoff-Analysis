"""同日手动与定点通知各自冻结，未知投递重放原业务身份。"""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts import stock_research_mvp_schedule as tick
from scripts.stock_research_mvp_schedule import daily_input
from stock_research import mvp, mvp_delivery
from stock_research.errors import PublicationScopeError
from stock_research.identity import canonical_json, digest, new_id
from stock_research.mvp_delivery import Publication, freeze_publication, publish
from stock_research.mvp_settings import MvpSettings
from tests.stock_research.test_mvp import settings as settings


@pytest.mark.parametrize("manual_first", [True, False])
@pytest.mark.parametrize("phase", ["initial", "recovery"])
def test_manual_and_scheduled_publications_keep_separate_frozen_scope(
    settings: MvpSettings, monkeypatch: pytest.MonkeyPatch, manual_first: bool, phase: str
) -> None:
    async def scenario() -> None:
        transmitted = []

        async def transport(_settings, kind, payload, _timeout):
            assert kind == "notify"
            transmitted.append(payload)
            return {"state": "unknown", "message_ref": ""}

        monkeypatch.setattr(mvp_delivery, "call_worker", transport)
        requests = []
        for manual in (manual_first, not manual_first):
            data = daily_input(datetime(2026, 11, 23, 13, 1, tzinfo=UTC), "self", manual=manual)
            candidate = Publication(body="手动结果" if manual else "定点结果", completed_stages=2, report_id=new_id())
            frozen, receipt = await publish(settings, data, phase, candidate)
            assert frozen == candidate
            assert receipt.state == "unknown"
            expected = f"us:{data.schedule.target_session}:" + ("manual:" if manual else "") + phase
            assert transmitted[-1]["window"] == expected
            directory = Path(transmitted[-1]["directory"])
            assert json.loads((directory / "scope.json").read_text()) == {
                **data.schedule.research_scope(),
                "phase": phase,
                "publication_hash": digest(candidate.model_dump(mode="json")),
            }
            assert (directory / "scope.json").stat().st_mode & 0o077 == 0
            requests.append((data, candidate, transmitted[-1]))
        assert requests[0][2]["window"] != requests[1][2]["window"]
        assert requests[0][2]["directory"] != requests[1][2]["directory"]
        for data, candidate, sent in requests:
            later = candidate.model_copy(update={"body": "新结果不能替换原通知", "report_id": new_id()})
            frozen, receipt = await publish(settings, data, phase, later)
            assert frozen == candidate and receipt.state == "unknown"
            assert transmitted[-1] == sent

    asyncio.run(scenario())


@pytest.mark.parametrize("manual", [True, False])
@pytest.mark.parametrize("phase", ["initial", "recovery"])
def test_unscoped_legacy_publication_is_preserved_and_blocks_delivery(
    settings: MvpSettings, monkeypatch: pytest.MonkeyPatch, manual: bool, phase: str
) -> None:
    async def forbidden(*_args):
        raise AssertionError("来源未确认时不能调用通知transport")

    monkeypatch.setattr(mvp_delivery, "call_worker", forbidden)
    data = daily_input(datetime(2026, 11, 23, 13, 1, tzinfo=UTC), "self", manual=manual)
    legacy = settings.state_path / str(data.schedule.target_session) / phase
    original = Publication(body="旧正文来源待核对", completed_stages=2, report_id=new_id())
    freeze_publication(legacy, original)
    raw = (legacy / "publication.json").read_bytes()
    with pytest.raises(PublicationScopeError, match="旧通知来源未确认"):
        asyncio.run(publish(settings, data, phase, original.model_copy(update={"body": "不可替换"})))
    assert (legacy / "publication.json").read_bytes() == raw
    assert not (legacy / "scope.json").exists()


@pytest.mark.parametrize("damage", ["workspace", "phase", "missing_body", "body_changed", "bad_body"])
def test_scoped_publication_damage_cannot_change_unknown_delivery(
    settings: MvpSettings, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    async def scenario() -> None:
        async def unknown(*_args):
            return {"state": "unknown", "message_ref": ""}

        monkeypatch.setattr(mvp_delivery, "call_worker", unknown)
        data = daily_input(datetime(2026, 11, 23, 13, 1, tzinfo=UTC), "self", manual=True)
        original = Publication(body="已尝试发送的原正文", completed_stages=2, report_id=new_id())
        await publish(settings, data, "initial", original)
        directory = settings.state_path / str(data.schedule.target_session) / "manual" / "initial"
        marker = directory / "scope.json"
        scope = json.loads(marker.read_text())
        if damage == "missing_body":
            (directory / "publication.json").unlink()
        elif damage == "body_changed":
            (directory / "publication.json").write_text(
                original.model_copy(update={"body": "事后篡改"}).model_dump_json()
            )
        elif damage == "bad_body":
            (directory / "publication.json").write_text("{")
        else:
            scope["workspace_id" if damage == "workspace" else "phase"] = "other"
            marker.write_text(json.dumps(scope))
        raw = marker.read_bytes()
        body = (directory / "publication.json").read_bytes() if damage != "missing_body" else None
        with pytest.raises(PublicationScopeError, match="范围不符"):
            await publish(settings, data, "initial", original.model_copy(update={"body": "不可重建正文"}))
        assert marker.read_bytes() == raw
        if damage == "missing_body":
            assert not (directory / "publication.json").exists()
        else:
            assert (directory / "publication.json").read_bytes() == body

    asyncio.run(scenario())


def test_explicitly_verified_legacy_scope_preserves_original_body_and_key(
    settings: MvpSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        transmitted = []

        async def transport(_settings, _kind, payload, _timeout):
            transmitted.append(payload)
            return {"state": "unknown", "message_ref": ""}

        monkeypatch.setattr(mvp_delivery, "call_worker", transport)
        data = daily_input(datetime(2026, 11, 23, 13, 1, tzinfo=UTC), "self")
        directory = settings.state_path / str(data.schedule.target_session) / "initial"
        original = Publication(body="已由操作员核对的定点旧正文", completed_stages=2, report_id=new_id())
        freeze_publication(directory, original)
        raw = (directory / "publication.json").read_bytes()
        (directory / "scope.json").write_text(
            canonical_json(
                {
                    **data.schedule.research_scope(),
                    "phase": "initial",
                    "publication_hash": digest(original.model_dump(mode="json")),
                }
            )
            + "\n"
        )
        frozen, result = await publish(settings, data, "initial", original.model_copy(update={"body": "不可替换"}))
        assert frozen == original and result.state == "unknown"
        assert transmitted[0]["window"] == "us:2026-11-23:initial" and transmitted[0]["body"] == original.body
        assert (directory / "publication.json").read_bytes() == raw

    asyncio.run(scenario())


@pytest.mark.parametrize("entry", ["tick", "workflow"])
def test_legacy_scope_failure_reaches_entrypoint_without_finishing_or_overwriting(
    settings: MvpSettings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    entry: str,
) -> None:
    stamp = datetime(2026, 11, 23, 13, 30, tzinfo=UTC)
    monkeypatch.setattr(tick, "now", lambda: stamp)
    monkeypatch.setattr(mvp, "now", lambda: stamp)
    config = tmp_path / "mvp.env"
    config.write_text(
        "\n".join(
            "STOCK_RESEARCH_MVP_" + key.upper() + "=" + str(value) for key, value in settings.model_dump().items()
        )
    )
    config.chmod(0o600)
    data = daily_input(stamp, "self", manual=True)
    root = settings.state_path / str(data.schedule.target_session)
    original = Publication(body="不可自动判断来源的旧首报", completed_stages=2, report_id=new_id())
    freeze_publication(root / "initial", original)
    raw = (root / "initial/publication.json").read_bytes()
    if entry == "tick":
        result = tick.main(["--config", str(config), "--workspace", "self", "--manual"])
    else:
        directory = tmp_path / "runner" / "execution"
        directory.mkdir(parents=True)
        (directory / "input.json").write_text(data.model_dump_json())
        for key, value in {
            "SA_WORKFLOW_NAME": "stock-research-premarket",
            "SA_RUN_ARTIFACTS": str(directory.parent),
            "SA_RUN_INPUT_JSON": str(directory / "input.json"),
            "SA_RUN_OUTPUT_JSON": str(directory / "output.json"),
        }.items():
            monkeypatch.setenv(key, value)
        result = mvp.main(["--config", str(config)])
        assert not (directory / "output.json").exists()
    assert result == 1
    assert json.loads(capsys.readouterr().out)["error_code"] == "mvp_publication_scope_unconfirmed"
    assert not (root / "done.json").exists() and not (root / "manual-done.json").exists()
    assert (root / "initial/publication.json").read_bytes() == raw
