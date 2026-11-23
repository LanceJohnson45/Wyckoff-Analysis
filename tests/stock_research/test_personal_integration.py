"""配置增量、精确撤回和真正清空继承环境；不用用户的运行配置作测试。"""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from stock_research import personal_integration
from stock_research.cli import main
from stock_research.personal_integration import configure_personal, query_plugin

PROFILE = """# 保留原有配置与自定义标签
- insert:
    - id: preset-personal-agent
      name: '@deepseek-ai/dsh-agent-preset'
      config:
        id: personal-agent
        plugins:
          - id: personal-mcp
            name: '@deepseek-ai/dsh-mcp-client'
            config: {serverName: personal, transport: stdio, command: /operator/python}
          - id: custom-guard
            name: custom
            config: !!js |
              throw new Error('this must never execute');
        sentinel: 保持
- id: unrelated
  config: {private_key: do-not-echo-private-fixture}
"""


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    (root / "stock_research").mkdir(parents=True)
    (root / "stock_research/query_mcp.py").write_text("import json,os\nprint(json.dumps(dict(os.environ)))\n")
    (root / ".venv/bin").mkdir(parents=True)
    (root / ".venv/bin/python").symlink_to(sys.executable)
    (root / ".stock-research").mkdir()
    identity = root / ".stock-research/query.env"
    identity.write_text("STOCK_RESEARCH_QUERY_TOKEN=synthetic-readonly-must-not-appear-in-config\n")
    identity.chmod(0o600)
    return root


@pytest.fixture
def profile(tmp_path: Path) -> Path:
    path = tmp_path / "cordis.patch.yml"
    path.write_text(PROFILE)
    path.chmod(0o600)
    return path


def test_profile_preview_apply_noop_and_exact_revoke_keep_other_settings(profile: Path, workspace: Path) -> None:
    before = profile.read_bytes()
    preview = configure_personal(profile, workspace)
    assert preview["change_required"] and not preview["applied"] and not preview["runtime_verified"]
    assert profile.read_bytes() == before and not list(profile.parent.glob("*.bak"))
    assert "do-not-echo-private-fixture" not in json.dumps(preview)
    assert "synthetic-readonly" not in json.dumps(preview)
    backup_dir = profile.parent / "backups"
    applied = configure_personal(
        profile, workspace, apply=True, expected_hash=preview["before_hash"], backup_dir=backup_dir
    )
    after = profile.read_bytes()
    assert applied["applied"] and hashlib.sha256(after).hexdigest() == preview["after_hash"]
    assert Path(applied["backup"]).read_bytes() == before
    assert Path(applied["backup"]).stat().st_mode & 0o777 == 0o600
    assert profile.stat().st_mode & 0o777 == 0o600
    assert "throw new Error('this must never execute');" in after.decode()
    assert "private_key: do-not-echo-private-fixture" in after.decode()
    stamp = profile.stat().st_mtime_ns
    replay = configure_personal(
        profile, workspace, apply=True, expected_hash=preview["before_hash"], backup_dir=backup_dir
    )
    assert replay["already_in_desired_state"] and not replay["applied"] and profile.stat().st_mtime_ns == stamp
    assert len(list(backup_dir.glob("*.bak"))) == 1
    profile.write_bytes(after.replace("sentinel: 保持".encode(), "sentinel: 后来变更".encode()))
    (workspace / ".stock-research/query.env").unlink()
    revoke = configure_personal(profile, workspace, remove=True)
    assert revoke["operation"] == "remove" and revoke["plugin"] is None
    removed = configure_personal(
        profile, workspace, remove=True, apply=True, expected_hash=revoke["before_hash"], backup_dir=backup_dir
    )
    assert removed["applied"]
    assert profile.read_bytes() == before.replace("sentinel: 保持".encode(), "sentinel: 后来变更".encode())
    assert configure_personal(profile, workspace, remove=True)["already_in_desired_state"]


def test_actual_query_child_inherits_no_model_database_or_personal_identity(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key in (
        "PG_PASSWORD",
        "SA_INTERNAL_TOKEN",
        "RESEARCH_MODEL_API_KEY",
        "STOCK_RESEARCH_QUERY_TOKEN",
        "PYTHONPATH",
    ):
        monkeypatch.setenv(key, "must-not-inherit-private-fixture")
    plugin = query_plugin(workspace)
    config = plugin["config"]
    result = subprocess.run(
        [config["command"], *config["args"]], cwd=config["cwd"], check=True, capture_output=True, text=True, timeout=10
    )
    environment = json.loads(result.stdout)
    assert environment["PATH"] == "/usr/bin:/bin" and environment["PYTHONUTF8"] == "1"
    assert not set(environment) & {
        "PG_PASSWORD",
        "SA_INTERNAL_TOKEN",
        "RESEARCH_MODEL_API_KEY",
        "STOCK_RESEARCH_QUERY_TOKEN",
        "PYTHONPATH",
        "HOME",
    }
    assert "must-not-inherit" not in result.stdout and "synthetic-readonly" not in json.dumps(plugin)


def test_stale_preview_and_intervening_write_never_overwrite_profile(
    profile: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preview = configure_personal(profile, workspace)
    other = profile.read_bytes() + b"# another owner changed configuration\n"
    profile.write_bytes(other)
    with pytest.raises(ValueError, match="expected-profile-hash"):
        configure_personal(profile, workspace, apply=True, expected_hash=preview["before_hash"])
    assert profile.read_bytes() == other and not (profile.parent / "stock-research-backups").exists()
    current = configure_personal(profile, workspace)
    temporary = personal_integration.NamedTemporaryFile
    latest = other + b"# changed during staging\n"

    def concurrent_change(**kwargs):
        output = temporary(**kwargs)
        profile.write_bytes(latest)
        return output

    monkeypatch.setattr(personal_integration, "NamedTemporaryFile", concurrent_change)
    with pytest.raises(ValueError, match="再次变化"):
        configure_personal(profile, workspace, apply=True, expected_hash=current["before_hash"])
    assert profile.read_bytes() == latest
    backups = list((profile.parent / "stock-research-backups").glob("*.bak"))
    assert len(backups) == 1 and backups[0].read_bytes() == other
    assert sorted(item.name for item in profile.parent.iterdir()) == [
        "cordis.patch.yml",
        "cordis.patch.yml.stock-research.lock",
        "stock-research-backups",
        "workspace",
    ]


@pytest.mark.parametrize("change", ["duplicate", "namespace", "alias", "identity", "flow", "duplicate-key"])
def test_ambiguous_profiles_are_rejected_without_writes(profile: Path, workspace: Path, change: str) -> None:
    content = PROFILE
    if change == "duplicate":
        content += PROFILE
    elif change == "namespace":
        content = content.replace("serverName: personal", "serverName: stocks")
    elif change == "alias":
        content = content.replace("plugins:\n", "plugins: &shared\n") + "- id: other\n  config: {plugins: *shared}\n"
    elif change == "identity":
        content = content.replace("id: personal-agent", "id: other-preset")
    elif change == "flow":
        content = "- {id: preset-personal-agent, name: '@deepseek-ai/dsh-agent-preset', config: {id: personal-agent, plugins: []}}\n"
    elif change == "duplicate-key":
        content = content.replace("        sentinel: 保持", "        sentinel: 保持\n        sentinel: 重复")
    profile.write_text(content)
    with pytest.raises(ValueError):
        configure_personal(profile, workspace, apply=True, expected_hash=hashlib.sha256(content.encode()).hexdigest())
    assert profile.read_text() == content and not list(profile.parent.glob("*.bak"))


def test_existing_registration_cannot_be_overwritten_and_cli_does_not_load_server_settings(
    profile: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setattr(
        "stock_research.cli.Settings", lambda: pytest.fail("operator preview must not load server identities")
    )
    assert main(["personal-mcp", "--profile", str(profile), "--workspace", str(workspace)]) == 0
    preview = json.loads(capsys.readouterr().out)
    configure_personal(profile, workspace, apply=True, expected_hash=preview["before_hash"])
    original = profile.read_bytes()
    profile.write_bytes(original.replace(b"toolCallTimeoutMs: 20000", b"toolCallTimeoutMs: 10000"))
    with pytest.raises(ValueError, match="不能自动覆盖"):
        configure_personal(profile, workspace)
    assert profile.read_bytes() != original
    profile.write_text("- private: [do-not-echo-secret,\n")
    assert main(["personal-mcp", "--profile", str(profile), "--workspace", str(workspace)]) == 1
    output = capsys.readouterr().out
    assert "do-not-echo-secret" not in output
    assert "无法安全解析" in json.loads(output)["error"]


def test_identity_permissions_profile_symlink_and_missing_preview_hash_are_rejected(
    profile: Path, workspace: Path
) -> None:
    identity = workspace / ".stock-research/query.env"
    identity.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        configure_personal(profile, workspace)
    identity.chmod(0o600)
    alias = profile.with_name("profile-symlink.yml")
    alias.symlink_to(profile)
    with pytest.raises(ValueError, match="软链"):
        configure_personal(alias, workspace)
    with pytest.raises(ValueError, match="expected-profile-hash"):
        configure_personal(profile, workspace, apply=True)
    assert profile.read_text() == PROFILE
