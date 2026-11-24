"""只给既有Personal预设追加只读MCP；不解释或重写其余运行配置。"""

import fcntl
import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path
from tempfile import NamedTemporaryFile

import yaml

from stock_research.identity import new_id

PRESET_ID = "preset-personal-agent"
PLUGIN_ID = "stock-research-mcp"
PROFILE_MAX_BYTES = 2_000_000


def query_plugin(workspace: Path, python: Path | None = None) -> dict[str, object]:
    """凭据只由清空继承环境的查询进程读取，配置片段只有固定argv和路径。"""
    root = workspace.expanduser().resolve(strict=True)
    executable = (python or root / ".venv/bin/python").expanduser().absolute()
    identity = root / ".stock-research/query.env"
    if not (root / "stock_research/query_mcp.py").is_file() or not os.access(executable, os.X_OK):
        raise ValueError("股票查询源码或Python解释器不可用，请先安装当前研究版本")
    if identity.is_symlink() or not identity.is_file() or identity.stat().st_mode & 0o077:
        raise ValueError("专用股票只读配置不存在或权限过宽，请准备0600的query.env")
    return {
        "id": PLUGIN_ID,
        "name": "@deepseek-ai/dsh-mcp-client",
        "config": {
            "serverName": "stocks",
            "transport": "stdio",
            "command": "/usr/bin/env",
            "args": ["-i", "PATH=/usr/bin:/bin", "PYTHONUTF8=1", str(executable), "-m", "stock_research.query_mcp"],
            "cwd": str(root),
            "toolCallTimeoutMs": 20000,
            "failOnStartupError": True,
        },
    }


def fields(node: yaml.Node) -> dict[str, yaml.Node]:
    """自定义JS标签只保留为节点；重复键不能悄悄选取最后一个。"""
    if not isinstance(node, yaml.MappingNode):
        raise ValueError("Personal配置节点必须为映射")
    result = {}
    for key, value in node.value:
        if not isinstance(key, yaml.ScalarNode) or key.tag != "tag:yaml.org,2002:str" or key.value in result:
            raise ValueError("Personal配置含重复或非文字键，须人工核对")
        result[key.value] = value
    return result


def personal_plugins(document: yaml.Node) -> yaml.SequenceNode:
    """定位唯一预设，别的预设或全局插件不能成为默认写入目标。"""
    pending, seen, matches, references = [document], set(), [], {}
    while pending:
        node = pending.pop()
        references[id(node)] = references.get(id(node), 0) + 1
        if id(node) in seen:
            continue
        seen.add(id(node))
        if len(seen) > 50000:
            raise ValueError("Personal配置节点过多")
        if isinstance(node, yaml.MappingNode):
            body = fields(node)
            identifier = body.get("id")
            if isinstance(identifier, yaml.ScalarNode) and identifier.value == PRESET_ID:
                config = fields(body.get("config"))
                if (
                    body.get("name", yaml.ScalarNode("", "")).value != "@deepseek-ai/dsh-agent-preset"
                    or config.get("id", yaml.ScalarNode("", "")).value != "personal-agent"
                ):
                    raise ValueError("Personal预设类型或配置身份不一致")
                matches.append((node, config.get("plugins")))
            pending.extend(body.values())
        elif isinstance(node, yaml.SequenceNode):
            pending.extend(node.value)
    if len(matches) != 1 or not isinstance(matches[0][1], yaml.SequenceNode):
        raise ValueError("Personal预设缺失、重复或没有明确的plugins列表")
    preset, plugins = matches[0]
    if references[id(preset)] != 1 or references[id(plugins)] != 1:
        raise ValueError("Personal预设或plugins被别处别名复用，不能自动修改")
    return plugins


def append_plugin(content: bytes, plugin: dict[str, object]) -> bytes:
    """沿原文本边界插入，移除新增节点后必须与原配置序列化完全一致。"""
    if len(content) > PROFILE_MAX_BYTES:
        raise ValueError("Personal运行配置超过限额")
    raw = content.decode("utf-8")
    document = yaml.compose(raw)
    plugins = personal_plugins(document)
    existing = [item for item in plugins.value if fields(item).get("id", yaml.ScalarNode("", "")).value == PLUGIN_ID]
    if len(existing) > 1:
        raise ValueError("股票MCP插件重复，须先核对现有注册")
    if existing:
        if yaml.safe_load(yaml.serialize(existing[0])) != plugin:
            raise ValueError("已有股票MCP配置不同，不能自动覆盖")
        return content
    for item in plugins.value:
        body = fields(item)
        if body.get("name", yaml.ScalarNode("", "")).value == "@deepseek-ai/dsh-mcp-client":
            config = fields(body.get("config"))
            if config.get("serverName", yaml.ScalarNode("", "")).value == "stocks":
                raise ValueError("stocks工具命名空间已有其他注册，不能追加重复服务器")
    if plugins.flow_style or not plugins.value or plugins.value[0].start_mark.column < 2:
        raise ValueError("Personal插件须为已有的非空块列表，不能重写其他配置")
    point = (
        len(raw)
        if plugins.end_mark.index == len(raw)
        else sum(len(line) for line in raw.splitlines(keepends=True)[: plugins.end_mark.line])
    )
    indentation = " " * (plugins.value[0].start_mark.column - 2)
    insertion = yaml.safe_dump([plugin], allow_unicode=True, sort_keys=False)
    insertion = "".join(indentation + line for line in insertion.splitlines(keepends=True))
    prefix = raw[:point] + ("" if raw[:point].endswith("\n") else "\n")
    candidate = prefix + insertion + raw[point:]
    checked = yaml.compose(candidate)
    appended = personal_plugins(checked)
    if yaml.safe_load(yaml.serialize(appended.value.pop())) != plugin or yaml.serialize(checked) != yaml.serialize(
        document
    ):
        raise ValueError("插入改变了其他Personal配置，拒绝写入")
    return candidate.encode("utf-8")


def remove_plugin(content: bytes) -> bytes:
    """撤回仅移除本次命名的MCP，不用旧备份覆盖后来配置或要求原查询文件仍在。"""
    if len(content) > PROFILE_MAX_BYTES:
        raise ValueError("Personal运行配置超过限额")
    raw = content.decode("utf-8")
    document = yaml.compose(raw)
    plugins = personal_plugins(document)
    existing = [item for item in plugins.value if fields(item).get("id", yaml.ScalarNode("", "")).value == PLUGIN_ID]
    if len(existing) > 1:
        raise ValueError("股票MCP插件重复，不能自动撤回")
    if not existing:
        return content
    node = existing[0]
    body = fields(node)
    config = fields(body.get("config"))
    if (
        body.get("name", yaml.ScalarNode("", "")).value != "@deepseek-ai/dsh-mcp-client"
        or config.get("serverName", yaml.ScalarNode("", "")).value != "stocks"
    ):
        raise ValueError("股票MCP身份不一致，不能删除其他插件")
    if plugins.flow_style or node.flow_style or len(plugins.value) < 2:
        raise ValueError("撤回不能重写流式配置或清空Personal原插件")
    lines = raw.splitlines(keepends=True)
    start = sum(len(line) for line in lines[: node.start_mark.line])
    end = len(raw) if node.end_mark.index == len(raw) else sum(len(line) for line in lines[: node.end_mark.line])
    candidate = raw[:start] + raw[end:]
    plugins.value.remove(node)
    if yaml.serialize(yaml.compose(candidate)) != yaml.serialize(document):
        raise ValueError("撤回改变了其他Personal配置，拒绝写入")
    return candidate.encode("utf-8")


def replace_plugin(content: bytes, plugin: dict[str, object]) -> bytes:
    """迁移只允许变更受管查询节点的目录/解释器，单次CAS前不写中间配置。"""
    if len(content) > PROFILE_MAX_BYTES:
        raise ValueError("Personal运行配置超过限额")
    document = yaml.compose(content.decode("utf-8"))
    plugins = personal_plugins(document)
    existing = [item for item in plugins.value if fields(item).get("id", yaml.ScalarNode("", "")).value == PLUGIN_ID]
    if len(existing) != 1:
        raise ValueError("股票MCP尚未注册或存在重复，不能迁移")
    previous = yaml.safe_load(yaml.serialize(existing[0]))
    config = previous.get("config", {})
    args = config.get("args", []) if isinstance(config, dict) else []
    expected = deepcopy(plugin)
    if (
        not isinstance(args, list)
        or len(args) != len(expected["config"]["args"])
        or not isinstance(args[3], str)
        or not Path(args[3]).is_absolute()
        or not isinstance(config.get("cwd"), str)
        or not Path(config["cwd"]).is_absolute()
    ):
        raise ValueError("仅允许迁移受管股票MCP的目录和解释器")
    expected["config"]["args"][3] = args[3]
    expected["config"]["cwd"] = config["cwd"]
    if previous != expected:
        raise ValueError("仅允许迁移受管股票MCP的目录和解释器")
    for item in plugins.value:
        body = fields(item)
        if item is not existing[0] and body.get("name", yaml.ScalarNode("", "")).value == "@deepseek-ai/dsh-mcp-client":
            if fields(body.get("config")).get("serverName", yaml.ScalarNode("", "")).value == "stocks":
                raise ValueError("stocks工具命名空间已有其他注册，不能迁移")
    retained = remove_plugin(content)
    node = existing[0]
    config_nodes = fields(fields(node)["config"])
    changes = [
        (config_nodes["cwd"], plugin["config"]["cwd"]),
        (config_nodes["args"].value[3], plugin["config"]["args"][3]),
    ]
    raw = content.decode("utf-8")
    for scalar, value in sorted(changes, key=lambda item: item[0].start_mark.index, reverse=True):
        if scalar.start_mark.index < node.start_mark.index or scalar.end_mark.index > node.end_mark.index:
            raise ValueError("查询路径被外部别名复用，不能迁移")
        raw = raw[: scalar.start_mark.index] + json.dumps(value, ensure_ascii=False) + raw[scalar.end_mark.index :]
    candidate = raw.encode("utf-8")
    checked = personal_plugins(yaml.compose(raw)).value[plugins.value.index(node)]
    if yaml.safe_load(yaml.serialize(checked)) != plugin:
        raise ValueError("迁移后的股票MCP与目标不一致")
    if remove_plugin(candidate) != retained:
        raise ValueError("迁移改变了其他Personal文本，拒绝写入")
    return content if previous == plugin else candidate


def apply_profile(profile: Path, before: bytes, after: bytes, expected_hash: str, backup_dir: Path) -> Path:
    """合作式文件锁和写前摘要检查；备份与替换文件都不继承宽权限。"""
    backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    backup_dir.chmod(0o700)
    lock = profile.with_name(profile.name + ".stock-research.lock")
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "r+") as handle:
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if hashlib.sha256(before).hexdigest() != expected_hash or profile.read_bytes() != before:
            raise ValueError("Personal配置已变化，请重新预览后再提交原摘要")
        backup = backup_dir / f"{profile.name}.{new_id()}.bak"
        backup_fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(backup_fd, "wb") as output:
            output.write(before)
            output.flush()
            os.fsync(output.fileno())
        temporary = None
        try:
            with NamedTemporaryFile(dir=profile.parent, delete=False) as output:
                temporary = Path(output.name)
                output.write(after)
                output.flush()
                os.fsync(output.fileno())
            if profile.read_bytes() != before:
                raise ValueError("Personal配置写入前再次变化，已保留原备份")
            os.replace(temporary, profile)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return backup


def configure_personal(
    profile: Path,
    workspace: Path,
    *,
    python: Path | None = None,
    expected_hash: str | None = None,
    apply: bool = False,
    backup_dir: Path | None = None,
    remove: bool = False,
    replace: bool = False,
) -> dict[str, object]:
    """预览不写配置；显式提交绑定预览摘要，不启动服务、模型或通知。"""
    profile = profile.expanduser().absolute()
    if profile.is_symlink():
        raise ValueError("Personal配置不能是软链，请核对真实受管文件")
    if remove and replace:
        raise ValueError("不能同时撤回和迁移股票MCP")
    plugin = None if remove else query_plugin(workspace, python)
    if not profile.is_file():
        raise ValueError("Personal配置必须为已有的普通文件")
    with profile.open("rb") as source:
        before = source.read(PROFILE_MAX_BYTES + 1)
    try:
        if remove:
            after = remove_plugin(before)
        else:
            after = replace_plugin(before, plugin) if replace else append_plugin(before, plugin)
    except (yaml.YAMLError, UnicodeError, RecursionError):
        raise ValueError("Personal配置无法安全解析，请检查语法及嵌套") from None
    before_hash = hashlib.sha256(before).hexdigest()
    if apply and (not expected_hash or (after != before and expected_hash != before_hash)):
        raise ValueError("提交必须提供与当前预览一致的expected-profile-hash")
    backup = None
    if apply and after != before:
        backup = apply_profile(
            profile, before, after, expected_hash, backup_dir or profile.parent / "stock-research-backups"
        )
    return {
        "profile": str(profile),
        "before_hash": before_hash,
        "after_hash": hashlib.sha256(after).hexdigest(),
        "change_required": before != after,
        "applied": bool(backup),
        "already_in_desired_state": before == after,
        "operation": "remove" if remove else "replace" if replace else "install",
        "backup": str(backup) if backup else None,
        "plugin": plugin,
        "runtime_verified": False,
    }
