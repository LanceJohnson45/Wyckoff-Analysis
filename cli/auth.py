# -*- coding: utf-8 -*-
"""CLI 认证 — 复用 Supabase Auth，session 持久化到 ~/.wyckoff/session.json。"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SESSION_DIR = Path.home() / ".wyckoff"
SESSION_FILE = SESSION_DIR / "session.json"

from core.constants import SUPABASE_ANON_URL as _SUPABASE_URL, SUPABASE_ANON_KEY as _SUPABASE_KEY


# ---------------------------------------------------------------------------
# Session 文件读写
# ---------------------------------------------------------------------------

def _save_session(data: dict[str, Any]) -> None:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _load_session() -> dict[str, Any] | None:
    if not SESSION_FILE.exists():
        return None
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _clear_session() -> None:
    try:
        SESSION_FILE.unlink(missing_ok=True)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# 登录 / 登出 / 恢复
# ---------------------------------------------------------------------------

def _create_client():
    """用内置的 anon key 创建 Supabase 客户端（不依赖 .env）。"""
    from supabase import create_client
    return create_client(_SUPABASE_URL, _SUPABASE_KEY)


def login(email: str, password: str) -> dict[str, Any]:
    """
    用邮箱密码登录 Supabase，返回用户信息。

    Returns: {"user_id": str, "email": str, "access_token": str, "refresh_token": str}
    Raises: Exception on auth failure
    """
    client = _create_client()
    resp = client.auth.sign_in_with_password({"email": email, "password": password})

    data = {
        "user_id": resp.user.id,
        "email": resp.user.email,
        "access_token": resp.session.access_token,
        "refresh_token": resp.session.refresh_token,
    }
    _save_session(data)
    return data


def restore_session() -> dict[str, Any] | None:
    """
    从 ~/.wyckoff/session.json 恢复登录态。

    Returns: 同 login() 的返回值，或 None（无 session / token 过期）。
    """
    data = _load_session()
    if not data or not data.get("access_token") or not data.get("refresh_token"):
        return None

    try:
        client = _create_client()
        client.auth.set_session(data["access_token"], data["refresh_token"])
        user_resp = client.auth.get_user()

        if not user_resp or not user_resp.user:
            _clear_session()
            return None

        # token 可能被 refresh，更新本地缓存
        session = client.auth.get_session()
        if session:
            data["access_token"] = session.access_token
            data["refresh_token"] = session.refresh_token
            _save_session(data)

        return data
    except Exception as e:
        logger.debug("Session restore failed", exc_info=True)
        err = str(e).lower()
        # 仅在 token 确认无效时清除；网络异常保留本地 session
        if "invalid" in err or "expired" in err or "revoked" in err:
            _clear_session()
            return None
        # 网络问题：保留 session，用本地缓存的 token 继续
        return data


def logout() -> None:
    """清除本地 session。"""
    _clear_session()


# ---------------------------------------------------------------------------
# 统一配置文件 ~/.wyckoff/wyckoff.json
# ---------------------------------------------------------------------------

CONFIG_FILE = SESSION_DIR / "wyckoff.json"
_OLD_CONFIG_FILE = SESSION_DIR / "config.json"


def _load_config() -> dict[str, Any]:
    """加载配置文件，首次运行自动迁移旧 config.json。"""
    if not CONFIG_FILE.exists() and _OLD_CONFIG_FILE.exists():
        try:
            _OLD_CONFIG_FILE.rename(CONFIG_FILE)
        except OSError:
            pass
    if not CONFIG_FILE.exists():
        return {}
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_config(data: dict[str, Any]) -> None:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
    )


def save_model_config(config: dict[str, Any]) -> None:
    """将模型配置合并写入 wyckoff.json。"""
    data = _load_config()
    data.update(config)
    _save_config(data)


def load_model_config() -> dict[str, Any] | None:
    """加载模型配置部分。"""
    data = _load_config()
    if data.get("provider_name") and data.get("api_key"):
        return data
    return None


def load_config() -> dict[str, Any]:
    """加载完整配置。"""
    return _load_config()


def save_config_key(key: str, value: Any) -> None:
    """写入单个配置项。"""
    data = _load_config()
    data[key] = value
    _save_config(data)
