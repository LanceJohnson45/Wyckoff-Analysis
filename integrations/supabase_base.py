# -*- coding: utf-8 -*-
"""
Supabase 客户端工厂 — 不依赖 Streamlit，CLI / 脚本 / Web 通用。

所有需要 Supabase 客户端的代码应从此模块获取，而不是各自 create_client。
- 脚本/定时任务：使用 create_admin_client()（service_role key，绕过 RLS）
- Web 端：使用 integrations.supabase_client.get_supabase_client()（内部调本模块 + 绑定用户 session）
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from supabase import Client


def create_admin_client() -> "Client":
    """Service-role 客户端（写库用，不经过 RLS）。

    优先读 SUPABASE_SERVICE_ROLE_KEY，回退到 SUPABASE_KEY。
    """
    from supabase import create_client

    url = os.getenv("SUPABASE_URL", "").strip()
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        or os.getenv("SUPABASE_KEY", "").strip()
    )
    if not url or not key:
        raise ValueError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY 未配置")
    return create_client(url, key)


def create_anon_client() -> "Client":
    """Anon-key 客户端（RLS 保护）。

    Web 端由 supabase_client.get_supabase_client() 在此基础上绑定用户 session。
    """
    from supabase import create_client

    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()

    # Streamlit Cloud 可能没有 .env，需要从 st.secrets 读取
    if not url or not key:
        try:
            import streamlit as st

            url = url or str(st.secrets.get("SUPABASE_URL", "") or "").strip()
            key = key or str(st.secrets.get("SUPABASE_KEY", "") or "").strip()
        except Exception:
            pass

    if not url or not key:
        raise ValueError(
            "Missing Supabase credentials. "
            "Please set SUPABASE_URL and SUPABASE_KEY in .env or st.secrets."
        )
    return create_client(url, key)


def is_admin_configured() -> bool:
    """检查 admin 写库环境变量是否已配置。"""
    url = os.getenv("SUPABASE_URL", "").strip()
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        or os.getenv("SUPABASE_KEY", "").strip()
    )
    return bool(url and key)
