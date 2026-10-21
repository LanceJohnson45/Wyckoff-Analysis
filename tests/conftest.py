# -*- coding: utf-8 -*-
"""
共享 pytest fixtures — mock 外部服务，让 core/ 单测可离线运行。
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ── 屏蔽 Streamlit ────────────────────────────────────────────────
# core/ 层不应依赖 streamlit，但某些 integrations 模块在顶层 import 了它。
# 通过环境变量让 supabase_client 等跳过初始化。


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """防止测试意外发出真实网络请求（可按需在单个测试中移除）。"""
    import socket

    def _guard(*args, **kwargs):
        raise RuntimeError("Tests must not make real network calls")

    monkeypatch.setattr(socket, "create_connection", _guard)


@pytest.fixture()
def mock_supabase():
    """返回一个 MagicMock supabase Client，用于 integrations 层测试。"""
    client = MagicMock()
    client.table.return_value.select.return_value.execute.return_value.data = []
    return client


@pytest.fixture
def inline_prewarm(monkeypatch):
    """行为测试在本进程使用隔离I/O；真实进程终止由专门用例验证。"""
    from concurrent.futures import Future
    from scripts import funnel_prewarm as prewarm

    def execute(function, jobs, **options):
        for symbol, args, kwargs in jobs:
            future = Future()
            try:
                future.set_result(function(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            yield symbol, future

    monkeypatch.setattr(prewarm, "bounded_call", lambda function, *args, timeout, **kwargs: function(*args, **kwargs))
    monkeypatch.setattr(prewarm, "iter_bounded_jobs", execute)
