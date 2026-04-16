# -*- coding: utf-8 -*-
"""
Tushare Pro 客户端封装（含分级限流）

从环境变量 TUSHARE_TOKEN 读取 token，提供 pro_api 实例。
所有通过 get_pro() 拿到的 pro 实例调用 API 时都会自动限流。
对 `stock_basic / daily_basic / index_daily` 等低频元数据接口使用更保守的单独限流，
避免触发 50 次/分钟类接口的权限上限。

用法:
    from integrations.tushare_client import get_pro

    pro = get_pro()
    if pro:
        df = pro.daily(ts_code="000001.SZ", start_date="20260101", end_date="20260411")
"""
from __future__ import annotations

import os
import time
import warnings
from threading import Lock


# ── 滑动窗口限流器（进程级单例，线程安全） ──
_RATE_LIMIT = int(os.getenv("TUSHARE_RATE_LIMIT", "45"))  # 普通接口，次/分钟
_LOW_FREQ_RATE_LIMIT = int(
    os.getenv("TUSHARE_LOW_FREQ_RATE_LIMIT", "40")
)  # 元数据接口，次/分钟
_LOW_FREQ_METHODS = {"stock_basic", "daily_basic", "index_daily"}

_call_times: list[float] = []
_call_lock = Lock()
_low_freq_call_times: list[float] = []
_low_freq_call_lock = Lock()


def _wait_for_rate_limit(method_name: str | None = None) -> None:
    """滑动窗口限流：低频元数据接口走独立更严格的限流桶。"""
    method_norm = str(method_name or "").strip().lower()
    is_low_freq = method_norm in _LOW_FREQ_METHODS
    rate_limit = _LOW_FREQ_RATE_LIMIT if is_low_freq else _RATE_LIMIT
    call_times = _low_freq_call_times if is_low_freq else _call_times
    call_lock = _low_freq_call_lock if is_low_freq else _call_lock
    while True:
        with call_lock:
            now = time.monotonic()
            call_times[:] = [t for t in call_times if now - t < 60]
            if len(call_times) < rate_limit:
                call_times.append(now)
                return
            sleep_for = 60 - (now - call_times[0]) + 0.1
        time.sleep(max(0.05, sleep_for))


class _RateLimitedPro:
    """透明代理：拦截所有 pro.xxx() 调用，自动限流。"""

    def __init__(self, pro):
        object.__setattr__(self, "_pro", pro)

    def __getattr__(self, name):
        attr = getattr(object.__getattribute__(self, "_pro"), name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                _wait_for_rate_limit(name)
                return attr(*args, **kwargs)
            wrapper.__name__ = name
            return wrapper
        return attr


def get_pro():
    """返回限流版 Tushare Pro API 实例；若未配置 token 则返回 None。"""
    token = ""
    # 优先尝试从 streamlit session 中获取用户配置
    try:
        import streamlit as st
        token = (st.session_state.get("tushare_token") or "").strip()
    except Exception:
        pass

    # 如果 session 中没有，再尝试从环境变量获取
    if not token:
        token = os.getenv("TUSHARE_TOKEN", "").strip()

    if not token:
        return None
    try:
        warnings.filterwarnings(
            "ignore",
            message=r".*Series\.fillna with 'method' is deprecated.*",
            category=FutureWarning,
            module=r"tushare\.pro\.data_pro",
        )
        import tushare as ts
        ts.set_token(token)
        return _RateLimitedPro(ts.pro_api())
    except ImportError:
        return None
