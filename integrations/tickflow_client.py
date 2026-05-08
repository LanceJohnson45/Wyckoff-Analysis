# -*- coding: utf-8 -*-
"""
TickFlow 行情客户端（带重试与超时控制）。
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests

TICKFLOW_BASE_URL = os.getenv("TICKFLOW_BASE_URL", "https://api.tickflow.org").strip().rstrip("/")
TICKFLOW_TIMEOUT_SECONDS = max(int(os.getenv("TICKFLOW_TIMEOUT_SECONDS", "12")), 3)
TICKFLOW_MAX_RETRIES = max(int(os.getenv("TICKFLOW_MAX_RETRIES", "3")), 1)
TICKFLOW_RETRY_BACKOFF_SECONDS = max(float(os.getenv("TICKFLOW_RETRY_BACKOFF_SECONDS", "1.5")), 0.1)

_PERIOD_SET = {"1m", "5m", "10m", "15m", "30m", "60m", "1d", "1w", "1M", "1Q", "1Y"}
_CN_TZ = "Asia/Shanghai"
_ADJUST_SET = {"none", "forward", "backward"}


def normalize_cn_symbol(raw: str) -> str:
    """将 A 股 6 位代码标准化为 TickFlow 接口格式：XXXXXX.SH / XXXXXX.SZ。"""
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    if "." in s and len(s.split(".", 1)[0]) == 6:
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) != 6:
        return s
    if digits.startswith(("0", "3", "2")):
        return f"{digits}.SZ"
    return f"{digits}.SH"


def parse_ohlcv_payload(payload: dict[str, Any]) -> pd.DataFrame:
    """将 TickFlow K线 payload 转为标准 DataFrame。"""
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return pd.DataFrame()
    ts = data.get("timestamp")
    if not isinstance(ts, list) or not ts:
        return pd.DataFrame()

    def _arr(name: str) -> list[float]:
        v = data.get(name)
        if isinstance(v, list):
            return v
        return [None] * len(ts)

    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": _arr("open"),
            "high": _arr("high"),
            "low": _arr("low"),
            "close": _arr("close"),
            "prev_close": _arr("prev_close"),
            "volume": _arr("volume"),
            "amount": _arr("amount"),
        }
    )
    for col in ("open", "high", "low", "close", "prev_close", "volume", "amount"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    dt = pd.to_datetime(df["timestamp"], unit="ms", utc=True, errors="coerce")
    df["datetime"] = dt.dt.tz_convert(_CN_TZ)
    df["date"] = df["datetime"].dt.date.astype(str)
    df = df.dropna(subset=["datetime", "close"]).sort_values("datetime").reset_index(drop=True)
    return df


@dataclass
class TickFlowClient:
    api_key: str
    base_url: str = TICKFLOW_BASE_URL
    timeout_seconds: int = TICKFLOW_TIMEOUT_SECONDS
    max_retries: int = TICKFLOW_MAX_RETRIES
    retry_backoff_seconds: float = TICKFLOW_RETRY_BACKOFF_SECONDS

    def __post_init__(self) -> None:
        self.api_key = str(self.api_key or "").strip()
        self.base_url = str(self.base_url or TICKFLOW_BASE_URL).strip().rstrip("/")
        self.timeout_seconds = max(int(self.timeout_seconds), 3)
        self.max_retries = max(int(self.max_retries), 1)
        self.retry_backoff_seconds = max(float(self.retry_backoff_seconds), 0.1)
        if not self.api_key:
            raise ValueError("TICKFLOW_API_KEY 未配置")

    def _request(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        last_err: Exception | None = None
        url = f"{self.base_url}{path}"
        headers = {"x-api-key": self.api_key}
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                if resp.status_code == 200:
                    return resp.json()
                # Cloudflare 1010 / 临时网关错误等，走重试
                body = (resp.text or "").strip()
                if attempt < self.max_retries and (
                    resp.status_code >= 500 or "error code: 1010" in body.lower()
                ):
                    time.sleep(self.retry_backoff_seconds * attempt)
                    continue
                raise RuntimeError(f"TickFlow HTTP {resp.status_code}: {body[:200]}")
            except Exception as e:  # requests.Timeout / requests.ConnectionError / RuntimeError
                last_err = e
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError(f"TickFlow 请求失败: {last_err}")

    def get_klines(
        self,
        symbol: str,
        *,
        period: str = "1d",
        count: int = 300,
        intraday: bool = False,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        adjust: str | None = None,
    ) -> pd.DataFrame:
        p = str(period or "1d").strip()
        if p not in _PERIOD_SET:
            raise ValueError(f"不支持的 period: {p}")
        endpoint = "/v1/klines/intraday" if intraday else "/v1/klines"
        params: dict[str, Any] = {
            "symbol": normalize_cn_symbol(symbol),
            "period": p,
            "count": max(int(count), 1),
        }
        if start_time_ms is not None:
            params["start_time"] = int(start_time_ms)
        if end_time_ms is not None:
            params["end_time"] = int(end_time_ms)
        if not intraday and adjust is not None:
            adj = str(adjust or "").strip().lower()
            if adj not in _ADJUST_SET:
                raise ValueError(f"不支持的 adjust: {adjust}")
            params["adjust"] = adj
        payload = self._request(
            endpoint,
            params=params,
        )
        return parse_ohlcv_payload(payload)

    def get_intraday(self, symbol: str, *, period: str = "1m", count: int = 500) -> pd.DataFrame:
        return self.get_klines(symbol, period=period, count=count, intraday=True)

    def get_intraday_batch(
        self,
        symbols: list[str],
        *,
        period: str = "1m",
        count: int = 500,
    ) -> dict[str, pd.DataFrame]:
        """
        批量查询当日分时 K 线。
        接口: GET /v1/klines/intraday/batch
        返回: { "000001.SZ": DataFrame, ... }
        """
        p = str(period or "1m").strip()
        if p not in _PERIOD_SET:
            raise ValueError(f"不支持的 period: {p}")
        clean = [normalize_cn_symbol(x) for x in symbols if str(x or "").strip()]
        clean = sorted(set(x for x in clean if x))
        if not clean:
            return {}

        payload = self._request(
            "/v1/klines/intraday/batch",
            params={
                "symbols": ",".join(clean),
                "period": p,
                "count": max(int(count), 1),
            },
        )
        raw = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(raw, dict):
            return {}

        out: dict[str, pd.DataFrame] = {}
        for sym, kline_payload in raw.items():
            symbol = normalize_cn_symbol(str(sym or "").strip())
            if not symbol or not isinstance(kline_payload, dict):
                continue
            df = parse_ohlcv_payload({"data": kline_payload})
            out[symbol] = df
        return out

    def get_quotes(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        clean = [normalize_cn_symbol(x) for x in symbols if str(x or "").strip()]
        clean = [x for x in clean if x]
        if not clean:
            return {}
        payload = self._request(
            "/v1/quotes",
            params={"symbols": ",".join(sorted(set(clean)))},
        )
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            return {}
        out: dict[str, dict[str, Any]] = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            sym = normalize_cn_symbol(str(row.get("symbol", "")).strip())
            if not sym:
                continue
            out[sym] = row
        return out
