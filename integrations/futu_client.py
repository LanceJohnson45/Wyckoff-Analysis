from __future__ import annotations

import os
import re
import socket
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

import pandas as pd


class FutuUnavailableError(RuntimeError):
    pass


def _prepare_futu_home() -> None:
    home = os.environ.get("HOME", "").strip()
    if not home:
        return

    target = Path(home) / ".com.futunn.FutuOpenD" / "Log"
    try:
        target.mkdir(parents=True, exist_ok=True)
        return
    except PermissionError:
        pass

    redirect_home = Path(os.getenv("TMPDIR") or "/tmp") / "codex-futu-home"
    (redirect_home / ".com.futunn.FutuOpenD" / "Log").mkdir(
        parents=True, exist_ok=True
    )
    os.environ["HOME"] = str(redirect_home)


def _import_futu() -> dict[str, Any]:
    _prepare_futu_home()
    try:
        from futu import (
            AuType,
            KLType,
            Market,
            OpenQuoteContext,
            Plate,
            RET_OK,
            SecurityType,
            TradeDateMarket,
        )
    except (ImportError, PermissionError) as exc:
        raise FutuUnavailableError(
            "futu-api 未安装，请先安装依赖并启动 Futu OpenD。"
        ) from exc

    return {
        "AuType": AuType,
        "KLType": KLType,
        "Market": Market,
        "OpenQuoteContext": OpenQuoteContext,
        "Plate": Plate,
        "RET_OK": RET_OK,
        "SecurityType": SecurityType,
        "TradeDateMarket": TradeDateMarket,
    }


def get_opend_host_port() -> tuple[str, int]:
    host = os.getenv("FUTU_OPEND_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.getenv("FUTU_OPEND_PORT", "11111"))
    return host, port


def is_futu_ready(timeout: float = 1.5) -> bool:
    try:
        _import_futu()
    except FutuUnavailableError:
        return False

    host, port = get_opend_host_port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


@contextmanager
def quote_context() -> Iterator[Any]:
    futu = _import_futu()
    host, port = get_opend_host_port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2.0)
    try:
        sock.connect((host, port))
    except OSError as exc:
        raise FutuUnavailableError(
            f"无法连接 Futu OpenD ({host}:{port})，请先启动 OpenD。"
        ) from exc
    finally:
        sock.close()

    ctx = futu["OpenQuoteContext"](host=host, port=port)
    try:
        yield ctx
    finally:
        try:
            ctx.close()
        except Exception:
            pass


def _normalize_turnover_rate(series: pd.Series) -> pd.Series:
    data = pd.to_numeric(series, errors="coerce")
    finite = data.dropna()
    if finite.empty:
        return data
    if finite.abs().max() <= 1.0:
        return data * 100.0
    return data


def _normalize_pct_change(frame: pd.DataFrame) -> pd.Series:
    for col in ("change_rate", "pct_chg", "pct_change"):
        if col in frame.columns:
            data = pd.to_numeric(frame[col], errors="coerce")
            finite = data.dropna()
            if not finite.empty and finite.abs().max() <= 1.0:
                return data * 100.0
            return data

    close = pd.to_numeric(frame.get("close"), errors="coerce")
    prev_close = pd.to_numeric(frame.get("last_close"), errors="coerce")
    if prev_close is not None and not prev_close.dropna().empty:
        return (close - prev_close) / prev_close.replace(0, pd.NA) * 100.0
    return close.pct_change() * 100.0


def normalize_futu_code(
    symbol: str,
    *,
    market: str = "cn",
    security_type: str = "stock",
) -> str:
    text = str(symbol or "").strip().upper()
    if "." in text:
        prefix, suffix = text.split(".", 1)
        if prefix in {"US", "HK", "SH", "SZ", "SG"}:
            if prefix == "HK" and suffix.isdigit():
                return f"{prefix}.{suffix.zfill(5)}"
            return f"{prefix}.{suffix}"

    market_norm = str(market or "cn").strip().lower()
    digits = "".join(ch for ch in text if ch.isdigit())

    if market_norm == "us":
        if not text:
            raise ValueError("empty US symbol")
        return f"US.{text}"

    if market_norm == "hk":
        code = digits or text
        if code.isdigit():
            code = code.zfill(5)
        return f"HK.{code}"

    if not digits:
        raise ValueError(f"invalid CN symbol: {symbol}")
    if len(digits) < 6:
        digits = digits.zfill(6)

    if security_type == "index":
        prefix = "SH" if digits.startswith(("000", "880", "899")) else "SZ"
        return f"{prefix}.{digits}"

    prefix = "SH" if digits.startswith(("5", "6", "9")) else "SZ"
    return f"{prefix}.{digits}"


def to_local_symbol(code: str) -> str:
    text = str(code or "").strip().upper()
    if "." not in text:
        return text
    prefix, suffix = text.split(".", 1)
    if prefix in {"SH", "SZ", "HK"} and suffix.isdigit():
        if prefix == "HK":
            return suffix.zfill(5)
        return suffix.zfill(6)
    return suffix


@lru_cache(maxsize=1)
def _market_constants() -> dict[str, Any]:
    futu = _import_futu()
    return {
        "cn_trade": getattr(futu["TradeDateMarket"], "CN", None),
        "hk_trade": getattr(futu["TradeDateMarket"], "HK", None),
        "us_trade": getattr(futu["TradeDateMarket"], "US", None),
        "market_sh": getattr(futu["Market"], "SH", None),
        "market_sz": getattr(futu["Market"], "SZ", None),
        "market_hk": getattr(futu["Market"], "HK", None),
        "market_us": getattr(futu["Market"], "US", None),
        "sec_stock": getattr(futu["SecurityType"], "STOCK", None),
        "plate_industry": getattr(futu["Plate"], "INDUSTRY", None),
    }


def fetch_history_kline(
    symbol: str,
    *,
    start: str,
    end: str,
    market: str = "cn",
    adjust: str = "qfq",
    security_type: str = "stock",
) -> pd.DataFrame:
    futu = _import_futu()
    code = normalize_futu_code(symbol, market=market, security_type=security_type)
    au_type = {
        "qfq": futu["AuType"].QFQ,
        "hfq": futu["AuType"].HFQ,
        "": futu["AuType"].NONE,
        "none": futu["AuType"].NONE,
    }.get(str(adjust or "qfq").lower(), futu["AuType"].QFQ)

    start_iso = pd.to_datetime(start).strftime("%Y-%m-%d")
    end_iso = pd.to_datetime(end).strftime("%Y-%m-%d")

    with quote_context() as ctx:
        ret, data, page_req_key = ctx.request_history_kline(
            code,
            start=start_iso,
            end=end_iso,
            ktype=futu["KLType"].K_DAY,
            autype=au_type,
            max_count=1000,
        )
        if ret != futu["RET_OK"]:
            raise RuntimeError(str(data))
        frames = [data] if data is not None and not data.empty else []
        while page_req_key is not None:
            ret, data, page_req_key = ctx.request_history_kline(
                code,
                start=start_iso,
                end=end_iso,
                ktype=futu["KLType"].K_DAY,
                autype=au_type,
                max_count=1000,
                page_req_key=page_req_key,
            )
            if ret != futu["RET_OK"]:
                raise RuntimeError(str(data))
            if data is not None and not data.empty:
                frames.append(data)

    if not frames:
        raise RuntimeError(f"futu history empty: {code}")

    frame = pd.concat(frames, ignore_index=True).drop_duplicates()
    frame = frame.copy()
    frame["日期"] = pd.to_datetime(
        frame.get("time_key"), errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    frame["开盘"] = pd.to_numeric(frame.get("open"), errors="coerce")
    frame["最高"] = pd.to_numeric(frame.get("high"), errors="coerce")
    frame["最低"] = pd.to_numeric(frame.get("low"), errors="coerce")
    frame["收盘"] = pd.to_numeric(frame.get("close"), errors="coerce")
    frame["成交量"] = pd.to_numeric(frame.get("volume"), errors="coerce")
    frame["成交额"] = pd.to_numeric(frame.get("turnover"), errors="coerce")
    frame["涨跌幅"] = _normalize_pct_change(frame)
    frame["换手率"] = _normalize_turnover_rate(frame.get("turnover_rate"))
    prev_close = pd.to_numeric(frame.get("last_close"), errors="coerce")
    frame["振幅"] = (
        (frame["最高"] - frame["最低"]) / prev_close.replace(0, pd.NA) * 100.0
    )
    frame = frame.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)
    return frame[
        [
            "日期",
            "开盘",
            "最高",
            "最低",
            "收盘",
            "成交量",
            "成交额",
            "涨跌幅",
            "换手率",
            "振幅",
        ]
    ].copy()


def fetch_snapshot(symbols: list[str], *, market: str = "cn") -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    futu = _import_futu()
    chunks: list[pd.DataFrame] = []
    codes = [normalize_futu_code(s, market=market) for s in symbols]
    with quote_context() as ctx:
        for i in range(0, len(codes), 400):
            batch = codes[i : i + 400]
            ret, data = ctx.get_market_snapshot(batch)
            if ret != futu["RET_OK"]:
                raise RuntimeError(str(data))
            if data is not None and not data.empty:
                chunks.append(data.copy())
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True).drop_duplicates().reset_index(drop=True)


def fetch_trading_days(
    *,
    market: str = "cn",
    start: str | None = None,
    end: str | None = None,
) -> list[pd.Timestamp]:
    constants = _market_constants()
    trade_market = {
        "cn": constants["cn_trade"],
        "hk": constants["hk_trade"],
        "us": constants["us_trade"],
    }.get(str(market or "cn").strip().lower())
    if trade_market is None:
        raise RuntimeError(f"unsupported futu trading calendar market: {market}")

    kwargs: dict[str, Any] = {"market": trade_market}
    if start:
        kwargs["start"] = pd.to_datetime(start).strftime("%Y-%m-%d")
    if end:
        kwargs["end"] = pd.to_datetime(end).strftime("%Y-%m-%d")

    futu = _import_futu()
    with quote_context() as ctx:
        ret, data = ctx.request_trading_days(**kwargs)
        if ret != futu["RET_OK"]:
            raise RuntimeError(str(data))

    if data is None:
        return []
    if hasattr(data, "iloc"):
        values = []
        for col in data.columns:
            if "date" in str(col).lower():
                values.extend(data[col].tolist())
        if not values and len(data.columns) == 1:
            values = data.iloc[:, 0].tolist()
    else:
        values = list(data)
    out = pd.to_datetime(values, errors="coerce").dropna().tolist()
    return out


def fetch_cn_stock_basic() -> pd.DataFrame:
    constants = _market_constants()
    futu = _import_futu()
    frames: list[pd.DataFrame] = []
    with quote_context() as ctx:
        for market in (constants["market_sh"], constants["market_sz"]):
            if market is None:
                continue
            ret, data = ctx.get_stock_basicinfo(
                market, stock_type=constants["sec_stock"]
            )
            if ret != futu["RET_OK"]:
                raise RuntimeError(str(data))
            if data is not None and not data.empty:
                frames.append(data.copy())
    if not frames:
        return pd.DataFrame(columns=["code", "name"])
    frame = pd.concat(frames, ignore_index=True).drop_duplicates()
    code_col = "code" if "code" in frame.columns else frame.columns[0]
    name_col = "name" if "name" in frame.columns else "stock_name"
    out = frame[[code_col, name_col]].copy()
    out.columns = ["code", "name"]
    out["code"] = out["code"].map(to_local_symbol)
    out["name"] = out["name"].astype(str)
    out = out[out["code"].astype(str).str.fullmatch(r"\d{6}")].reset_index(drop=True)
    return out


def fetch_cn_industry_plates() -> pd.DataFrame:
    constants = _market_constants()
    futu = _import_futu()
    frames: list[pd.DataFrame] = []
    with quote_context() as ctx:
        for market in (constants["market_sh"], constants["market_sz"]):
            if market is None:
                continue
            ret, data = ctx.get_plate_list(market, constants["plate_industry"])
            if ret != futu["RET_OK"]:
                raise RuntimeError(str(data))
            if data is not None and not data.empty:
                frames.append(data.copy())
    if not frames:
        return pd.DataFrame(columns=["code", "name"])
    frame = pd.concat(frames, ignore_index=True).drop_duplicates()
    name_col = "plate_name" if "plate_name" in frame.columns else "stock_name"
    out = frame[["code", name_col]].copy()
    out.columns = ["code", "name"]
    return out.reset_index(drop=True)
