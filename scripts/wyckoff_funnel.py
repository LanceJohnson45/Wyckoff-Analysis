# -*- coding: utf-8 -*-
# Copyright (c) 2024 youngcan. All Rights Reserved.
# 本代码仅供个人学习研究使用，未经授权不得用于商业目的。
# 商业授权请联系作者支付授权费用。

"""
Wyckoff Funnel 定时任务：4 层漏斗筛选 → 飞书发送

Layer 1: 剥离垃圾 → Layer 2: 强弱甄别 → Layer 3: 板块共振 → Layer 4: 威科夫狙击
"""

from __future__ import annotations
import os
import socket
import sys
import time
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError as FuturesTimeoutError,
    as_completed,
)
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from integrations.fetch_a_share_csv import (
    _resolve_trading_window,
    get_stocks_by_board,
    _normalize_symbols,
)
from core.wyckoff_engine import (
    FunnelConfig,
    layer1_filter,
    layer2_strength,
    layer3_sector_resonance,
    layer4_triggers,
    normalize_hist_from_fetch,
)
from integrations.data_source import (
    fetch_index_hist,
    fetch_sector_map,
    fetch_market_cap_map,
    fetch_stock_spot_snapshot,
)
from utils.feishu import send_feishu_notification

TRIGGER_LABELS = {
    "spring": "Spring（终极震仓）",
    "lps": "LPS（缩量回踩）",
    "evr": "Effort vs Result（放量不跌）",
}
TRADING_DAYS = 500
MAX_RETRIES = int(os.getenv("FUNNEL_FETCH_RETRIES", "2"))
RETRY_BASE_DELAY = float(os.getenv("FUNNEL_RETRY_BASE_DELAY", "1.0"))
SOCKET_TIMEOUT = int(os.getenv("FUNNEL_SOCKET_TIMEOUT", "20"))
FETCH_TIMEOUT = int(os.getenv("FUNNEL_FETCH_TIMEOUT", "45"))
BATCH_TIMEOUT = int(os.getenv("FUNNEL_BATCH_TIMEOUT", "420"))
BATCH_SIZE = int(os.getenv("FUNNEL_BATCH_SIZE", "200"))
BATCH_SLEEP = float(os.getenv("FUNNEL_BATCH_SLEEP", "2"))
MAX_WORKERS = int(os.getenv("FUNNEL_MAX_WORKERS", "8"))
EXECUTOR_MODE = os.getenv("FUNNEL_EXECUTOR_MODE", "thread").strip().lower()
if EXECUTOR_MODE not in {"thread", "process"}:
    EXECUTOR_MODE = "thread"
CN_TZ = ZoneInfo("Asia/Shanghai")
MARKET_CLOSE_HOUR = int(os.getenv("MARKET_CLOSE_HOUR", "15"))
MARKET_DATA_READY_HOUR = int(
    os.getenv(
        "MARKET_DATA_READY_HOUR",
        str(max(MARKET_CLOSE_HOUR, 20)),
    )
)
ENFORCE_TARGET_TRADE_DATE = os.getenv(
    "ENFORCE_TARGET_TRADE_DATE", "1"
).strip().lower() in {"1", "true", "yes", "on"}
FUNNEL_ENABLE_SPOT_PATCH = os.getenv("FUNNEL_ENABLE_SPOT_PATCH", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FUNNEL_SPOT_PATCH_RETRIES = int(os.getenv("FUNNEL_SPOT_PATCH_RETRIES", "2"))
FUNNEL_SPOT_PATCH_SLEEP = float(os.getenv("FUNNEL_SPOT_PATCH_SLEEP", "0.2"))
BREADTH_MA_WINDOW = int(os.getenv("FUNNEL_BREADTH_MA_WINDOW", "20"))
BREADTH_RISK_OFF_THRESHOLD = float(os.getenv("FUNNEL_BREADTH_RISK_OFF_PCT", "20.0"))
BREADTH_RISK_ON_THRESHOLD = float(os.getenv("FUNNEL_BREADTH_RISK_ON_PCT", "60.0"))
BREADTH_RISK_ON_MIN_DELTA = float(os.getenv("FUNNEL_BREADTH_RISK_ON_DELTA", "0.0"))


def _normalize_hist(df: pd.DataFrame) -> pd.DataFrame:
    return normalize_hist_from_fetch(df)


def _fetch_hist(symbol: str, window, adjust: str) -> pd.DataFrame:
    from integrations.fetch_a_share_csv import _fetch_hist as _fh

    df = _fh(symbol=symbol, window=window, adjust=adjust)
    return _normalize_hist(df)


def _stock_name_map() -> dict[str, str]:
    try:
        from integrations.fetch_a_share_csv import get_all_stocks

        items = get_all_stocks()
        return {
            x.get("code", ""): x.get("name", "") for x in items if isinstance(x, dict)
        }
    except Exception:
        return {}


def _fetch_one_with_retry(
    sym: str, window, max_retries: int = MAX_RETRIES
) -> tuple[str, pd.DataFrame | None]:
    """在子进程中执行，单票硬超时 + 重试，避免个别数据源卡死拖慢整批。"""
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    for attempt in range(max_retries):
        try:
            df = _run_with_timeout(sym, window, FETCH_TIMEOUT)
            return (sym, df)
        except Exception:
            if attempt < max_retries - 1:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                time.sleep(delay)
    return (sym, None)


def _fetch_one_with_retry_thread(
    sym: str, window, max_retries: int = MAX_RETRIES
) -> tuple[str, pd.DataFrame | None]:
    """
    线程模式：避免 signal，依赖数据源请求超时与重试。
    """
    for attempt in range(max_retries):
        try:
            df = _fetch_hist(sym, window, "qfq")
            return (sym, df)
        except Exception:
            if attempt < max_retries - 1:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                time.sleep(delay)
    return (sym, None)


def _run_with_timeout(sym: str, window, timeout_s: int) -> pd.DataFrame:
    """
    在 worker 进程内给单票请求加硬超时（Unix 下用 SIGALRM）。
    若平台不支持 SIGALRM（例如 Windows），则退化为直接调用。
    注意：在 Windows / 不支持 SIGALRM 的运行环境里，本函数不会提供单票硬超时，
    仅依赖外层批次超时(BATCH_TIMEOUT)做兜底。
    """
    if timeout_s <= 0:
        return _fetch_hist(sym, window, "qfq")

    try:
        import signal
    except Exception:
        return _fetch_hist(sym, window, "qfq")

    if not hasattr(signal, "SIGALRM"):
        return _fetch_hist(sym, window, "qfq")

    def _alarm_handler(signum, frame):  # pragma: no cover - signal handler
        raise TimeoutError(f"single fetch timeout>{timeout_s}s")

    old = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(timeout_s)
    try:
        return _fetch_hist(sym, window, "qfq")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def _job_end_calendar_day() -> date:
    """
    定时任务统一口径：
    - 北京时间达到 MARKET_DATA_READY_HOUR（默认 >=20:00）走 T+0（当天）
    - 否则走 T-1（上一自然日），避免 15:00~18:00 数据源时差导致截面错位
    """
    now = datetime.now(CN_TZ)
    if now.hour >= MARKET_DATA_READY_HOUR:
        return now.date()
    return (now - timedelta(days=1)).date()


def _latest_trade_date_from_hist(df: pd.DataFrame) -> date | None:
    if df is None or df.empty or "date" not in df.columns:
        return None
    s = pd.to_datetime(df["date"], errors="coerce").dropna()
    if s.empty:
        return None
    return s.iloc[-1].date()


def _append_spot_bar_if_needed(
    symbol: str,
    df: pd.DataFrame,
    target_trade_date: date,
) -> tuple[pd.DataFrame, bool]:
    if not FUNNEL_ENABLE_SPOT_PATCH or df is None or df.empty:
        return (df, False)
    latest_trade_date = _latest_trade_date_from_hist(df)
    if latest_trade_date is None or latest_trade_date >= target_trade_date:
        return (df, False)
    if target_trade_date != datetime.now(CN_TZ).date():
        return (df, False)

    df_s = df.sort_values("date").reset_index(drop=True)
    last_close_series = pd.to_numeric(df_s.get("close"), errors="coerce").dropna()
    prev_close = float(last_close_series.iloc[-1]) if not last_close_series.empty else None

    for attempt in range(max(FUNNEL_SPOT_PATCH_RETRIES, 1)):
        snap = fetch_stock_spot_snapshot(symbol, force_refresh=attempt > 0)
        close_v = None if not snap else snap.get("close")
        if close_v is None or float(close_v) <= 0:
            if attempt < max(FUNNEL_SPOT_PATCH_RETRIES, 1) - 1:
                time.sleep(max(FUNNEL_SPOT_PATCH_SLEEP, 0.0))
            continue

        close_f = float(close_v)
        open_f = float(snap.get("open")) if snap and snap.get("open") is not None else close_f
        high_raw = float(snap.get("high")) if snap and snap.get("high") is not None else close_f
        low_raw = float(snap.get("low")) if snap and snap.get("low") is not None else close_f
        high_f = max(high_raw, open_f, close_f)
        low_f = min(low_raw, open_f, close_f)
        volume_f = float(snap.get("volume")) if snap and snap.get("volume") is not None else 0.0
        amount_f = float(snap.get("amount")) if snap and snap.get("amount") is not None else 0.0
        pct_f = float(snap.get("pct_chg")) if snap and snap.get("pct_chg") is not None else None
        if pct_f is None and prev_close and prev_close > 0:
            pct_f = (close_f - prev_close) / prev_close * 100.0

        new_row = {
            "date": target_trade_date.isoformat(),
            "open": open_f,
            "high": high_f,
            "low": low_f,
            "close": close_f,
            "volume": volume_f,
            "amount": amount_f,
            "pct_chg": pct_f if pct_f is not None else 0.0,
        }
        patched = pd.concat([df_s, pd.DataFrame([new_row])], ignore_index=True)
        patched = patched.sort_values("date").reset_index(drop=True)
        return (patched, True)
    return (df, False)


def _terminate_executor_processes(ex: ProcessPoolExecutor, batch_no: int) -> None:
    """
    批次超时时，主动终止仍存活的子进程，避免 wait=False 仅“逻辑结束”但进程继续跑。
    这里使用私有属性是出于稳定性权衡：该任务更看重硬超时止损。
    """
    procs = getattr(ex, "_processes", {}) or {}
    killed = 0
    for proc in procs.values():
        try:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=1)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=1)
                killed += 1
        except Exception as e:
            print(f"[funnel] 批次#{batch_no} 终止子进程异常: {e}")
    if killed:
        print(f"[funnel] 批次#{batch_no} 已强制终止 {killed} 个卡住子进程")


def _analyze_benchmark_and_tune_cfg(
    bench_df: pd.DataFrame | None,
    cfg: FunnelConfig,
    breadth: dict | None = None,
) -> dict:
    """
    Step 0：大盘总闸
    - 输出宏观水温（RISK_ON / NEUTRAL / RISK_OFF）
    - 在 RISK_OFF 时动态收紧个股过滤阈值
    """
    context = {
        "regime": "UNKNOWN",
        "close": None,
        "ma50": None,
        "ma200": None,
        "ma50_slope_5d": None,
        "recent3_pct": [],
        "recent3_cum_pct": None,
        "tuned": {
            "min_avg_amount_wan": cfg.min_avg_amount_wan,
            "rs_min_long": cfg.rs_min_long,
            "rs_min_short": cfg.rs_min_short,
            "rps_fast_min": cfg.rps_fast_min,
            "rps_slow_min": cfg.rps_slow_min,
        },
        "breadth": {
            "ratio_pct": None,
            "prev_ratio_pct": None,
            "delta_pct": None,
            "sample_size": 0,
            "ma_window": BREADTH_MA_WINDOW,
        },
    }
    close = None
    ma50 = None
    ma200 = None
    ma50_slope_5d = None
    recent3_list: list[float] = []
    recent3_cum = None

    if bench_df is not None and not bench_df.empty:
        b = bench_df.sort_values("date").copy()
        b["close"] = pd.to_numeric(b["close"], errors="coerce")
        b["pct_chg"] = pd.to_numeric(b["pct_chg"], errors="coerce")
        if len(b) >= 60:
            close = float(b["close"].iloc[-1])
            ma50 = float(b["close"].rolling(50).mean().iloc[-1])
            ma200 = float(b["close"].rolling(200).mean().iloc[-1])
            ma50_prev = b["close"].rolling(50).mean().shift(5).iloc[-1]
            ma50_slope_5d = None if pd.isna(ma50_prev) else float(ma50 - ma50_prev)
            recent3 = b["pct_chg"].dropna().tail(3)
            recent3_list = [float(x) for x in recent3.tolist()]
            if not recent3.empty:
                recent3_cum = float(((recent3 / 100.0 + 1.0).prod() - 1.0) * 100.0)

    regime = "NEUTRAL"
    if (
        ma200 is not None
        and ma50 is not None
        and ma50_slope_5d is not None
        and recent3_cum is not None
        and close is not None
    ):
        risk_off = (
            (close < ma200)
            and (ma50 < ma200)
            and (ma50_slope_5d < 0)
            and (recent3_cum <= -2.0)
        )
        risk_on = (
            (close > ma50 > ma200) and (ma50_slope_5d > 0) and (recent3_cum >= 0.0)
        )
        if risk_off:
            regime = "RISK_OFF"
        elif risk_on:
            regime = "RISK_ON"

    breadth_ratio = None
    breadth_prev = None
    breadth_delta = None
    breadth_sample = 0
    if breadth:
        breadth_ratio = breadth.get("ratio_pct")
        breadth_prev = breadth.get("prev_ratio_pct")
        breadth_delta = breadth.get("delta_pct")
        breadth_sample = int(breadth.get("sample_size") or 0)
    if breadth_ratio is not None:
        if float(breadth_ratio) <= BREADTH_RISK_OFF_THRESHOLD:
            regime = "RISK_OFF"
        elif float(breadth_ratio) >= BREADTH_RISK_ON_THRESHOLD:
            if breadth_delta is None or float(breadth_delta) >= BREADTH_RISK_ON_MIN_DELTA:
                regime = "RISK_ON"

    # 动态调参：风险越冷，过滤越严
    if regime == "RISK_OFF":
        cfg.min_avg_amount_wan = max(cfg.min_avg_amount_wan, 10000.0)
        cfg.rs_min_long = max(cfg.rs_min_long, 2.0)
        cfg.rs_min_short = max(cfg.rs_min_short, 0.5)
        cfg.rps_fast_min = max(cfg.rps_fast_min, 90.0)
        cfg.rps_slow_min = max(cfg.rps_slow_min, 85.0)
        if recent3_cum is not None and recent3_cum <= -4.0:
            cfg.min_avg_amount_wan = max(cfg.min_avg_amount_wan, 15000.0)
            cfg.rs_min_long = max(cfg.rs_min_long, 4.0)
            cfg.rs_min_short = max(cfg.rs_min_short, 1.0)
    elif regime == "RISK_ON":
        cfg.rs_min_long = max(cfg.rs_min_long, 0.0)
        cfg.rs_min_short = max(cfg.rs_min_short, 0.0)
        cfg.rps_fast_min = min(cfg.rps_fast_min, 80.0)
        cfg.rps_slow_min = min(cfg.rps_slow_min, 75.0)

    context.update(
        {
            "regime": regime,
            "close": close,
            "ma50": ma50,
            "ma200": ma200,
            "ma50_slope_5d": ma50_slope_5d,
            "recent3_pct": recent3_list,
            "recent3_cum_pct": recent3_cum,
            "tuned": {
                "min_avg_amount_wan": cfg.min_avg_amount_wan,
                "rs_min_long": cfg.rs_min_long,
                "rs_min_short": cfg.rs_min_short,
                "rps_fast_min": cfg.rps_fast_min,
                "rps_slow_min": cfg.rps_slow_min,
            },
            "breadth": {
                "ratio_pct": breadth_ratio,
                "prev_ratio_pct": breadth_prev,
                "delta_pct": breadth_delta,
                "sample_size": breadth_sample,
                "ma_window": BREADTH_MA_WINDOW,
            },
        }
    )
    return context


def _calc_market_breadth(
    df_map: dict[str, pd.DataFrame],
    ma_window: int = BREADTH_MA_WINDOW,
) -> dict:
    """
    全市场广度：
    breadth = 收盘价站上 MA20 的股票占比（%）。
    额外给出前一日广度与日变化，用于识别扩散/收敛。
    """
    valid_now = 0
    valid_prev = 0
    above_now = 0
    above_prev = 0
    w = max(int(ma_window), 2)
    for df in df_map.values():
        if df is None or df.empty:
            continue
        s = df.sort_values("date")
        close = pd.to_numeric(s.get("close"), errors="coerce")
        if close.dropna().shape[0] < (w + 1):
            continue
        ma = close.rolling(w).mean()

        c_now = close.iloc[-1]
        ma_now = ma.iloc[-1]
        if pd.notna(c_now) and pd.notna(ma_now):
            valid_now += 1
            if float(c_now) >= float(ma_now):
                above_now += 1

        c_prev = close.iloc[-2]
        ma_prev = ma.iloc[-2]
        if pd.notna(c_prev) and pd.notna(ma_prev):
            valid_prev += 1
            if float(c_prev) >= float(ma_prev):
                above_prev += 1

    ratio_now = (above_now / valid_now * 100.0) if valid_now > 0 else None
    ratio_prev = (above_prev / valid_prev * 100.0) if valid_prev > 0 else None
    delta = None
    if ratio_now is not None and ratio_prev is not None:
        delta = ratio_now - ratio_prev
    return {
        "ratio_pct": ratio_now,
        "prev_ratio_pct": ratio_prev,
        "delta_pct": delta,
        "sample_size": valid_now,
    }


def run_funnel_job() -> tuple[dict[str, list[tuple[str, float]]], dict]:
    """执行 Wyckoff Funnel，返回 (triggers, metrics)。"""
    cfg = FunnelConfig(trading_days=TRADING_DAYS)
    window = _resolve_trading_window(
        end_calendar_day=_job_end_calendar_day(),
        trading_days=TRADING_DAYS,
    )
    start_s = window.start_trade_date.strftime("%Y%m%d")
    end_s = window.end_trade_date.strftime("%Y%m%d")

    # 股票池：主板 + 创业板 - ST（预过滤，减少无效拉取）
    main_items = get_stocks_by_board("main")
    chinext_items = get_stocks_by_board("chinext")
    merged_code_to_name: dict[str, str] = {}
    for item in main_items + chinext_items:
        code = str(item.get("code", "")).strip()
        if not code:
            continue
        if code not in merged_code_to_name:
            merged_code_to_name[code] = str(item.get("name", "")).strip()
    merged_symbols = _normalize_symbols(list(merged_code_to_name.keys()))
    st_symbols = [
        sym
        for sym in merged_symbols
        if "ST" in merged_code_to_name.get(sym, "").upper()
    ]
    st_set = set(st_symbols)
    all_symbols = [sym for sym in merged_symbols if sym not in st_set]
    total_batches = (
        (len(all_symbols) + BATCH_SIZE - 1) // BATCH_SIZE if all_symbols else 0
    )
    print(
        "[funnel] 股票池统计: "
        f"main={len(main_items)}, chinext={len(chinext_items)}, "
        f"merged={len(merged_symbols)}, st_excluded={len(st_symbols)}, "
        f"final={len(all_symbols)}, batches={total_batches} (batch_size={BATCH_SIZE})"
    )

    # 批量元数据
    print(f"[funnel] 加载行业映射...")
    sector_map = fetch_sector_map()
    print(f"[funnel] 加载市值数据...")
    market_cap_map = fetch_market_cap_map()
    if not market_cap_map:
        print(
            "[funnel] ⚠️ 市值数据为空（TUSHARE_TOKEN 可能缺失/失效），Layer1 将跳过市值过滤"
        )
    print(f"[funnel] 加载股票名称...")
    name_map = _stock_name_map()

    # 大盘基准
    bench_df = None
    try:
        bench_df = fetch_index_hist("000001", start_s, end_s)
        print(f"[funnel] 大盘基准加载成功")
    except Exception as e:
        print(f"[funnel] 大盘基准加载失败: {e}")

    # 并发拉取日线（只负责取数，不负责计算）
    all_df_map: dict[str, pd.DataFrame] = {}
    fetch_ok = 0
    fetch_fail = 0
    fetch_date_mismatch = 0
    fetch_spot_patched = 0

    print(
        f"[funnel] 开始拉取 {len(all_symbols)} 只股票日线 "
        f"(executor={EXECUTOR_MODE}, batch_size={BATCH_SIZE}, max_workers={MAX_WORKERS}, batch_timeout={BATCH_TIMEOUT}s, "
        f"fetch_timeout={FETCH_TIMEOUT}s, retries={MAX_RETRIES})"
    )
    _baostock_orig = os.environ.get("DATA_SOURCE_DISABLE_BAOSTOCK")
    if EXECUTOR_MODE == "process":
        if not _baostock_orig:
            os.environ["DATA_SOURCE_DISABLE_BAOSTOCK"] = "1"
            print(
                "[funnel] 检测到 process 并发，已自动设置 DATA_SOURCE_DISABLE_BAOSTOCK=1 "
                "以规避并发 login 风险（任务结束后自动还原）。"
            )
    total_fetch_started = time.monotonic()
    for i in range(0, len(all_symbols), BATCH_SIZE):
        batch_no = i // BATCH_SIZE + 1
        batch = all_symbols[i : i + BATCH_SIZE]
        batch_ok = 0
        batch_fail = 0
        batch_started = time.monotonic()
        print(f"[funnel] 批次#{batch_no}/{total_batches} 启动，股票数={len(batch)}")

        use_process = EXECUTOR_MODE == "process"
        ex = (
            ProcessPoolExecutor(max_workers=MAX_WORKERS)
            if use_process
            else ThreadPoolExecutor(max_workers=MAX_WORKERS)
        )
        fetch_fn = (
            _fetch_one_with_retry if use_process else _fetch_one_with_retry_thread
        )
        futures = {ex.submit(fetch_fn, s, window): s for s in batch}
        try:
            for f in as_completed(futures, timeout=BATCH_TIMEOUT):
                sym = futures[f]
                try:
                    _, df = f.result()
                except Exception as e:
                    print(f"[funnel] 批次#{batch_no} 拉取失败 {sym}: {e}")
                    batch_fail += 1
                    fetch_fail += 1
                    continue
                if df is not None:
                    if ENFORCE_TARGET_TRADE_DATE:
                        latest_trade_date = _latest_trade_date_from_hist(df)
                        if latest_trade_date != window.end_trade_date:
                            df, patched = _append_spot_bar_if_needed(
                                sym,
                                df,
                                window.end_trade_date,
                            )
                            if patched:
                                latest_trade_date = _latest_trade_date_from_hist(df)
                                fetch_spot_patched += 1
                            batch_fail += 1
                            if latest_trade_date != window.end_trade_date:
                                fetch_fail += 1
                                fetch_date_mismatch += 1
                                print(
                                    f"[funnel] 批次#{batch_no} 跳过 {sym}: "
                                    f"latest_trade_date={latest_trade_date}, "
                                    f"target_trade_date={window.end_trade_date}"
                                )
                                continue
                            batch_fail -= 1
                    batch_ok += 1
                    fetch_ok += 1
                    all_df_map[sym] = df
                else:
                    batch_fail += 1
                    fetch_fail += 1
        except FuturesTimeoutError:
            pending_symbols = [futures[ft] for ft in futures if not ft.done()]
            timed_out = len(pending_symbols)
            batch_fail += timed_out
            fetch_fail += timed_out
            print(
                f"[funnel] 批次#{batch_no} 超时({BATCH_TIMEOUT}s)，"
                f"已完成={batch_ok + batch_fail - timed_out}/{len(batch)}，"
                f"未完成={timed_out}，将跳过剩余任务"
            )
            if pending_symbols:
                preview = ", ".join(pending_symbols[:10])
                suffix = "..." if len(pending_symbols) > 10 else ""
                print(f"[funnel] 批次#{batch_no} 超时股票: {preview}{suffix}")
            if use_process:
                _terminate_executor_processes(ex, batch_no)
        finally:
            for ft in futures:
                ft.cancel()
            ex.shutdown(wait=False, cancel_futures=True)

        batch_elapsed = time.monotonic() - batch_started
        batch_qps = (batch_ok / batch_elapsed) if batch_elapsed > 0 else 0.0
        print(
            f"[funnel] 批次#{batch_no} 完成: 成功={batch_ok}, 失败={batch_fail}, "
            f"耗时={batch_elapsed:.1f}s, qps={batch_qps:.2f}, 累计成功={fetch_ok}, 累计失败={fetch_fail}"
        )
        if i + BATCH_SIZE < len(all_symbols) and BATCH_SLEEP > 0:
            time.sleep(BATCH_SLEEP)

    total_fetch_elapsed = time.monotonic() - total_fetch_started
    overall_qps = (fetch_ok / total_fetch_elapsed) if total_fetch_elapsed > 0 else 0.0
    print(
        f"[funnel] 日线拉取完成: 成功={fetch_ok}, 失败={fetch_fail}, "
        f"总耗时={total_fetch_elapsed:.1f}s, 平均qps={overall_qps:.2f}"
    )
    if ENFORCE_TARGET_TRADE_DATE:
        print(
            f"[funnel] 交易日对齐检查: mismatch={fetch_date_mismatch}, "
            f"spot_patched={fetch_spot_patched}, target_trade_date={window.end_trade_date}"
        )

    # Step 0: 大盘总闸 + 全市场广度 + 动态阈值
    breadth_context = _calc_market_breadth(all_df_map, BREADTH_MA_WINDOW)
    benchmark_context = _analyze_benchmark_and_tune_cfg(
        bench_df,
        cfg,
        breadth=breadth_context,
    )
    print(
        "[funnel] 大盘总闸: "
        f"regime={benchmark_context['regime']}, "
        f"close={benchmark_context['close']}, ma50={benchmark_context['ma50']}, ma200={benchmark_context['ma200']}, "
        f"ma50_slope_5d={benchmark_context['ma50_slope_5d']}, recent3={benchmark_context['recent3_pct']}, "
        f"recent3_cum={benchmark_context['recent3_cum_pct']}, "
        f"breadth={benchmark_context.get('breadth')}, "
        f"tuned={benchmark_context['tuned']}"
    )

    # 统一漏斗计算：L1 -> L2 -> L3 -> L4
    print(f"[funnel] 开始执行全量漏斗筛选...")

    # Layer 1
    l1_input = list(all_df_map.keys())
    l1_passed = layer1_filter(l1_input, name_map, market_cap_map, all_df_map, cfg)

    # Layer 2
    l2_passed = layer2_strength(l1_passed, all_df_map, bench_df, cfg)

    # Layer 3 (Sector Resonance)
    l3_passed, top_sectors = layer3_sector_resonance(l2_passed, sector_map, cfg)

    # Layer 4 (Wyckoff Triggers)
    # L4 需要 l2_df_map，这里直接用 all_df_map 即可，因为 key 都在里面
    triggers = layer4_triggers(l3_passed, all_df_map, cfg)

    total_hits = sum(len(v) for v in triggers.values())
    metrics = {
        "total_symbols": len(all_symbols),
        "pool_main": len(main_items),
        "pool_chinext": len(chinext_items),
        "pool_merged": len(merged_symbols),
        "pool_st_excluded": len(st_symbols),
        "pool_batches": total_batches,
        "fetch_ok": fetch_ok,
        "fetch_fail": fetch_fail,
        "fetch_date_mismatch": fetch_date_mismatch,
        "fetch_spot_patched": fetch_spot_patched,
        "layer1": len(l1_passed),
        "layer2": len(l2_passed),
        "layer3": len(l3_passed),
        "top_sectors": top_sectors,
        "total_hits": total_hits,
        "by_trigger": {k: len(v) for k, v in triggers.items()},
        "benchmark_context": benchmark_context,
    }
    print(
        f"[funnel] L1={metrics['layer1']}, L2={metrics['layer2']}, "
        f"L3={metrics['layer3']}, 命中={total_hits}, "
        f"Top行业={top_sectors}, 各触发={metrics['by_trigger']}"
    )

    # 还原环境变量，避免污染同进程内的后续任务
    if EXECUTOR_MODE == "process" and not _baostock_orig:
        os.environ.pop("DATA_SOURCE_DISABLE_BAOSTOCK", None)

    return triggers, metrics


def run(webhook_url: str) -> tuple[bool, list[dict], dict]:
    """
    执行 Wyckoff Funnel，漏斗完成后立即发送飞书通知。
    返回 (成功与否, 用于研报的股票信息列表, 大盘上下文)。
    每项为 {"code": str, "name": str, "tag": str}。
    """
    triggers, metrics = run_funnel_job()
    benchmark_context = metrics.get("benchmark_context", {}) or {}
    name_map = _stock_name_map()

    code_to_reasons: dict[str, list[str]] = {}
    code_to_best_score: dict[str, float] = {}
    for key, label in TRIGGER_LABELS.items():
        for code, score in triggers.get(key, []):
            if code not in code_to_reasons:
                code_to_reasons[code] = []
                code_to_best_score[code] = score
            code_to_reasons[code].append(label)
            code_to_best_score[code] = max(code_to_best_score.get(code, 0), score)

    sorted_codes = sorted(
        code_to_reasons.keys(),
        key=lambda c: -code_to_best_score.get(c, 0),
    )
    unique_hit_count = len(sorted_codes)
    selected_for_ai = sorted_codes

    print(
        f"[funnel] 候选分层: 命中事件={metrics['total_hits']}, 命中股票={unique_hit_count}, "
        f"AI输入=全量{len(selected_for_ai)}, "
        f"AI分析={len(selected_for_ai)}"
    )

    bench_line = "未知"
    if benchmark_context:
        breadth = benchmark_context.get("breadth", {}) or {}
        breadth_text = (
            f", breadth={breadth.get('ratio_pct')}% "
            f"(prev={breadth.get('prev_ratio_pct')}%, Δ={breadth.get('delta_pct')}%, n={breadth.get('sample_size')})"
            if breadth
            else ""
        )
        bench_line = (
            f"{benchmark_context.get('regime')} | close={benchmark_context.get('close')} "
            f"ma50={benchmark_context.get('ma50')} ma200={benchmark_context.get('ma200')} "
            f"3d={benchmark_context.get('recent3_pct')} cum3={benchmark_context.get('recent3_cum_pct')}"
            f"{breadth_text}"
        )

    lines = [
        (
        f"**股票池**: 主板{metrics['pool_main']} + 创业板{metrics['pool_chinext']} "
        f"-> 去重{metrics['pool_merged']} -> 去ST{metrics['pool_st_excluded']} "
        f"= {metrics['total_symbols']} (共{metrics['pool_batches']}批)"
        ),
        f"**漏斗概览**: {metrics['total_symbols']}只 → L1:{metrics['layer1']} → L2:{metrics['layer2']} → L3:{metrics['layer3']} → 命中:{metrics['total_hits']}",
        f"**数据对齐**: fetch_ok={metrics['fetch_ok']} / fetch_fail={metrics['fetch_fail']} / "
        f"date_mismatch={metrics.get('fetch_date_mismatch', 0)} / spot_patched={metrics.get('fetch_spot_patched', 0)}",
        f"**大盘水温**: {bench_line}",
        f"**候选分层**: 命中股票{unique_hit_count} -> AI输入全量{len(selected_for_ai)}",
        f"**Top 行业**: {', '.join(metrics['top_sectors']) if metrics['top_sectors'] else '无'}",
        "",
        "**命中列表（按优先级）代码 名称 | 筛选理由 | 分值**",
        "",
    ]
    for code in selected_for_ai:
        name = name_map.get(code, code)
        reasons = "、".join(code_to_reasons[code])
        lines.append(
            f"• {code} {name} | {reasons} | score={code_to_best_score.get(code, 0):.2f}"
        )

    if not selected_for_ai:
        lines.append("无")

    content = "\n".join(lines)
    title = f"🔬 Wyckoff Funnel {date.today().strftime('%Y-%m-%d')}"
    ok = send_feishu_notification(webhook_url, title, content)

    symbols_for_report = [
        {
            "code": c,
            "name": name_map.get(c, c),
            "tag": "、".join(code_to_reasons[c]),
        }
        for c in selected_for_ai
    ]
    return (ok, symbols_for_report, benchmark_context)
