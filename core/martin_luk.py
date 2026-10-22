"""Martin Luk 日线辅助研究：只识别可复核形态，不推断盘中成交。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

import numpy as np
import pandas as pd

from core.market_scope import require_supported_stock

from core.kline_quality import REQUIRED_COLUMNS, check_kline_quality

MIN_HISTORY = 100
RULE_VERSION = "日线研究 v1"
MARKET_LABELS = {"cn": "A 股", "us": "美股"}
COLUMN_ALIASES = {
    "日期": "date", "开盘": "open", "最高": "high", "最低": "low",
    "收盘": "close", "成交量": "volume", "代码": "symbol",
}


@dataclass(frozen=True)
class LukParameters:
    """工程研究参数，非 Martin 本人披露的统一交易公式。"""

    support_atr: float = 0.75
    extension_atr: float = 3.0
    contraction_ratio: float = 0.70

    def __post_init__(self) -> None:
        if not 0 < self.support_atr < self.extension_atr:
            raise ValueError("支撑距离须为正数，且小于延伸预警距离")
        if not 0 < self.contraction_ratio < 1:
            raise ValueError("区间收缩比例必须在 0 和 1 之间")


@dataclass(frozen=True)
class LukObservation:
    """某一已完成日线时点的观察卡及绘图数据。"""

    symbol: str
    market: str
    data_date: str
    source: str
    tags: tuple[str, ...]
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    trigger: float | None
    invalidation: float | None
    next_step: str
    frame: pd.DataFrame
    rs20_pct: float | None
    parameters: LukParameters

    def summary(self) -> dict[str, str | float | None]:
        """返回列表与 CSV 使用的显式字段。"""
        latest = self.frame.iloc[-1]
        return {
            "代码": self.symbol,
            "市场": MARKET_LABELS[self.market],
            "数据日期": self.data_date,
            "观察标签": " / ".join(self.tags),
            "收盘": round(float(latest["close"]), 3),
            "20日涨幅%": round(float(latest["return20"]), 2),
            "相对基准20日超额百分点": self.rs20_pct,
            "ADR20%": round(float(latest["adr20"]), 2),
            "距EMA21/ATR": round(float(latest["distance_atr"]), 2),
            "待突破参考": self.trigger,
            "失效参考": self.invalidation,
            "数据源": self.source,
            "风险提示": "；".join(self.warnings),
            "规则版本": RULE_VERSION,
            "支撑带宽/ATR": self.parameters.support_atr,
            "延伸预警/ATR": self.parameters.extension_atr,
            "区间收缩比例": self.parameters.contraction_ratio,
        }


def normalize_daily_frame(
    df: pd.DataFrame, *, as_of: date, require_traded: bool = True,
) -> pd.DataFrame:
    """校验日线并移除截止日之后的行，不修补异常价格或伪造成交量。"""
    if df is None or df.empty:
        raise ValueError("没有可分析的日线数据")
    frame = df.copy()
    frame.columns = [COLUMN_ALIASES.get(str(c).strip(), str(c).strip().lower()) for c in frame.columns]
    if frame.columns.duplicated().any():
        raise ValueError("列名重复，或中英文列名映射后冲突")
    missing = set(REQUIRED_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"缺少日线字段：{', '.join(sorted(missing))}")
    try:
        dates = pd.to_datetime(frame["date"], format="mixed", errors="raise")
        if (dates != dates.dt.normalize()).any():
            raise ValueError("只支持完整日线，date 不应包含盘中时间")
        frame["date"] = dates.dt.tz_localize(None)
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("date 必须是有效交易日期；仅接受每日一根的日线") from exc
    frame = frame.loc[frame["date"] <= pd.Timestamp(as_of)].sort_values("date").reset_index(drop=True)
    for column in REQUIRED_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if not np.isfinite(frame[list(REQUIRED_COLUMNS[1:])].to_numpy(dtype=float)).all():
        raise ValueError("OHLCV 存在缺失、非数值或无穷值")
    report = check_kline_quality(frame)
    if not report.ok:
        raise ValueError("；".join(issue.message for issue in report.issues if issue.severity == "error"))
    if len(frame) < MIN_HISTORY:
        raise ValueError(f"截止日内仅有 {len(frame)} 根日线，至少需要 {MIN_HISTORY} 根")
    if require_traded and frame.iloc[-1]["volume"] <= 0:
        raise ValueError("最新日线成交量为零，可能停牌或为不完整快照，不生成信号")
    frame.attrs.update(df.attrs)
    return frame


def analyze_daily(
    df: pd.DataFrame,
    *,
    symbol: str,
    market: Literal["cn", "us"],
    as_of: date,
    parameters: LukParameters = LukParameters(),
    benchmark: pd.DataFrame | None = None,
) -> LukObservation:
    """使用截止日内数据生成多标签观察结果；所有参考位只来自当时可知数据。"""
    if market not in MARKET_LABELS:
        raise ValueError("仅支持 A 股 cn 或美股 us")
    require_supported_stock(symbol, market)
    frame = normalize_daily_frame(df, as_of=as_of)
    close, high, low, volume = (frame[c] for c in ("close", "high", "low", "volume"))
    for window in (9, 21, 50):
        frame[f"ema{window}"] = close.ewm(span=window, adjust=False).mean()
    true_range = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    frame["atr14"] = true_range.rolling(14).mean()
    frame["adr20"] = ((high / low - 1) * 100).rolling(20).mean()
    frame["return20"] = close.pct_change(20, fill_method=None) * 100
    frame["distance_atr"] = (close - frame["ema21"]) / frame["atr14"].replace(0, np.nan)
    frame["prior_high10"] = high.shift().rolling(10).max()
    frame["prior_low20"] = low.shift().rolling(20).min()
    daily_range = high - low
    range_base = daily_range.shift(3).rolling(10).mean()
    volume_base = volume.shift(3).rolling(10).mean()
    contraction = (daily_range.rolling(3).mean() <= range_base * parameters.contraction_ratio) & (volume.rolling(3).mean() <= volume_base)
    latest, previous = frame.iloc[-1], frame.iloc[-2]
    if latest["atr14"] <= 0:
        raise ValueError("近14日真实波动为零，无法计算支撑与延伸距离")
    rising = all(latest[f"ema{w}"] > frame.iloc[-6][f"ema{w}"] for w in (9, 21, 50))
    trend = bool(latest["ema9"] > latest["ema21"] > latest["ema50"] and rising and latest["close"] >= latest["ema21"])
    tags: list[str] = []
    reasons = ["EMA9/21/50 多头排列且近5日上行" if trend else "尚未满足完整多头均线条件"]
    warnings: list[str] = []
    trigger: float | None = None
    invalidation: float | None = None
    next_step = "保留观察，当前未命中三类日线候选。"
    extended = bool(latest["distance_atr"] > parameters.extension_atr)

    near_support = any(
        abs(latest["low"] - latest[f"ema{w}"]) <= parameters.support_atr * latest["atr14"]
        and latest["close"] >= latest[f"ema{w}"] for w in (9, 21)
    )
    has_pullback = bool(
        latest["close"] < close.iloc[-6:-1].max()
        or (latest["low"] < previous["low"] and latest["close"] < previous["high"])
    )
    if trend and near_support and has_pullback and not extended:
        tags.append("强势回调")
        reasons.append("回到 EMA9/21 观察带且收在其上，尚未明显延伸")
        if bool(contraction.iloc[-1]):
            reasons.append("回调期间最近3日区间与成交量同时收缩")
        trigger = float(latest["high"])
        invalidation = float(low.tail(5).min())
        next_step = "观察后续是否突破本日高点；日线候选不代表分钟入场已触发。"

    breakout = bool(latest["close"] > latest["prior_high10"] and contraction.iloc[-2])
    if trend and (bool(contraction.iloc[-1]) or breakout) and not extended:
        tags.append("收紧突破" if breakout else "收紧待突破")
        reasons.append("此前3日量幅收缩后，收盘突破前10日高" if breakout else "最近3日均幅收窄，均量不高于此前10日")
        trigger = float(latest["prior_high10"])
        invalidation = float(low.iloc[-4:-1].min() if breakout else low.tail(3).min())
        next_step = "收盘突破已发生，观察后续跟随或回测；不假定按突破价成交。" if breakout else "等待收盘突破既有10日高点；未突破前保持观察。"

    if latest["low"] < latest["prior_low20"] < latest["close"]:
        tags.append("U&R 收复")
        reasons.append(f"日低跌破此前20日低点 {latest['prior_low20']:.3f}，收盘重新站回")
        trigger = float(latest["high"])
        invalidation = float(latest["low"])
        next_step = "观察收复位能否守住及后续转强；U&R 不自动代表趋势已反转。"
        if not trend:
            warnings.append("U&R 出现在非完整多头结构中，单独评估趋势风险")
    if extended:
        tags.append("延伸预警")
        warnings.append(f"收盘距离 EMA21 超过 {parameters.extension_atr:g} ATR，缺乏靠近支撑的位置")
    if latest["close"] < latest["ema50"]:
        tags.append("结构偏弱")
        warnings.append("收盘位于 EMA50 下方，普通强势回调条件不成立")
    if latest["close"] < latest["ema9"] and previous["close"] >= previous["ema9"]:
        warnings.append("本日收盘下穿 EMA9；若已有持仓，复核事先约定的退出条件")
    if latest["close"] < latest["prior_low20"]:
        warnings.append("收盘跌破此前20日低点，原支撑结构失效")
    report = check_kline_quality(frame)
    warnings.extend(issue.message for issue in report.issues if issue.severity == "warning")
    if (as_of - latest["date"].date()).days > 7:
        warnings.append("最新数据距请求截止日超过7个自然日，请确认停牌、假期或行情延迟；不是当前交易日信号")

    rs20: float | None = None
    if benchmark is not None:
        bench = normalize_daily_frame(benchmark, as_of=as_of, require_traded=False).set_index("date")["close"]
        stock_dates = frame["date"].tail(21)
        aligned = bench.reindex(stock_dates)
        if aligned.notna().all():
            bench_return = (aligned.iloc[-1] / aligned.iloc[0] - 1) * 100
            rs20 = round(float(latest["return20"] - bench_return), 2)
            comparison = "领先" if rs20 > 0 else "落后" if rs20 < 0 else "持平"
            reasons.append(f"同日期20日收益较基准{comparison} {abs(rs20):.2f} 个百分点")
            if rs20 < 0:
                warnings.append("近20日跑输基准；均线趋势或形态标签不代表跑赢基准，需复核相对强弱。")
        else:
            warnings.append("基准与个股交易日期未完全对齐，不计算相对强度")
    if not tags:
        tags.append("普通观察")
    return LukObservation(
        symbol=symbol, market=market, data_date=latest["date"].strftime("%Y-%m-%d"),
        source=str(df.attrs.get("source", "用户 CSV")), tags=tuple(tags),
        reasons=tuple(reasons), warnings=tuple(warnings), trigger=trigger,
        invalidation=invalidation, next_step=next_step, frame=frame, rs20_pct=rs20,
        parameters=parameters,
    )
