"""纯日线参考公式；只消费已冻结快照，不执行隐式取数。"""

import math

import numpy as np
import pandas as pd

from stock_research.models import Bar
from stock_research.parameters import MartinParameters

FACTOR_DEFINITIONS = {
    "ema": "日线close.ewm(span=period,adjust=False).mean；9/21/50概念有访谈依据，初始化为旧工作台工程口径",
    "atr": "max(H-L,abs(H-prevC),abs(L-prevC))的滚动简单均值；窗口及ATR尺度属于工程定义",
    "adr": "mean((H/L-1)*100)；ADR概念有依据，窗口与精确公式为工程定义",
    "rs": "相同交易日的个股窗口收益减SPY收益，单位百分点；窗口与单一差值公式为工程定义",
    "contraction": "近recent根均幅/此前base根均幅，成交量同样比较；窗口和阈值为工程定义",
    "avwap": "已确认锚点起累计日线(H+L+C)/3乘成交量再除总量；日线估算，硬过滤关闭",
}


def factor_frame(bars: tuple[Bar, ...], parameters: MartinParameters) -> pd.DataFrame:
    """沿用来源模块EMA adjust=False、ATR简单平均和ADR20定义。"""
    if not bars or len({(bar.kind, bar.symbol) for bar in bars}) != 1:
        raise ValueError("因子输入必须包含单个证券的完整日线")
    frame = (
        pd.DataFrame(
            [
                {
                    "date": bar.session_date,
                    **{name: getattr(bar, name) for name in ("open", "high", "low", "close", "volume")},
                }
                for bar in bars
            ]
        )
        .sort_values("date")
        .reset_index(drop=True)
    )
    if frame.empty or frame["date"].duplicated().any():
        raise ValueError("因子输入为空或日期重复")
    close, high, low, volume = (frame[name] for name in ("close", "high", "low", "volume"))
    for label, period in zip(("fast", "middle", "slow"), parameters.ema_periods):
        frame[f"ema_{label}"] = close.ewm(span=period, adjust=False).mean()
    volatility_factors(frame, parameters)
    for period in sorted({1, 3, 5, 20, 60, parameters.rs_window}):
        frame[f"return_{period}"] = close.pct_change(period, fill_method=None) * 100
    recent, base = parameters.recent_window, parameters.base_window
    frame["range_ratio"] = (high - low).rolling(recent).mean() / (high - low).shift(recent).rolling(
        base
    ).mean().replace(0, np.nan)
    frame["volume_ratio"] = volume.rolling(recent).mean() / volume.shift(recent).rolling(base).mean().replace(0, np.nan)
    frame["extension"] = (close - frame["ema_middle"]) / frame["atr"].replace(0, np.nan)
    frame["breakout_level"] = high.shift().rolling(parameters.breakout_window).max()
    frame["breakout"] = close > frame["breakout_level"]
    frame["trend"] = (
        (frame["ema_fast"] > frame["ema_middle"])
        & (frame["ema_middle"] > frame["ema_slow"])
        & (close >= frame["ema_middle"])
    )
    for label in ("fast", "middle", "slow"):
        frame["trend"] &= frame[f"ema_{label}"] > frame[f"ema_{label}"].shift(parameters.slope_window)
    return frame


def volatility_factors(frame: pd.DataFrame, parameters: MartinParameters) -> None:
    """生产路径与对拍入口共用ATR/ADR参考公式，不另写评估代理。"""
    high, low, close = (frame[name] for name in ("high", "low", "close"))
    true_range = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    frame["atr"] = true_range.rolling(parameters.atr_window).mean()
    frame["adr"] = ((high / low - 1) * 100).rolling(parameters.adr_window).mean()


def finite(value: object) -> float | None:
    """预热和除零结果保存为缺失，不向JSON写入NaN或无穷值。"""
    result = float(value)
    return result if math.isfinite(result) else None


def latest_factors(
    frame: pd.DataFrame, benchmark: pd.DataFrame, parameters: MartinParameters
) -> dict[str, float | None]:
    """按相同交易日计算基准收益差，金额代理因单位未确认保持缺失。"""
    latest = frame.iloc[-1]
    values = {
        name: finite(latest[name])
        for name in (
            "close",
            "high",
            "low",
            "volume",
            "ema_fast",
            "ema_middle",
            "ema_slow",
            "atr",
            "adr",
            "return_1",
            "return_3",
            "return_5",
            "return_20",
            "return_60",
            "range_ratio",
            "volume_ratio",
            "extension",
        )
    }
    dates = frame["date"].tail(parameters.rs_window + 1)
    aligned = benchmark.set_index("date")["close"].reindex(dates)
    values["rs"] = (
        finite(latest[f"return_{parameters.rs_window}"] - (aligned.iloc[-1] / aligned.iloc[0] - 1) * 100)
        if len(dates) == parameters.rs_window + 1 and aligned.notna().all()
        else None
    )
    values["dollar_volume_proxy"] = None
    return values


def anchor_evidence(frame: pd.DataFrame, parameters: MartinParameters) -> tuple[dict[str, object], ...]:
    """摆动锚点须经过右侧确认，只展示日线典型价格的估算均价。"""
    points = []
    start = max(parameters.swing_left, len(frame) - parameters.anchor_window)
    for index in range(start, len(frame) - parameters.swing_right):
        window = frame.iloc[index - parameters.swing_left : index + parameters.swing_right + 1]
        row = frame.iloc[index]
        for kind, column, extreme in (
            ("swing_low", "low", window["low"].min()),
            ("swing_high", "high", window["high"].max()),
        ):
            if row[column] == extreme:
                points.append((index, kind, str(frame.iloc[index + parameters.swing_right]["date"])))
    for index in frame.index[frame["breakout"]].tolist():
        if index >= len(frame) - parameters.anchor_window:
            points.append((index, "breakout", str(frame.iloc[index]["date"])))
    result = []
    for index, kind, confirmed in points:
        segment = frame.iloc[index:]
        total = segment["volume"].sum()
        price = (
            ((segment["high"] + segment["low"] + segment["close"]) / 3 * segment["volume"]).sum() / total
            if total > 0
            else np.nan
        )
        result.append(
            {
                "kind": kind,
                "date": str(frame.iloc[index]["date"]),
                "confirmed_date": confirmed,
                "daily_estimate": finite(price),
                "hard_filter_enabled": False,
            }
        )
    return tuple(result)
