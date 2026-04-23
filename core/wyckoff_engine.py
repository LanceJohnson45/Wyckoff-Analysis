# -*- coding: utf-8 -*-
"""
Wyckoff Funnel 5 层漏斗筛选引擎

Layer 1: 剥离垃圾（ST / 北交所 / 科创板 / 市值 / 成交额）
Layer 2: C-lite 三轨甄选（主升确认/启动确认/吸筹改善）
Layer 2.5: Markup 加速检测
Layer 3: 板块共振（行业分布 Top-N + RPS 动量）
Layer 4: 威科夫狙击（Spring / SOS / LPS / Effort vs Result）
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DataIntegrityPolicy:
    min_coverage_200: float = 0.98
    min_coverage_20: float = 0.95
    strict_recent_days: int = 3


@dataclass(frozen=True)
class LiquidityFilterConfig:
    min_market_cap_yi: float
    min_avg_amount_wan: float
    amount_avg_window: int


@dataclass(frozen=True)
class TrendFilterConfig:
    ma_short: int
    ma_long: int
    ma_hold: int
    bench_drop_days: int
    bench_drop_threshold: float
    rs_window_long: int
    rs_window_short: int
    rs_min_long: float
    rs_min_short: float
    rps_window_fast: int
    rps_window_slow: int
    rps_fast_min: float
    rps_slow_min: float


@dataclass(frozen=True)
class TriggerConfig:
    spring_support_window: int
    lps_vol_ref_window: int
    sos_vol_window: int
    sos_breakout_window: int
    evr_vol_window: int
    evr_min_turnover: float


@dataclass(frozen=True)
class ExitSignalConfig:
    enable_exit_signals: bool
    exit_stop_loss_pct: float
    exit_trailing_active_pct: float
    exit_trailing_drawdown_pct: float
    dist_high_threshold_pct: float
    dist_vol_dry_ratio: float
    dist_confirm_days: int


@dataclass(frozen=True)
class SymbolFeatureBundle:
    df: pd.DataFrame
    close: pd.Series
    high: pd.Series
    low: pd.Series
    volume: pd.Series
    amount: pd.Series
    pct_chg: pd.Series
    turnover: pd.Series
    ma_short: pd.Series
    ma_long: pd.Series
    ma_hold: pd.Series


@dataclass(frozen=True)
class L2Metrics:
    symbol: str
    market: str
    close: float | None
    ma20: float | None
    ma50: float | None
    ma200: float | None
    bias_200: float | None
    rs_long: float | None
    rs_short: float | None
    rps_fast: float | None
    rps_slow: float | None
    rps_slope: float | None
    ret_5: float | None
    ret_10: float | None
    ret_20: float | None
    breakout_proximity_20: float | None
    breakout_proximity_60: float | None
    volume_expansion: float | None
    price_from_250d_low: float | None
    range_60_pct: float | None
    dry_volume_ratio: float | None
    ma_gap_pct: float | None
    old_channels: dict[str, bool]


@dataclass(frozen=True)
class TrackScoreDetail:
    track: str
    required_passed: bool
    score: float
    reasons: dict[str, object]
    penalties: dict[str, float]


@dataclass(frozen=True)
class L2Decision:
    symbol: str
    passed: bool
    selected_track: str | None
    selected_score: float
    track_scores: dict[str, float]
    old_channels: dict[str, bool]
    reasons: dict[str, object]


def normalize_hist_from_fetch(df: pd.DataFrame) -> pd.DataFrame:
    """将 fetch_a_share_csv._fetch_hist 返回的 DataFrame 转为筛选器所需格式。"""
    from core.stock_cache import _COL_MAP

    col_map = {**_COL_MAP, "换手率": "turnover", "换手": "turnover"}
    out = df.rename(columns=col_map)
    keep = [
        c
        for c in [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "pct_chg",
            "turnover",
        ]
        if c in out.columns
    ]
    out = out[keep].copy()
    if "pct_chg" not in out.columns and "close" in out.columns:
        out["pct_chg"] = out["close"].astype(float).pct_change() * 100
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pct_chg",
        "turnover",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _sorted_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return df
    try:
        if df["date"].is_monotonic_increasing:
            return df
    except Exception:
        pass
    return df.sort_values("date")


def _latest_trade_date(df: pd.DataFrame) -> object | None:
    if df is None or df.empty or "date" not in df.columns:
        return None
    s = pd.to_datetime(df["date"], errors="coerce").dropna()
    return s.iloc[-1].date() if not s.empty else None


def build_symbol_feature_bundle(
    df: pd.DataFrame,
    cfg: FunnelConfig,
) -> SymbolFeatureBundle:
    df_sorted = _sorted_if_needed(df)
    close = pd.to_numeric(df_sorted.get("close"), errors="coerce")
    high = pd.to_numeric(df_sorted.get("high"), errors="coerce")
    low = pd.to_numeric(df_sorted.get("low"), errors="coerce")
    volume = pd.to_numeric(df_sorted.get("volume"), errors="coerce")
    amount = pd.to_numeric(df_sorted.get("amount"), errors="coerce")
    pct_chg = pd.to_numeric(df_sorted.get("pct_chg"), errors="coerce")
    turnover = pd.to_numeric(df_sorted.get("turnover"), errors="coerce")
    return SymbolFeatureBundle(
        df=df_sorted,
        close=close,
        high=high,
        low=low,
        volume=volume,
        amount=amount,
        pct_chg=pct_chg,
        turnover=turnover,
        ma_short=close.rolling(cfg.ma_short).mean(),
        ma_long=close.rolling(cfg.ma_long).mean(),
        ma_hold=close.rolling(cfg.ma_hold).mean(),
    )


def build_feature_map(
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
) -> dict[str, SymbolFeatureBundle]:
    return {
        sym: build_symbol_feature_bundle(df, cfg)
        for sym, df in df_map.items()
        if df is not None and not df.empty
    }


def assess_hist_integrity(
    df: pd.DataFrame,
    expected_dates: list[object],
    policy: DataIntegrityPolicy | None = None,
) -> tuple[bool, dict[str, float | int]]:
    policy = policy or DataIntegrityPolicy()
    if df is None or df.empty or "date" not in df.columns or not expected_dates:
        strict_recent = max(int(policy.strict_recent_days), 0)
        return (
            False,
            {
                "coverage_200": 0.0,
                "coverage_20": 0.0,
                "missing_recent": strict_recent,
            },
        )

    actual_dates = set(
        pd.to_datetime(df["date"], errors="coerce").dropna().dt.date.tolist()
    )
    normalized_expected = [
        pd.to_datetime(x, errors="coerce").date()
        for x in expected_dates
        if pd.notna(pd.to_datetime(x, errors="coerce"))
    ]
    if not normalized_expected:
        return (
            False,
            {"coverage_200": 0.0, "coverage_20": 0.0, "missing_recent": 0},
        )

    overall_200 = normalized_expected[-min(len(normalized_expected), 200) :]
    recent_20 = normalized_expected[-min(len(normalized_expected), 20) :]
    strict_recent = normalized_expected[
        -min(len(normalized_expected), max(int(policy.strict_recent_days), 0)) :
    ]
    coverage_200 = (
        sum(1 for d in overall_200 if d in actual_dates) / len(overall_200)
        if overall_200
        else 0.0
    )
    coverage_20 = (
        sum(1 for d in recent_20 if d in actual_dates) / len(recent_20)
        if recent_20
        else 0.0
    )
    missing_recent = sum(1 for d in strict_recent if d not in actual_dates)
    ok = (
        coverage_200 >= float(policy.min_coverage_200)
        and coverage_20 >= float(policy.min_coverage_20)
        and missing_recent == 0
    )
    return (
        ok,
        {
            "coverage_200": round(float(coverage_200), 4),
            "coverage_20": round(float(coverage_20), 4),
            "missing_recent": int(missing_recent),
        },
    )


def filter_symbols_by_integrity(
    df_map: dict[str, pd.DataFrame],
    expected_dates: list[object],
    policy: DataIntegrityPolicy | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, float | int]]]:
    passed: dict[str, pd.DataFrame] = {}
    rejected: dict[str, dict[str, float | int]] = {}
    for sym, df in df_map.items():
        ok, stats = assess_hist_integrity(df, expected_dates, policy=policy)
        if ok:
            passed[sym] = df
        else:
            rejected[sym] = stats
    return passed, rejected


# Config


@dataclass
class FunnelConfig:
    profile: str = "cn"
    market_template: str = "cn"
    style_template: str = "cn_value"
    trading_days: int = 320

    # Layer 1
    min_market_cap_yi: float = 35.0
    min_avg_amount_wan: float = 5000.0
    amount_avg_window: int = 20

    # Layer 2
    ma_short: int = 50
    ma_long: int = 200
    ma_hold: int = 20
    bench_drop_days: int = 3
    bench_drop_threshold: float = -2.0
    rs_window_long: int = 10
    rs_window_short: int = 3
    rs_min_long: float = 2.0  # 10 日 RS 至少跑赢大盘 2%（原 0.0 形同虚设）
    rs_min_short: float = 1.0  # 3 日 RS 至少跑赢大盘 1%
    enable_rs_filter: bool = True
    enable_rps_filter: bool = True
    rps_window_fast: int = 50
    rps_window_slow: int = 120
    rps_fast_min: float = 75.0
    rps_slow_min: float = 70.0
    rps_slope_window: int = 10  # 计算 RPS 斜率的窗口（交易日）
    rps_slope_min: float = 0.5  # RPS 斜率最小值（%/day），用于判断 RPS 是否还在上升
    require_bench_latest_alignment: bool = False
    momentum_bias_200_max: float = 0.25  # 防止主升通道选出离 200 日线太远的鱼尾老妖股
    # Layer 2 潜伏通道（长强短弱）
    enable_ambush_channel: bool = True
    ambush_rps_fast_max: float = 45.0
    ambush_rps_slow_min: float = 70.0
    ambush_rs_long_min: float = -2.0
    ambush_rs_short_min: float = -8.0
    ambush_bias_200_abs_max: float = 0.08
    ambush_ret20_max: float = -3.0

    # Layer 2 低位吸筹通道（Wyckoff Accumulation Channel）
    # 不依赖 RPS 强势排名，专门捕捉”已止跌横盘蓄势”的 Phase A/B/C 股票。
    # 触发条件：低位区间 + 横盘振幅小 + 量能萎缩 + 均线胶着（尚未多头排列）。
    # 这类股票应与 L4 Spring/LPS 配合使用，单独出现时仅进观察池。
    enable_accumulation_channel: bool = True
    accum_lookback_days: int = 250  # 年内低点计算窗口（交易日）
    accum_price_from_low_max: float = 0.35  # 现价不超过年内低点 +35%
    accum_range_window: int = 60  # 横盘振幅计算窗口（交易日）
    accum_range_max_pct: float = 30.0  # 窗口内 (high_max-low_min)/low_min 不超过 30%
    accum_vol_dry_window: int = 20  # 量能萎缩统计近 N 日
    accum_vol_dry_ref_window: int = 120  # 量能萎缩对比参考窗口
    accum_vol_dry_ratio: float = 0.65  # 近 N 日均量 / 参考均量 < 此值（量能萎缩）
    accum_ma_gap_max: float = 0.06  # |MA50 - MA200| / MA200 < 此值（均线胶着）

    # Layer 2 地量蓄势通道（Dry Volume Channel）
    # 低位区间内，近期某日出现了年内最低级别的单日成交量，说明卖压完全枯竭。
    enable_dry_vol_channel: bool = True
    dry_vol_lookback: int = 10  # 在最近 N 日内寻找地量
    dry_vol_ref_window: int = 250  # 地量参考窗口（年维度）
    dry_vol_quantile: float = 0.05  # 地量标准：低于年内成交量的 5% 分位数
    dry_vol_price_from_low_max: float = 0.35  # 位阶保护：现价 <= 年内低点 +35%

    # Layer 2 暗中护盘通道（RS Divergence Channel）
    # 大盘近期创新低，但该股拒绝创新低，形成 Higher Low，说明有资金托底。
    enable_rs_divergence_channel: bool = True
    rs_div_bench_window: int = 20  # 大盘近 N 日内需出现新低
    rs_div_stock_window: int = 20  # 个股同期窗口
    rs_div_bench_ref_window: int = 60  # 大盘新低对比的参考窗口（近 60 日）
    rs_div_price_from_low_max: float = 0.50  # 位阶保护：现价 <= 年内低点 +50%

    # Layer 2 C-lite 三轨决策：复用上方指标，重构最终通过逻辑。
    enable_track_momentum: bool = True
    enable_track_early_strength: bool = True
    enable_track_accumulation: bool = True
    track_a_min_score: float = 70.0
    track_b_min_score: float = 62.0
    track_c_min_score: float = 58.0
    track_a_rps_fast_min: float = 75.0
    track_a_rps_slow_min: float = 70.0
    track_a_rs_long_min: float = 2.0
    track_a_rps_slope_min: float = 0.5
    track_a_bias_200_max: float = 0.25
    track_b_rps_fast_min: float = 60.0
    track_b_rs_short_min: float = 0.0
    track_b_breakout_proximity_min: float = 70.0
    track_c_price_from_low_max: float = 0.40
    track_c_severe_downtrend_ret20_min: float = -8.0

    # Layer 3
    # 行业共振过滤：按”行业样本数分位阈值 + 最小样本数”动态过滤，避免固定 TopN 误杀。
    top_n_sectors: int = 5
    sector_min_count: int = 3
    sector_count_quantile: float = 0.70
    sector_super_strength_quantile: float = 0.90  # 小而强板块免死阈值（强度分位）

    # Layer 4 - Spring
    spring_support_window: int = 60
    spring_vol_ratio: float = (
        1.1  # 放宽以激活 Accum 轨（原 1.3 导致 Spring 几乎不触发）
    )
    spring_tr_max_range_pct: float = 30.0
    spring_tr_max_drift_pct: float = 12.0
    # Spring 动态振幅
    spring_tr_atr_window: int = 20  # 计算 ATR 的历史窗口
    spring_tr_atr_max_multiple: float = (
        4.0  # 区间最大允许振幅为 ATR_pct 的 N 倍(替代固定的30%)
    )
    spring_vol_expand_ratio: float = (
        1.15  # 收回时的成交量 / 下探时的成交量 > 此值（原 1.3 过严）
    )

    # Layer 4 - LPS
    lps_lookback: int = 3
    lps_ma: int = 20
    lps_ma_tolerance: float = 0.02
    lps_vol_dry_ratio: float = (
        0.48  # A/B 验证：0.48 夏普 2.325 >> 0.55 夏普 0.831 ⚠️ 样内优化，需 OOS 验证
    )
    lps_vol_ref_window: int = 60

    # Layer 4 - Effort vs Result
    enable_evr_trigger: bool = True
    evr_lookback: int = 3
    evr_vol_ratio: float = 1.3
    evr_min_turnover: float = 1.0  # 保守过滤：剔除死水微量放大，不对大票一刀切
    evr_vol_window: int = 20
    evr_max_drop: float = 2.0
    evr_max_bias_200: float = 40.0
    evr_confirm_days: int = 1
    evr_confirm_allow_break_pct: float = 0.0

    # Layer 4 - SOS / JAC (Sign of Strength / Jump Across the Creek)
    sos_pct_min: float = 6.0  # 提高门槛过滤弱突破（原 4.5 追高触发止损率极高）
    sos_vol_ratio: float = 2.5  # 要求更强量能确认（原 2.0 噪音太多）
    sos_vol_window: int = 20  # 计算点火爆量时的参考窗口
    sos_breakout_window: int = 20  # 要求突破或接近近 N 日的高点
    sos_breakout_tolerance: float = 0.01  # 改为 0.01：突破容差 1%（从 2% 改为 1%）
    sos_max_bias_200: float = 20.0  # 收紧离 200 日线的距离（防止在高空放量诱多）
    # SOS 动态极值爆量
    sos_vol_quantile_window: int = 60  # 计算量能分位数的滚动窗口
    sos_vol_quantile: float = 0.95  # 要求当日量能突破历史 N 日的 95% 分位数

    # Markup 阶段识别（Layer 2.5）
    enable_markup_detection: bool = True
    markup_ma_crossover_confirm_days: int = 5  # MA50 穿过 MA200 后，需要连续 N 日在上方
    markup_ma_angle_min: float = (
        2.0  # MA50 的角度（% per 5 days），用于确认上升趋势强度
    )
    markup_rs_positive_min: float = 0.5  # RS_short 需要保持正值且持续增强

    # Accumulation ABC 细化（Layer 2 增强）
    enable_accum_abc_detail: bool = True
    accum_b_test_count: int = 3  # B 阶段需要测试底部至少 N 次
    accum_c_max_drop_ratio: float = 0.03  # C 阶段下跌不超过 A 低的 3%

    # Exit 策略（Layer 5）
    enable_exit_signals: bool = True
    exit_stop_loss_pct: float = (
        -7.0
    )  # 网格优化最佳：-7%/+18%（夏普2.493），-6%偏紧，-8%偏松
    exit_trailing_active_pct: float = (
        15.0  # 利润激活线：从底部上涨超过此比例，激活移动跟踪止损
    )
    exit_trailing_drawdown_pct: float = -10.0  # 利润保护线：高位跟踪回撤止损幅度（%）

    # Distribution 识别：高位缩量警告
    dist_high_threshold_pct: float = 30.0  # 相对 MA200 的高度（%）
    dist_vol_dry_ratio: float = 0.5  # 高位缩量比
    dist_confirm_days: int = 3  # 需要连续确认 N 日

    @classmethod
    def for_market(cls, market: str) -> "FunnelConfig":
        market_norm = str(market or "cn").strip().lower()
        return cls.for_profile(_PROFILE_ALIASES.get(market_norm, "cn"))

    @classmethod
    def for_profile(cls, profile: str, *, market: str | None = None) -> "FunnelConfig":
        """Build one of the three market profiles: cn, hk, or us."""
        profile_key = _normalize_template_key(profile)
        profile_key = _PROFILE_ALIASES.get(profile_key, profile_key)
        if profile_key in _PROFILE_OVERRIDES:
            cfg = cls()
            _apply_config_overrides(cfg, _PROFILE_OVERRIDES[profile_key])
            cfg.profile = profile_key
            cfg.market_template = profile_key
            cfg.style_template = f"{profile_key}_value"
            return cfg
        raise ValueError(f"unknown funnel profile: {profile}")

    @property
    def liquidity(self) -> LiquidityFilterConfig:
        return LiquidityFilterConfig(
            min_market_cap_yi=float(self.min_market_cap_yi),
            min_avg_amount_wan=float(self.min_avg_amount_wan),
            amount_avg_window=int(self.amount_avg_window),
        )

    @property
    def trend(self) -> TrendFilterConfig:
        return TrendFilterConfig(
            ma_short=int(self.ma_short),
            ma_long=int(self.ma_long),
            ma_hold=int(self.ma_hold),
            bench_drop_days=int(self.bench_drop_days),
            bench_drop_threshold=float(self.bench_drop_threshold),
            rs_window_long=int(self.rs_window_long),
            rs_window_short=int(self.rs_window_short),
            rs_min_long=float(self.rs_min_long),
            rs_min_short=float(self.rs_min_short),
            rps_window_fast=int(self.rps_window_fast),
            rps_window_slow=int(self.rps_window_slow),
            rps_fast_min=float(self.rps_fast_min),
            rps_slow_min=float(self.rps_slow_min),
        )

    @property
    def triggers(self) -> TriggerConfig:
        return TriggerConfig(
            spring_support_window=int(self.spring_support_window),
            lps_vol_ref_window=int(self.lps_vol_ref_window),
            sos_vol_window=int(self.sos_vol_window),
            sos_breakout_window=int(self.sos_breakout_window),
            evr_vol_window=int(self.evr_vol_window),
            evr_min_turnover=float(self.evr_min_turnover),
        )

    @property
    def exit(self) -> ExitSignalConfig:
        return ExitSignalConfig(
            enable_exit_signals=bool(self.enable_exit_signals),
            exit_stop_loss_pct=float(self.exit_stop_loss_pct),
            exit_trailing_active_pct=float(self.exit_trailing_active_pct),
            exit_trailing_drawdown_pct=float(self.exit_trailing_drawdown_pct),
            dist_high_threshold_pct=float(self.dist_high_threshold_pct),
            dist_vol_dry_ratio=float(self.dist_vol_dry_ratio),
            dist_confirm_days=int(self.dist_confirm_days),
        )


ConfigOverride = dict[str, float | int | bool]


def _normalize_template_key(value: str) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _apply_config_overrides(cfg: FunnelConfig, overrides: ConfigOverride) -> None:
    for key, value in overrides.items():
        setattr(cfg, key, value)


_PROFILE_ALIASES: dict[str, str] = {
    "cn": "cn",
    "hk": "hk",
    "us": "us",
    "a": "cn",
    "ashare": "cn",
    "a_value": "cn",
    "cn_mainboard": "cn",
    "hk_value": "hk",
    "hk_mainboard": "hk",
    "us_value": "us",
    "us_trend_value": "us",
}

_PROFILE_OVERRIDES: dict[str, ConfigOverride] = {
    "cn": {},
    "hk": {
        "min_market_cap_yi": 0.0,
        "min_avg_amount_wan": 4000.0,
        "bench_drop_threshold": -2.2,
        "rs_window_long": 15,
        "rs_window_short": 5,
        "rs_min_long": 1.5,
        "rs_min_short": 0.8,
        "rps_window_fast": 40,
        "rps_window_slow": 100,
        "rps_fast_min": 70.0,
        "rps_slow_min": 65.0,
        "rps_slope_min": 0.45,
        "momentum_bias_200_max": 0.27,
        "top_n_sectors": 4,
        "sector_min_count": 1,
        "sector_count_quantile": 0.60,
        "sector_super_strength_quantile": 0.88,
        "enable_ambush_channel": False,
        "track_a_min_score": 76.0,
        "track_a_rps_fast_min": 80.0,
        "track_a_rps_slow_min": 72.0,
        "track_a_rs_long_min": 2.0,
        "track_a_rps_slope_min": 0.45,
        "track_b_min_score": 62.0,
        "track_b_rps_fast_min": 58.0,
        "track_b_breakout_proximity_min": 68.0,
        "track_c_min_score": 58.0,
        "track_c_price_from_low_max": 0.45,
        "ambush_rps_fast_max": 50.0,
        "ambush_rps_slow_min": 62.0,
        "ambush_rs_long_min": -3.0,
        "ambush_rs_short_min": -6.0,
        "ambush_bias_200_abs_max": 0.10,
        "ambush_ret20_max": -4.0,
        "accum_price_from_low_max": 0.40,
        "accum_range_window": 70,
        "accum_range_max_pct": 32.0,
        "accum_vol_dry_ratio": 0.72,
        "accum_ma_gap_max": 0.08,
        "dry_vol_lookback": 12,
        "dry_vol_quantile": 0.08,
        "dry_vol_price_from_low_max": 0.40,
        "rs_div_price_from_low_max": 0.55,
        "spring_support_window": 70,
        "spring_vol_ratio": 1.12,
        "spring_tr_max_range_pct": 35.0,
        "spring_tr_max_drift_pct": 14.0,
        "spring_tr_atr_max_multiple": 4.5,
        "spring_vol_expand_ratio": 1.10,
        "lps_lookback": 4,
        "lps_ma_tolerance": 0.025,
        "lps_vol_dry_ratio": 0.50,
        "enable_evr_trigger": True,
        "evr_vol_ratio": 1.22,
        "evr_min_turnover": 0.0,
        "evr_max_drop": 2.8,
        "evr_max_bias_200": 45.0,
        "evr_confirm_allow_break_pct": 0.5,
        "sos_pct_min": 5.5,
        "sos_vol_ratio": 2.2,
        "sos_breakout_window": 25,
        "sos_breakout_tolerance": 0.012,
        "sos_max_bias_200": 26.0,
        "sos_vol_quantile": 0.93,
        "markup_ma_angle_min": 1.6,
        "markup_rs_positive_min": 0.2,
        "accum_c_max_drop_ratio": 0.035,
        "exit_stop_loss_pct": -9.0,
        "exit_trailing_active_pct": 15.0,
        "exit_trailing_drawdown_pct": -10.0,
        "dist_high_threshold_pct": 32.0,
        "dist_vol_dry_ratio": 0.55,
    },
    "us": {
        "min_market_cap_yi": 0.0,
        "min_avg_amount_wan": 0.0,
        "bench_drop_threshold": -2.5,
        "rs_window_long": 20,
        "rs_window_short": 5,
        "rs_min_long": 4.0,
        "rs_min_short": 3.0,
        "rps_window_fast": 50,
        "rps_window_slow": 120,
        "rps_fast_min": 85.0,
        "rps_slow_min": 80.0,
        "rps_slope_window": 12,
        "rps_slope_min": 0.75,
        "momentum_bias_200_max": 0.28,
        "top_n_sectors": 20,
        "sector_min_count": 1,
        "sector_count_quantile": 0.50,
        "sector_super_strength_quantile": 0.85,
        "enable_ambush_channel": False,
        "enable_accumulation_channel": False,
        "enable_dry_vol_channel": False,
        "enable_rs_divergence_channel": False,
        "enable_track_accumulation": False,
        "track_a_min_score": 74.0,
        "track_b_min_score": 64.0,
        "track_a_rps_fast_min": 85.0,
        "track_a_rps_slow_min": 80.0,
        "track_a_rs_long_min": 4.0,
        "track_a_rps_slope_min": 0.75,
        "track_b_rps_fast_min": 65.0,
        "track_b_rs_short_min": 1.0,
        "track_b_breakout_proximity_min": 72.0,
        "ambush_rps_fast_max": 55.0,
        "ambush_rps_slow_min": 65.0,
        "ambush_rs_long_min": -4.0,
        "ambush_rs_short_min": -6.0,
        "ambush_bias_200_abs_max": 0.12,
        "ambush_ret20_max": -5.0,
        "accum_price_from_low_max": 0.45,
        "accum_range_window": 80,
        "accum_range_max_pct": 35.0,
        "accum_vol_dry_ratio": 0.75,
        "accum_ma_gap_max": 0.10,
        "dry_vol_lookback": 15,
        "dry_vol_quantile": 0.10,
        "dry_vol_price_from_low_max": 0.45,
        "rs_div_price_from_low_max": 0.60,
        "spring_support_window": 80,
        "spring_vol_ratio": 1.2,
        "spring_tr_max_range_pct": 28.0,
        "spring_tr_max_drift_pct": 15.0,
        "spring_tr_atr_max_multiple": 4.5,
        "spring_vol_expand_ratio": 1.08,
        "lps_lookback": 4,
        "lps_ma_tolerance": 0.025,
        "lps_vol_dry_ratio": 0.60,
        "enable_evr_trigger": True,
        "evr_vol_ratio": 1.3,
        "evr_min_turnover": 0.0,
        "evr_max_drop": 2.8,
        "evr_max_bias_200": 35.0,
        "evr_confirm_days": 2,
        "evr_confirm_allow_break_pct": 0.5,
        "sos_pct_min": 4.5,
        "sos_vol_ratio": 2.5,
        "sos_breakout_window": 30,
        "sos_breakout_tolerance": 0.012,
        "sos_max_bias_200": 22.0,
        "sos_vol_quantile": 0.92,
        "markup_ma_crossover_confirm_days": 6,
        "markup_ma_angle_min": 1.4,
        "markup_rs_positive_min": 0.1,
        "accum_b_test_count": 2,
        "accum_c_max_drop_ratio": 0.04,
        "exit_stop_loss_pct": -8.0,
        "exit_trailing_active_pct": 18.0,
        "exit_trailing_drawdown_pct": -10.0,
        "dist_high_threshold_pct": 35.0,
        "dist_vol_dry_ratio": 0.60,
        "dist_confirm_days": 4,
    },
}


class FunnelResult(NamedTuple):
    layer1_symbols: list[str]
    layer2_symbols: list[str]
    layer3_symbols: list[str]
    top_sectors: list[str]
    triggers: dict[str, list[tuple[str, float]]]
    # 威科夫阶段细节
    stage_map: dict[
        str, str
    ]  # code -> stage_name（如 "Accumulation A"、"Markup"、"Distribution"）
    markup_symbols: list[str]  # 已进入 Markup 的股票
    exit_signals: dict[
        str, dict
    ]  # code -> {"signal": "stop_loss|distribution_warning", "price": xxx, "reason": xxx}
    channel_map: dict[str, str]
    explanations: dict[str, dict] | None = None
    layer1_rejections: dict[str, dict] | None = None
    layer2_rejections: dict[str, dict] | None = None


def fit_ai_candidate_quotas(
    total_cap: int,
    trend_quota: int,
    accum_quota: int,
) -> tuple[int, int]:
    """Fit requested Trend/Accum quotas into a global total cap."""
    total_cap_local = max(int(total_cap), 0)
    trend_quota_local = max(int(trend_quota), 0)
    accum_quota_local = max(int(accum_quota), 0)
    if total_cap_local <= 0:
        return (0, 0)

    requested_total = trend_quota_local + accum_quota_local
    if requested_total <= total_cap_local:
        return (trend_quota_local, accum_quota_local)
    if requested_total <= 0:
        return (0, 0)

    trend_eff = min(
        max(int(round(total_cap_local * (trend_quota_local / requested_total))), 0),
        trend_quota_local,
    )
    accum_eff = min(accum_quota_local, max(total_cap_local - trend_eff, 0))
    remaining = max(total_cap_local - trend_eff - accum_eff, 0)

    if remaining > 0 and trend_eff < trend_quota_local:
        take = min(remaining, trend_quota_local - trend_eff)
        trend_eff += take
        remaining -= take
    if remaining > 0 and accum_eff < accum_quota_local:
        take = min(remaining, accum_quota_local - accum_eff)
        accum_eff += take

    return (trend_eff, accum_eff)


def resolve_ai_candidate_policy(
    regime: str,
    override_total_cap: int = -1,
) -> dict[str, int | str]:
    """
    Central source of truth for AI allocation defaults.

    CRASH / PANIC_REPAIR / BLACK_SWAN all share the defensive quota family
    instead of silently falling back to NEUTRAL.
    """
    import os

    total_cap = (
        max(int(os.getenv("FUNNEL_AI_TOTAL_CAP", "12")), 0)
        if override_total_cap < 0
        else max(int(override_total_cap), 0)
    )
    # 配额重平衡：原版严重偏向 Accum 左侧（4/8, 3/7, 1/5），导致大量底部横盘股拉低胜率。
    # 现改为 Trend 优先：右侧已确认趋势的股票胜率远高于左侧潜伏。
    risk_on_trend = max(int(os.getenv("FUNNEL_AI_RISK_ON_TREND", "7")), 0)
    risk_on_accum = max(int(os.getenv("FUNNEL_AI_RISK_ON_ACCUM", "5")), 0)
    risk_off_trend = max(int(os.getenv("FUNNEL_AI_RISK_OFF_TREND", "2")), 0)
    risk_off_accum = max(int(os.getenv("FUNNEL_AI_RISK_OFF_ACCUM", "3")), 0)
    neutral_trend = max(int(os.getenv("FUNNEL_AI_NEUTRAL_TREND", "5")), 0)
    neutral_accum = max(int(os.getenv("FUNNEL_AI_NEUTRAL_ACCUM", "5")), 0)
    max_trend_l3_fill = max(int(os.getenv("FUNNEL_AI_MAX_TREND_L3_FILL", "0")), 0)
    max_accum_l3_fill = max(int(os.getenv("FUNNEL_AI_MAX_ACCUM_L3_FILL", "0")), 0)

    regime_norm = str(regime or "").strip().upper()
    if regime_norm == "RISK_ON":
        requested_trend = risk_on_trend
        requested_accum = risk_on_accum
        quota_family = "RISK_ON"
    elif regime_norm in {"RISK_OFF", "CRASH", "PANIC_REPAIR", "BLACK_SWAN"}:
        requested_trend = risk_off_trend
        requested_accum = risk_off_accum
        quota_family = "RISK_OFF"
    else:
        requested_trend = neutral_trend
        requested_accum = neutral_accum
        quota_family = "NEUTRAL"

    trend_quota, accum_quota = fit_ai_candidate_quotas(
        total_cap,
        requested_trend,
        requested_accum,
    )
    return {
        "regime": regime_norm or "NEUTRAL",
        "quota_family": quota_family,
        "total_cap": total_cap,
        "requested_trend_quota": requested_trend,
        "requested_accum_quota": requested_accum,
        "trend_quota": trend_quota,
        "accum_quota": accum_quota,
        "max_trend_l3_fill": max_trend_l3_fill,
        "max_accum_l3_fill": max_accum_l3_fill,
    }


# Layer 1: 剥离垃圾


def _is_main_or_chinext(code: str) -> bool:
    return code.startswith(
        ("600", "601", "603", "605", "000", "001", "002", "003", "300", "301")
    )


def layer1_filter(
    symbols: list[str],
    name_map: dict[str, str],
    market_cap_map: dict[str, float],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
    *,
    market: str = "cn",
    feature_map: dict[str, SymbolFeatureBundle] | None = None,
    return_rejections: bool = False,
) -> list[str] | tuple[list[str], dict[str, dict[str, float | str | bool | None]]]:
    """
    硬过滤：剔除 ST、北交所/科创板、市值<阈值、近期均成交额<阈值。
    market_cap_map 单位：亿元。若 market_cap_map 为空则跳过市值过滤。
    """
    cap_available = bool(market_cap_map)
    market_norm = str(market or "cn").strip().lower()
    passed: list[str] = []
    rejected: dict[str, dict[str, float | str | bool | None]] = {}
    for sym in symbols:
        if market_norm == "cn" and not _is_main_or_chinext(sym):
            rejected[sym] = {"reason": "unsupported_cn_board"}
            continue
        name = name_map.get(sym, "")
        if market_norm == "cn" and "ST" in name.upper():
            rejected[sym] = {"reason": "st_flagged", "name": name}
            continue
        if cap_available:
            cap_raw = market_cap_map.get(sym)
            try:
                cap = float(cap_raw) if cap_raw is not None else None
            except Exception:
                cap = None
            # Partial market-cap caches are expected when sharesOutstanding is
            # backfilled gradually. Missing caps should not reject otherwise
            # valid symbols; only enforce the threshold for known cap values.
            if cap is not None and cap < cfg.min_market_cap_yi:
                rejected[sym] = {
                    "reason": "market_cap_below_threshold",
                    "market_cap_yi": float(cap),
                    "min_market_cap_yi": float(cfg.min_market_cap_yi),
                }
                continue
        bundle = feature_map.get(sym) if feature_map else None
        df = bundle.df if bundle is not None else df_map.get(sym)
        if df is None or df.empty:
            rejected[sym] = {"reason": "missing_hist"}
            continue
        amount_series = (
            bundle.amount
            if bundle is not None
            else pd.to_numeric(_sorted_if_needed(df).get("amount"), errors="coerce")
        )
        if isinstance(amount_series, pd.Series):
            avg_amt = amount_series.tail(cfg.amount_avg_window).mean()
            if pd.notna(avg_amt) and avg_amt < cfg.min_avg_amount_wan * 10000:
                rejected[sym] = {
                    "reason": "avg_amount_below_threshold",
                    "avg_amount_wan": round(float(avg_amt) / 10000.0, 2),
                    "min_avg_amount_wan": float(cfg.min_avg_amount_wan),
                    "window": int(cfg.amount_avg_window),
                }
                continue
        passed.append(sym)
    if return_rejections:
        return passed, rejected
    return passed


# Layer 2: 强弱甄别


_MARKET_ALLOWED_TRACKS: dict[str, tuple[str, ...]] = {
    "cn": ("A", "B", "C"),
    "us": ("A", "B"),
    "hk": ("B", "C", "A"),
}

_TRACK_LABELS: dict[str, str] = {
    "A": "主升确认",
    "B": "启动确认",
    "C": "吸筹改善",
}


def _clamp_score(value: float | None, low: float = 0.0, high: float = 100.0) -> float:
    if value is None or pd.isna(value):
        return 0.0
    if high <= low:
        return 0.0
    raw = (float(value) - low) / (high - low) * 100.0
    return max(0.0, min(100.0, raw))


def _score_inverse_ratio(value: float | None, max_value: float) -> float:
    if value is None or pd.isna(value) or max_value <= 0:
        return 0.0
    ratio = max(float(value), 0.0) / max_value
    return max(0.0, min(100.0, (1.0 - ratio) * 100.0))


def _resolve_l2_market(cfg: FunnelConfig) -> str:
    market = str(getattr(cfg, "market_template", "") or getattr(cfg, "profile", "cn"))
    market_norm = _PROFILE_ALIASES.get(_normalize_template_key(market), market)
    return market_norm if market_norm in {"cn", "us", "hk"} else "cn"


def _market_allowed_tracks(cfg: FunnelConfig, market: str) -> tuple[str, ...]:
    allowed = list(_MARKET_ALLOWED_TRACKS.get(market, ("A", "B", "C")))
    if not getattr(cfg, "enable_track_momentum", True):
        allowed = [x for x in allowed if x != "A"]
    if not getattr(cfg, "enable_track_early_strength", True):
        allowed = [x for x in allowed if x != "B"]
    if not getattr(cfg, "enable_track_accumulation", True):
        allowed = [x for x in allowed if x != "C"]
    return tuple(allowed)


def _l2_component_scores(metrics: L2Metrics, cfg: FunnelConfig) -> dict[str, float]:
    ma_alignment_score = 0.0
    if (
        metrics.ma50 is not None
        and metrics.ma200 is not None
        and metrics.close is not None
        and metrics.ma200 > 0
    ):
        ma_gap = (metrics.ma50 - metrics.ma200) / metrics.ma200
        if metrics.ma50 > metrics.ma200 and metrics.close >= metrics.ma50:
            ma_alignment_score = 100.0
        elif abs(ma_gap) <= getattr(cfg, "accum_ma_gap_max", 0.06) and metrics.close >= min(
            metrics.ma50, metrics.ma200
        ):
            ma_alignment_score = 65.0
        elif metrics.close >= metrics.ma50:
            ma_alignment_score = 45.0
        else:
            ma_alignment_score = 20.0

    rps_fast_score = _clamp_score(metrics.rps_fast)
    rps_slow_score = _clamp_score(metrics.rps_slow)
    rs_long_score = _clamp_score(metrics.rs_long, -5.0, 10.0)
    rs_short_score = _clamp_score(metrics.rs_short, -3.0, 6.0)
    rps_slope_score = _clamp_score(metrics.rps_slope, -1.0, 2.0)
    breakout_score = max(
        _clamp_score(metrics.breakout_proximity_20, 80.0, 100.0),
        _clamp_score(metrics.breakout_proximity_60, 75.0, 100.0),
    )
    recent_return_score = max(
        _clamp_score(metrics.ret_5, -5.0, 8.0),
        _clamp_score(metrics.ret_10, -8.0, 12.0),
    )
    volume_expansion_score = _clamp_score(metrics.volume_expansion, 0.8, 1.8)

    old = metrics.old_channels
    accumulation_channel_score = 100.0 if old.get("accum") else 0.0
    dry_volume_score = 100.0 if old.get("dry_vol") else 0.0
    rs_divergence_score = 100.0 if old.get("rs_div") else 0.0
    low_position_score = _score_inverse_ratio(
        metrics.price_from_250d_low, getattr(cfg, "track_c_price_from_low_max", 0.40)
    )
    ma_convergence_score = _score_inverse_ratio(
        abs(metrics.ma_gap_pct) if metrics.ma_gap_pct is not None else None,
        getattr(cfg, "accum_ma_gap_max", 0.06) * 100.0,
    )

    trend_maturity_score = (
        0.30 * ma_alignment_score
        + 0.25 * rps_fast_score
        + 0.20 * rps_slow_score
        + 0.15 * rs_long_score
        + 0.10 * rs_short_score
    )
    early_strength_score = (
        0.30 * rs_short_score
        + 0.25 * rps_slope_score
        + 0.20 * breakout_score
        + 0.15 * recent_return_score
        + 0.10 * volume_expansion_score
    )
    accum_readiness_score = (
        0.30 * accumulation_channel_score
        + 0.25 * dry_volume_score
        + 0.20 * rs_divergence_score
        + 0.15 * low_position_score
        + 0.10 * ma_convergence_score
    )
    return {
        "ma_alignment_score": ma_alignment_score,
        "rps_fast_score": rps_fast_score,
        "rps_slow_score": rps_slow_score,
        "rs_long_score": rs_long_score,
        "rs_short_score": rs_short_score,
        "rps_slope_score": rps_slope_score,
        "breakout_score": breakout_score,
        "recent_return_score": recent_return_score,
        "volume_expansion_score": volume_expansion_score,
        "accum_readiness_score": accum_readiness_score,
        "trend_maturity_score": trend_maturity_score,
        "early_strength_score": early_strength_score,
    }


def score_track_a(metrics: L2Metrics, cfg: FunnelConfig, market: str) -> TrackScoreDetail:
    c = _l2_component_scores(metrics, cfg)
    bias_max = float(getattr(cfg, "track_a_bias_200_max", getattr(cfg, "momentum_bias_200_max", 0.25)))
    required = (
        bool(getattr(cfg, "enable_track_momentum", True))
        and metrics.ma50 is not None
        and metrics.ma200 is not None
        and metrics.close is not None
        and metrics.ma50 > metrics.ma200
        and metrics.close >= metrics.ma50
        and (metrics.rps_fast is not None and metrics.rps_fast >= cfg.track_a_rps_fast_min)
        and (metrics.rps_slow is not None and metrics.rps_slow >= cfg.track_a_rps_slow_min)
        and (metrics.rs_long is None or metrics.rs_long >= cfg.track_a_rs_long_min)
        and (metrics.rps_slope is None or metrics.rps_slope >= cfg.track_a_rps_slope_min)
        and (metrics.bias_200 is None or metrics.bias_200 <= bias_max)
    )
    markup_score = 100.0 if metrics.old_channels.get("momentum") else 0.0
    sos_score = 100.0 if metrics.old_channels.get("sos") else 0.0
    overextended_penalty = 0.0
    if metrics.bias_200 is not None and metrics.bias_200 > bias_max * 0.8:
        overextended_penalty = min(20.0, max(0.0, (metrics.bias_200 / bias_max - 0.8) * 50.0))
    score = (
        0.55 * c["trend_maturity_score"]
        + 0.25 * c["early_strength_score"]
        + 0.10 * markup_score
        + 0.10 * sos_score
        - overextended_penalty
    )
    return TrackScoreDetail(
        track="A",
        required_passed=bool(required),
        score=round(max(0.0, min(100.0, score)), 2),
        reasons={"components": c, "market": market, "required": bool(required)},
        penalties={"overextended": round(overextended_penalty, 2)},
    )


def score_track_b(metrics: L2Metrics, cfg: FunnelConfig, market: str) -> TrackScoreDetail:
    c = _l2_component_scores(metrics, cfg)
    breakout_proximity = max(
        metrics.breakout_proximity_20 or 0.0,
        metrics.breakout_proximity_60 or 0.0,
    )
    close_near_ma = False
    if metrics.close is not None:
        close_near_ma = (
            (metrics.ma20 is not None and metrics.close >= metrics.ma20)
            or (metrics.ma50 is not None and metrics.close >= metrics.ma50)
        )
    required = (
        bool(getattr(cfg, "enable_track_early_strength", True))
        and (metrics.rps_fast is not None and metrics.rps_fast >= cfg.track_b_rps_fast_min)
        and (metrics.rs_short is None or metrics.rs_short > cfg.track_b_rs_short_min)
        and (metrics.rps_slope is None or metrics.rps_slope > 0)
        and close_near_ma
        and breakout_proximity >= cfg.track_b_breakout_proximity_min
    )
    volume_confirmation_score = c["volume_expansion_score"]
    failed_breakout_penalty = 0.0
    if metrics.ret_5 is not None and metrics.ret_5 < -5 and breakout_proximity >= 95:
        failed_breakout_penalty = 12.0
    score = (
        0.35 * c["trend_maturity_score"]
        + 0.45 * c["early_strength_score"]
        + 0.10 * c["breakout_score"]
        + 0.10 * volume_confirmation_score
        - failed_breakout_penalty
    )
    return TrackScoreDetail(
        track="B",
        required_passed=bool(required),
        score=round(max(0.0, min(100.0, score)), 2),
        reasons={"components": c, "market": market, "required": bool(required)},
        penalties={"failed_breakout": round(failed_breakout_penalty, 2)},
    )


def score_track_c(metrics: L2Metrics, cfg: FunnelConfig, market: str) -> TrackScoreDetail:
    c = _l2_component_scores(metrics, cfg)
    old = metrics.old_channels
    has_left_signal = bool(old.get("accum") or old.get("dry_vol") or old.get("rs_div"))
    severe_downtrend = False
    if metrics.close is not None and metrics.ma50 is not None and metrics.ma200 is not None:
        severe_downtrend = (
            metrics.close < metrics.ma50 < metrics.ma200
            and metrics.ret_20 is not None
            and metrics.ret_20 <= cfg.track_c_severe_downtrend_ret20_min
        )
    required = (
        bool(getattr(cfg, "enable_track_accumulation", True))
        and metrics.price_from_250d_low is not None
        and metrics.price_from_250d_low <= cfg.track_c_price_from_low_max
        and has_left_signal
        and not severe_downtrend
    )
    support_resilience_score = 100.0 if old.get("rs_div") else max(0.0, c["rs_short_score"] * 0.5)
    downtrend_continuation_penalty = 18.0 if severe_downtrend else 0.0
    score = (
        0.15 * c["trend_maturity_score"]
        + 0.20 * c["early_strength_score"]
        + 0.55 * c["accum_readiness_score"]
        + 0.10 * support_resilience_score
        - downtrend_continuation_penalty
    )
    return TrackScoreDetail(
        track="C",
        required_passed=bool(required),
        score=round(max(0.0, min(100.0, score)), 2),
        reasons={
            "components": c,
            "market": market,
            "required": bool(required),
            "severe_downtrend": bool(severe_downtrend),
            "has_left_signal": bool(has_left_signal),
        },
        penalties={"downtrend_continuation": round(downtrend_continuation_penalty, 2)},
    )


def select_l2_decision(
    symbol: str,
    metrics: L2Metrics,
    track_details: dict[str, TrackScoreDetail],
    cfg: FunnelConfig,
    market: str,
) -> L2Decision:
    allowed_tracks = _market_allowed_tracks(cfg, market)
    allowed = set(allowed_tracks)
    min_scores = {
        "A": float(getattr(cfg, "track_a_min_score", 70.0)),
        "B": float(getattr(cfg, "track_b_min_score", 62.0)),
        "C": float(getattr(cfg, "track_c_min_score", 58.0)),
    }
    valid = [
        d
        for d in track_details.values()
        if d.track in allowed
        and d.required_passed
        and d.score >= min_scores.get(d.track, 0.0)
    ]
    selected = max(valid, key=lambda d: d.score) if valid else None
    return L2Decision(
        symbol=symbol,
        passed=selected is not None,
        selected_track=selected.track if selected else None,
        selected_score=float(selected.score) if selected else 0.0,
        track_scores={k: float(v.score) for k, v in track_details.items()},
        old_channels=dict(metrics.old_channels),
        reasons={
            "allowed_tracks": allowed_tracks,
            "min_scores": min_scores,
            "track_details": {
                k: {
                    "required_passed": v.required_passed,
                    "score": v.score,
                    "reasons": v.reasons,
                    "penalties": v.penalties,
                }
                for k, v in track_details.items()
            },
        },
    )


def layer2_strength_detailed(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame | None,
    cfg: FunnelConfig,
    *,
    rps_universe: list[str] | None = None,
    feature_map: dict[str, SymbolFeatureBundle] | None = None,
    return_rejections: bool = False,
    return_decisions: bool = False,
) -> (
    tuple[list[str], dict[str, str]]
    | tuple[
        list[str],
        dict[str, str],
        dict[str, dict[str, float | str | bool | None]],
    ]
    | tuple[
        list[str],
        dict[str, str],
        dict[str, dict[str, float | str | bool | None]],
        dict[str, L2Decision],
    ]
):
    """
    Layer2 C-lite 三轨决策：
    1) Track A 主升确认：趋势成熟 + RS/RPS 强势 + 不过度乖离
    2) Track B 启动确认：短期转强 + 接近突破 + 仍有结构余量
    3) Track C 吸筹改善：低位改善 + 吸筹/地量/拒绝新低证据

    返回：
    - passed: 通过 Layer2 的股票
    - channel_map: code -> 唯一主 Track 标签
    """

    def _cum_return_pct_from_series(pct_series: pd.Series) -> float | None:
        s = pd.to_numeric(pct_series, errors="coerce").dropna()
        if s.empty:
            return None
        return float(((s / 100.0 + 1.0).prod() - 1.0) * 100.0)

    def _close_return_pct(close_series: pd.Series, lookback: int) -> float | None:
        s = pd.to_numeric(close_series, errors="coerce").dropna()
        lb = max(int(lookback), 1)
        if len(s) <= lb:
            return None
        start = float(s.iloc[-lb - 1])
        end = float(s.iloc[-1])
        if start == 0:
            return None
        return (end - start) / start * 100.0

    def _calc_rs(
        stock_df: pd.DataFrame, bench_sorted_df: pd.DataFrame
    ) -> tuple[float | None, float | None]:
        stock_p = stock_df[["date", "pct_chg"]].copy()
        bench_p = bench_sorted_df[["date", "pct_chg"]].copy()
        merged = stock_p.merge(bench_p, on="date", how="inner", suffixes=("_s", "_b"))
        if merged.empty:
            return (None, None)
        w_long = max(int(cfg.rs_window_long), 1)
        w_short = max(int(cfg.rs_window_short), 1)
        if len(merged) < max(w_long, w_short):
            return (None, None)
        s_long = _cum_return_pct_from_series(merged["pct_chg_s"].tail(w_long))
        b_long = _cum_return_pct_from_series(merged["pct_chg_b"].tail(w_long))
        s_short = _cum_return_pct_from_series(merged["pct_chg_s"].tail(w_short))
        b_short = _cum_return_pct_from_series(merged["pct_chg_b"].tail(w_short))
        if s_long is None or b_long is None or s_short is None or b_short is None:
            return (None, None)
        return (s_long - b_long, s_short - b_short)

    bench_dropping = False
    bench_sorted: pd.DataFrame | None = None
    bench_latest_date = None
    market = _resolve_l2_market(cfg)
    if bench_df is not None and not bench_df.empty:
        bench_sorted = _sorted_if_needed(bench_df)
        bench_latest_date = _latest_trade_date(bench_sorted)
        if len(bench_sorted) >= cfg.bench_drop_days:
            recent_bench = bench_sorted.tail(cfg.bench_drop_days)
            bench_cum = (recent_bench["pct_chg"].dropna() / 100.0 + 1).prod() - 1
            bench_dropping = bench_cum * 100 <= cfg.bench_drop_threshold

    # 截面强弱：RPS50 / RPS120（欧奈尔思路）
    # 使用全市场 universe 排名（如有），避免仅在 L1 子集内排名导致 RPS 偏高
    rps_fast_map: dict[str, float] = {}
    rps_slow_map: dict[str, float] = {}
    rps_filter_active = False
    _rps_pool = rps_universe if rps_universe else symbols
    if cfg.enable_rps_filter and _rps_pool:
        rows: list[tuple[str, float, float]] = []
        for sym in _rps_pool:
            bundle = feature_map.get(sym) if feature_map else None
            df = bundle.df if bundle is not None else df_map.get(sym)
            if df is None or df.empty:
                continue
            close = (
                bundle.close
                if bundle is not None
                else pd.to_numeric(_sorted_if_needed(df).get("close"), errors="coerce")
            )
            ret_fast = _close_return_pct(close, cfg.rps_window_fast)
            ret_slow = _close_return_pct(close, cfg.rps_window_slow)
            if ret_fast is None or ret_slow is None:
                continue
            rows.append((sym, ret_fast, ret_slow))
        if rows:
            rps_df = pd.DataFrame(rows, columns=["sym", "ret_fast", "ret_slow"])
            rps_df["rps_fast"] = (
                rps_df["ret_fast"].rank(pct=True, ascending=True, method="average")
                * 100.0
            )
            rps_df["rps_slow"] = (
                rps_df["ret_slow"].rank(pct=True, ascending=True, method="average")
                * 100.0
            )
            rps_fast_map = rps_df.set_index("sym")["rps_fast"].astype(float).to_dict()
            rps_slow_map = rps_df.set_index("sym")["rps_slow"].astype(float).to_dict()
            rps_filter_active = True

    passed: list[str] = []
    channel_map: dict[str, str] = {}
    rejected: dict[str, dict[str, float | str | bool | None]] = {}
    decision_map: dict[str, L2Decision] = {}
    for sym in symbols:
        bundle = feature_map.get(sym) if feature_map else None
        df = bundle.df if bundle is not None else df_map.get(sym)
        if df is None or len(df) < cfg.ma_long:
            rejected[sym] = {
                "reason": "insufficient_history",
                "rows": int(len(df)) if df is not None else 0,
                "min_rows": int(cfg.ma_long),
            }
            continue
        df_sorted = bundle.df if bundle is not None else _sorted_if_needed(df)
        if (
            cfg.require_bench_latest_alignment
            and bench_latest_date is not None
            and _latest_trade_date(df_sorted) != bench_latest_date
        ):
            rejected[sym] = {
                "reason": "bench_latest_misaligned",
                "stock_latest_date": str(_latest_trade_date(df_sorted)),
                "bench_latest_date": str(bench_latest_date),
            }
            continue
        close = bundle.close if bundle is not None else df_sorted["close"].astype(float)
        ma_short = (
            bundle.ma_short
            if bundle is not None
            else close.rolling(cfg.ma_short).mean()
        )
        ma_long = (
            bundle.ma_long if bundle is not None else close.rolling(cfg.ma_long).mean()
        )
        last_ma_short = ma_short.iloc[-1]
        last_ma_long = ma_long.iloc[-1]
        last_close = close.iloc[-1]

        bullish_alignment = (
            pd.notna(last_ma_short)
            and pd.notna(last_ma_long)
            and last_ma_short > last_ma_long
        )

        holding_ma20 = False
        if bench_dropping:
            ma_hold = (
                bundle.ma_hold
                if bundle is not None
                else close.rolling(cfg.ma_hold).mean()
            )
            last_ma_hold = ma_hold.iloc[-1]
            if pd.notna(last_ma_hold) and last_close >= last_ma_hold:
                holding_ma20 = True

        momentum_rs_ok = True
        ambush_rs_ok = True
        rs_long = None
        rs_short = None
        if cfg.enable_rs_filter and bench_sorted is not None and not bench_sorted.empty:
            rs_long, rs_short = _calc_rs(df_sorted, bench_sorted)
            if rs_long is None or rs_short is None:
                momentum_rs_ok = False
                ambush_rs_ok = False
            else:
                momentum_rs_ok = (
                    rs_long >= cfg.rs_min_long and rs_short >= cfg.rs_min_short
                )
                ambush_rs_ok = (
                    rs_long >= cfg.ambush_rs_long_min
                    and rs_short >= cfg.ambush_rs_short_min
                )

        rps_fast = rps_fast_map.get(sym)
        rps_slow = rps_slow_map.get(sym)
        momentum_rps_ok = True
        ambush_rps_ok = True

        # 计算 RPS 斜率：判断 RPS 是否还在上升
        rps_slope = None
        rps_slope_ok = True
        if (
            cfg.enable_rps_filter
            and rps_filter_active
            and len(df_sorted) >= cfg.rps_slope_window
        ):
            close_series = pd.to_numeric(df_sorted["close"], errors="coerce")
            rps_window = max(int(cfg.rps_slope_window), 2)

            # 修正：计算最近 N 日的累计收益率曲线（相对起点），而不是单日收益率
            recent_closes = []
            for i in range(-rps_window, 0):
                if len(close_series) + i >= 0:
                    recent_closes.append(float(close_series.iloc[i]))

            # 线性回归斜率：判断累计涨幅曲线是否在爬升
            if len(recent_closes) >= 2:
                import numpy as np

                base_price = recent_closes[0]
                if base_price > 0:
                    cum_returns = [
                        (p - base_price) / base_price * 100.0 for p in recent_closes
                    ]
                    x = np.arange(len(cum_returns))
                    y = np.array(cum_returns)
                    slope = np.polyfit(x, y, 1)[0]
                    rps_slope = float(slope)
                    rps_slope_ok = slope >= cfg.rps_slope_min

        if cfg.enable_rps_filter and rps_filter_active:
            momentum_rps_ok = (
                rps_fast is not None
                and rps_slow is not None
                and rps_fast >= cfg.rps_fast_min
                and rps_slow >= cfg.rps_slow_min
                and rps_slope_ok  # 加入 RPS 斜率判断
            )
            ambush_rps_ok = (
                rps_fast is not None
                and rps_slow is not None
                and rps_fast <= cfg.ambush_rps_fast_max
                and rps_slow >= cfg.ambush_rps_slow_min
            )

        momentum_bias_ok = True
        bias_200 = None
        if pd.notna(last_ma_long) and float(last_ma_long) > 0 and pd.notna(last_close):
            bias_200 = (float(last_close) - float(last_ma_long)) / float(last_ma_long)
            momentum_bias_ok = bias_200 <= getattr(cfg, "momentum_bias_200_max", 0.25)

        momentum_ok = (
            (bullish_alignment or holding_ma20)
            and momentum_rs_ok
            and momentum_rps_ok
            and momentum_bias_ok
        )

        ambush_shape_ok = False
        if (
            cfg.enable_ambush_channel
            and pd.notna(last_ma_long)
            and float(last_ma_long) > 0
            and pd.notna(last_close)
        ):
            bias_200 = (float(last_close) - float(last_ma_long)) / float(last_ma_long)
            ret20 = _close_return_pct(close, 20)
            ambush_shape_ok = (
                abs(bias_200) <= cfg.ambush_bias_200_abs_max
                and ret20 is not None
                and ret20 <= cfg.ambush_ret20_max
            )
        ambush_ok = (
            cfg.enable_ambush_channel
            and ambush_shape_ok
            and ambush_rs_ok
            and ambush_rps_ok
        )

        # 低位吸筹通道（Wyckoff Accumulation Channel）
        # 四个条件逐一检测，全通才标记。不依赖 RPS 排名。
        accum_ok = False
        if cfg.enable_accumulation_channel and len(df_sorted) >= max(
            cfg.accum_lookback_days, cfg.accum_vol_dry_ref_window
        ):
            _c = close  # alias，避免遮蔽外层

            # 条件 1：低位区——现价在年内低点 +X% 以内
            lookback_w = max(int(cfg.accum_lookback_days), 2)
            period_low = float(_c.tail(lookback_w).min())
            accum_low_ok = period_low > 0 and float(last_close) <= period_low * (
                1.0 + cfg.accum_price_from_low_max
            )

            # 条件 2：横盘振幅——近 N 日 high/low 振幅不超过阈值
            accum_range_ok = False
            if accum_low_ok:
                rw = max(int(cfg.accum_range_window), 5)
                zone = df_sorted.tail(rw)
                _high = pd.to_numeric(zone.get("high"), errors="coerce")
                _low = pd.to_numeric(zone.get("low"), errors="coerce")
                if not _high.dropna().empty and not _low.dropna().empty:
                    h_max = float(_high.max())
                    l_min = float(_low.min())
                    if l_min > 0:
                        range_pct = (h_max - l_min) / l_min * 100.0
                        accum_range_ok = range_pct <= cfg.accum_range_max_pct

            # 条件 3：量能萎缩——近 N 日均量 / 参考均量 < 阈值
            accum_vol_ok = False
            if accum_range_ok:
                vol = (
                    bundle.volume
                    if bundle is not None
                    else pd.to_numeric(df_sorted.get("volume"), errors="coerce")
                )
                dw = max(int(cfg.accum_vol_dry_window), 2)
                rfw = max(int(cfg.accum_vol_dry_ref_window), dw + 1)
                recent_vol_mean = float(vol.tail(dw).mean()) if len(vol) >= dw else None
                ref_vol_mean = (
                    float(vol.tail(rfw).iloc[:-dw].mean()) if len(vol) >= rfw else None
                )
                if (
                    recent_vol_mean is not None
                    and ref_vol_mean is not None
                    and ref_vol_mean > 0
                ):
                    accum_vol_ok = (
                        recent_vol_mean / ref_vol_mean
                    ) < cfg.accum_vol_dry_ratio

            # 条件 4：均线即将穿越——MA50 即将穿过或刚穿过 MA200（吸筹完成信号）
            accum_ma_ok = False
            if accum_vol_ok:
                if (
                    pd.notna(last_ma_short)
                    and pd.notna(last_ma_long)
                    and float(last_ma_long) > 0
                ):
                    # MA50 与 MA200 的差距百分比：允许在 ±accum_ma_gap_max 之间
                    # 即 MA50 可以在 MA200 下方 N% 以内（即将穿），或在上方 N% 以内（刚穿）
                    ma_gap_pct = (
                        (float(last_ma_short) - float(last_ma_long))
                        / float(last_ma_long)
                        * 100.0
                    )
                    ma_gap_limit = (
                        cfg.accum_ma_gap_max * 100.0
                    )  # 配置值为小数（如 0.06 → 6%）
                    accum_ma_ok = -ma_gap_limit <= ma_gap_pct <= ma_gap_limit

            accum_ok = accum_low_ok and accum_range_ok and accum_vol_ok and accum_ma_ok

        # 地量蓄势通道（Dry Volume Channel）
        # 低位区 + 近 N 日内出现了年内最低级别的单日成交量 → 卖压完全枯竭
        dry_vol_ok = False
        if cfg.enable_dry_vol_channel and len(df_sorted) >= cfg.dry_vol_ref_window:
            vol = (
                bundle.volume
                if bundle is not None
                else pd.to_numeric(df_sorted.get("volume"), errors="coerce")
            )
            _c_dv = close
            lookback_dv = max(int(cfg.dry_vol_ref_window), 2)
            period_low_dv = float(_c_dv.tail(lookback_dv).min())
            if period_low_dv > 0 and float(last_close) <= period_low_dv * (
                1.0 + cfg.dry_vol_price_from_low_max
            ):
                ref_vol = vol.tail(lookback_dv)
                if len(ref_vol.dropna()) >= 50:
                    vol_threshold = float(
                        np.quantile(ref_vol.dropna().values, cfg.dry_vol_quantile)
                    )
                    recent_vol = vol.tail(cfg.dry_vol_lookback)
                    if float(recent_vol.min()) <= vol_threshold:
                        dry_vol_ok = True

        # 暗中护盘通道（RS Divergence Channel）
        # 大盘近期在更大窗口内创了新低，但个股同期拒绝创新低（Higher Low）
        # 加入成交量确认：大盘创新低时成交量放大，个股拒绝创新低时成交量缩小
        rs_div_ok = False
        if (
            cfg.enable_rs_divergence_channel
            and bench_sorted is not None
            and not bench_sorted.empty
            and len(df_sorted) >= cfg.rs_div_bench_ref_window
        ):
            bench_close = pd.to_numeric(bench_sorted.get("close"), errors="coerce")
            if len(bench_close.dropna()) >= cfg.rs_div_bench_ref_window:
                # 位阶保护
                _c_rd = close
                lookback_rd = max(int(cfg.dry_vol_ref_window), 250)
                period_low_rd = float(_c_rd.tail(min(lookback_rd, len(_c_rd))).min())
                if period_low_rd > 0 and float(last_close) <= period_low_rd * (
                    1.0 + cfg.rs_div_price_from_low_max
                ):
                    # 大盘：近 N 日的最低收盘价 < 前 ref_window 日的最低收盘价（创新低）
                    bench_recent = bench_close.tail(cfg.rs_div_bench_window)
                    bench_ref = bench_close.tail(cfg.rs_div_bench_ref_window).iloc[
                        : -cfg.rs_div_bench_window
                    ]
                    if not bench_ref.dropna().empty and not bench_recent.dropna().empty:
                        bench_recent_low = float(bench_recent.min())
                        bench_ref_low = float(bench_ref.min())
                        bench_made_lower_low = bench_recent_low < bench_ref_low

                        if bench_made_lower_low:
                            # 个股：近 N 日的最低收盘价 >= 前 ref_window 日的最低收盘价（Higher Low）
                            stock_low_col = pd.to_numeric(
                                df_sorted.get("low"), errors="coerce"
                            )
                            stock_recent = stock_low_col.tail(cfg.rs_div_stock_window)
                            stock_ref = stock_low_col.tail(
                                cfg.rs_div_bench_ref_window
                            ).iloc[: -cfg.rs_div_stock_window]
                            if (
                                not stock_ref.dropna().empty
                                and not stock_recent.dropna().empty
                            ):
                                stock_recent_low = float(stock_recent.min())
                                stock_ref_low = float(stock_ref.min())
                                if stock_recent_low >= stock_ref_low:
                                    # 加入成交量确认
                                    bench_vol = pd.to_numeric(
                                        bench_sorted.get("volume"), errors="coerce"
                                    )
                                    stock_vol = (
                                        bundle.volume
                                        if bundle is not None
                                        else pd.to_numeric(
                                            df_sorted.get("volume"), errors="coerce"
                                        )
                                    )

                                    vol_confirm_ok = True
                                    if not bench_vol.empty and not stock_vol.empty:
                                        bench_recent_vol = bench_vol.tail(
                                            cfg.rs_div_bench_window
                                        ).mean()
                                        bench_ref_vol = (
                                            bench_vol.tail(cfg.rs_div_bench_ref_window)
                                            .iloc[: -cfg.rs_div_bench_window]
                                            .mean()
                                        )
                                        stock_recent_vol = stock_vol.tail(
                                            cfg.rs_div_stock_window
                                        ).mean()
                                        stock_ref_vol = (
                                            stock_vol.tail(cfg.rs_div_bench_ref_window)
                                            .iloc[: -cfg.rs_div_stock_window]
                                            .mean()
                                        )

                                        if bench_ref_vol > 0 and stock_ref_vol > 0:
                                            # 大盘创新低时成交量放大，个股拒绝创新低时成交量缩小
                                            bench_vol_expand = (
                                                bench_recent_vol > bench_ref_vol * 1.2
                                            )
                                            stock_vol_shrink = (
                                                stock_recent_vol < stock_ref_vol * 0.8
                                            )
                                            vol_confirm_ok = (
                                                bench_vol_expand and stock_vol_shrink
                                            )

                                    if vol_confirm_ok:
                                        rs_div_ok = True

        # 点火破局通道（SOS Bypass）
        # 如果当天爆发了放量大阳线，哪怕它此前 RPS 很低或者量能没萎缩，也直接送入 L4 让扳机去二次确认
        sos_ok = False
        if hasattr(cfg, "sos_vol_ratio"):
            sos_score = _detect_sos(df_sorted, cfg)
            if sos_score is not None:
                sos_ok = True

        def _latest_ma_value(series: pd.Series) -> float | None:
            try:
                val = series.iloc[-1]
                return float(val) if pd.notna(val) else None
            except Exception:
                return None

        def _breakout_proximity(window: int) -> float | None:
            if len(df_sorted) < max(int(window), 2):
                return None
            high_series = pd.to_numeric(df_sorted.get("high"), errors="coerce")
            recent_high = high_series.tail(int(window)).max()
            if pd.isna(recent_high) or float(recent_high) <= 0 or pd.isna(last_close):
                return None
            return max(0.0, min(100.0, float(last_close) / float(recent_high) * 100.0))

        ma_hold = bundle.ma_hold if bundle is not None else close.rolling(cfg.ma_hold).mean()
        last_ma_hold = _latest_ma_value(ma_hold)
        ret_5 = _close_return_pct(close, 5)
        ret_10 = _close_return_pct(close, 10)
        ret_20 = _close_return_pct(close, 20)

        volume_expansion = None
        dry_volume_ratio = None
        vol = (
            bundle.volume
            if bundle is not None
            else pd.to_numeric(df_sorted.get("volume"), errors="coerce")
        )
        if isinstance(vol, pd.Series) and len(vol) >= 40:
            recent_vol_mean = float(vol.tail(5).mean())
            ref_vol_mean = float(vol.tail(40).iloc[:-5].mean())
            if ref_vol_mean > 0:
                volume_expansion = recent_vol_mean / ref_vol_mean
        if isinstance(vol, pd.Series) and len(vol) >= max(cfg.accum_vol_dry_ref_window, cfg.accum_vol_dry_window + 1):
            dw = max(int(cfg.accum_vol_dry_window), 2)
            rfw = max(int(cfg.accum_vol_dry_ref_window), dw + 1)
            recent_dry_mean = float(vol.tail(dw).mean())
            ref_dry_mean = float(vol.tail(rfw).iloc[:-dw].mean())
            if ref_dry_mean > 0:
                dry_volume_ratio = recent_dry_mean / ref_dry_mean

        price_from_250d_low = None
        lookback_low = min(max(int(cfg.accum_lookback_days), 2), len(close))
        period_low_for_metrics = float(close.tail(lookback_low).min())
        if period_low_for_metrics > 0 and pd.notna(last_close):
            price_from_250d_low = float(last_close) / period_low_for_metrics - 1.0

        range_60_pct = None
        if len(df_sorted) >= 60:
            range_zone = df_sorted.tail(60)
            range_high = pd.to_numeric(range_zone.get("high"), errors="coerce").max()
            range_low = pd.to_numeric(range_zone.get("low"), errors="coerce").min()
            if pd.notna(range_high) and pd.notna(range_low) and float(range_low) > 0:
                range_60_pct = (float(range_high) - float(range_low)) / float(range_low) * 100.0

        ma_gap_pct = None
        if pd.notna(last_ma_short) and pd.notna(last_ma_long) and float(last_ma_long) > 0:
            ma_gap_pct = (float(last_ma_short) - float(last_ma_long)) / float(last_ma_long) * 100.0

        metrics = L2Metrics(
            symbol=sym,
            market=market,
            close=round(float(last_close), 4) if pd.notna(last_close) else None,
            ma20=last_ma_hold,
            ma50=round(float(last_ma_short), 4) if pd.notna(last_ma_short) else None,
            ma200=round(float(last_ma_long), 4) if pd.notna(last_ma_long) else None,
            bias_200=float(bias_200)
            if bias_200 is not None and pd.notna(bias_200)
            else None,
            rs_long=float(rs_long) if rs_long is not None else None,
            rs_short=float(rs_short) if rs_short is not None else None,
            rps_fast=float(rps_fast) if rps_fast is not None else None,
            rps_slow=float(rps_slow) if rps_slow is not None else None,
            rps_slope=rps_slope,
            ret_5=ret_5,
            ret_10=ret_10,
            ret_20=ret_20,
            breakout_proximity_20=_breakout_proximity(20),
            breakout_proximity_60=_breakout_proximity(60),
            volume_expansion=volume_expansion,
            price_from_250d_low=price_from_250d_low,
            range_60_pct=range_60_pct,
            dry_volume_ratio=dry_volume_ratio,
            ma_gap_pct=ma_gap_pct,
            old_channels={
                "momentum": bool(momentum_ok),
                "ambush": bool(ambush_ok),
                "accum": bool(accum_ok),
                "dry_vol": bool(dry_vol_ok),
                "rs_div": bool(rs_div_ok),
                "sos": bool(sos_ok),
            },
        )
        track_details = {
            "A": score_track_a(metrics, cfg, market),
            "B": score_track_b(metrics, cfg, market),
            "C": score_track_c(metrics, cfg, market),
        }
        decision = select_l2_decision(sym, metrics, track_details, cfg, market)
        decision_map[sym] = decision

        if decision.passed and decision.selected_track:
            passed.append(sym)
            channel_map[sym] = _TRACK_LABELS.get(decision.selected_track, decision.selected_track)
        else:
            rejection_reason = "no_track_passed"
            if not any(metrics.old_channels.values()):
                rejection_reason = "no_channel_evidence"
            elif not bullish_alignment and not holding_ma20 and not (
                metrics.old_channels.get("accum")
                or metrics.old_channels.get("dry_vol")
                or metrics.old_channels.get("rs_div")
            ):
                rejection_reason = "trend_alignment_failed"
            elif cfg.enable_rs_filter and not momentum_rs_ok and not ambush_rs_ok:
                rejection_reason = "rs_filter_failed"
            elif (
                cfg.enable_rps_filter
                and rps_filter_active
                and not momentum_rps_ok
                and not ambush_rps_ok
            ):
                rejection_reason = "rps_filter_failed"
            elif not momentum_bias_ok:
                rejection_reason = "momentum_bias_too_high"

            rejected[sym] = {
                "reason": rejection_reason,
                "bullish_alignment": bool(bullish_alignment),
                "holding_ma20": bool(holding_ma20),
                "momentum_rs_ok": bool(momentum_rs_ok),
                "ambush_rs_ok": bool(ambush_rs_ok),
                "momentum_rps_ok": bool(momentum_rps_ok),
                "ambush_rps_ok": bool(ambush_rps_ok),
                "momentum_bias_ok": bool(momentum_bias_ok),
                "rps_filter_active": bool(rps_filter_active),
                "bench_dropping": bool(bench_dropping),
                "rps_fast": round(float(rps_fast), 2) if rps_fast is not None else None,
                "rps_slow": round(float(rps_slow), 2) if rps_slow is not None else None,
                "rs_long": round(float(rs_long), 2) if rs_long is not None else None,
                "rs_short": round(float(rs_short), 2) if rs_short is not None else None,
                "last_close": round(float(last_close), 4)
                if pd.notna(last_close)
                else None,
                "last_ma_short": round(float(last_ma_short), 4)
                if pd.notna(last_ma_short)
                else None,
                "last_ma_long": round(float(last_ma_long), 4)
                if pd.notna(last_ma_long)
                else None,
                "selected_track": decision.selected_track,
                "selected_score": decision.selected_score,
                "track_scores": decision.track_scores,
                "candidate_channels": metrics.old_channels,
                "layer2_decision": decision.reasons,
            }
    if return_rejections and return_decisions:
        return passed, channel_map, rejected, decision_map
    if return_rejections:
        return passed, channel_map, rejected
    if return_decisions:
        return passed, channel_map, {}, decision_map
    return passed, channel_map


# Layer 3: 板块共振


def layer3_sector_resonance(
    symbols: list[str],
    sector_map: dict[str, str],
    cfg: FunnelConfig,
    base_symbols: list[str] | None = None,
    df_map: dict[str, pd.DataFrame] | None = None,
) -> tuple[list[str], list[str]]:
    """
    统计行业分布，做“行业通过率 + 行业强度中位数”分析：
    - 行业通过率 = L2行业样本数 / 基准池(L1)行业样本数
    - 行业强度 = 行业内个股短中期动量分数中位数
    注：为了避免错杀刚刚启动的威科夫吸筹/潜伏标的，
    本层不再对股票做硬性剔除，仅进行 Top 行业计算以供后续打分和标识使用。
    返回 (原始输入 symbols 列表, top_sectors 强势行业列表)。
    """
    if base_symbols is None:
        base_symbols = symbols

    counts: dict[str, int] = {}
    for sym in symbols:
        sector = sector_map.get(sym, "")
        if sector:
            counts[sector] = counts.get(sector, 0) + 1

    if not counts:
        return symbols, []

    base_counts: dict[str, int] = {}
    for sym in base_symbols:
        sector = sector_map.get(sym, "")
        if sector:
            base_counts[sector] = base_counts.get(sector, 0) + 1

    # 个股强度：20日收益(40%) + 5日收益(30%) + 3日收益(30%) 的截面百分位分数
    # 加入 3 日动量以适配 A 股板块快速轮动（"一日游"）特征。
    strength_map: dict[str, float] = {}
    if df_map:
        rows: list[tuple[str, float, float, float]] = []
        for sym in symbols:
            df = df_map.get(sym)
            if df is None or df.empty:
                continue
            s = _sorted_if_needed(df)
            close = pd.to_numeric(s.get("close"), errors="coerce").dropna()
            if len(close) <= 20:
                continue
            ret20 = (
                (float(close.iloc[-1]) - float(close.iloc[-21]))
                / float(close.iloc[-21])
                * 100.0
            )
            ret5 = (
                (float(close.iloc[-1]) - float(close.iloc[-6]))
                / float(close.iloc[-6])
                * 100.0
                if len(close) > 5
                else ret20
            )
            ret3 = (
                (float(close.iloc[-1]) - float(close.iloc[-4]))
                / float(close.iloc[-4])
                * 100.0
                if len(close) > 3
                else ret5
            )
            rows.append((sym, ret20, ret5, ret3))
        if rows:
            st_df = pd.DataFrame(rows, columns=["sym", "ret20", "ret5", "ret3"])
            st_df["q20"] = st_df["ret20"].rank(
                pct=True, ascending=True, method="average"
            )
            st_df["q5"] = st_df["ret5"].rank(pct=True, ascending=True, method="average")
            st_df["q3"] = st_df["ret3"].rank(pct=True, ascending=True, method="average")
            st_df["strength"] = (
                0.4 * st_df["q20"] + 0.3 * st_df["q5"] + 0.3 * st_df["q3"]
            )
            strength_map = st_df.set_index("sym")["strength"].astype(float).to_dict()

    ranked = sorted(counts.items(), key=lambda x: -x[1])
    min_count = max(int(cfg.sector_min_count), 1)
    q = float(cfg.sector_count_quantile)
    q = min(max(q, 0.0), 1.0)
    size_arr = np.array(list(counts.values()), dtype=float)
    q_count = int(np.ceil(np.quantile(size_arr, q))) if size_arr.size > 0 else min_count
    threshold = max(min_count, q_count)

    # 行业通过率阈值（动态）：按行业通过率分位数（默认与 sector_count_quantile 同步）
    pass_ratios: list[float] = []
    pass_ratio_map: dict[str, float] = {}
    for sec, cnt in ranked:
        base_cnt = max(int(base_counts.get(sec, 0)), 1)
        ratio = float(cnt) / float(base_cnt)
        pass_ratio_map[sec] = ratio
        pass_ratios.append(ratio)
    pass_threshold = (
        float(np.quantile(np.array(pass_ratios, dtype=float), q))
        if pass_ratios
        else 0.0
    )

    # 行业强度阈值（动态）：行业内强度中位数分位阈值
    sector_strength_map: dict[str, float] = {}
    for sec, _ in ranked:
        vals = [
            strength_map.get(sym)
            for sym in symbols
            if sector_map.get(sym, "") == sec and sym in strength_map
        ]
        vals = [float(v) for v in vals if v is not None]
        sector_strength_map[sec] = float(np.median(vals)) if vals else 0.0
    strength_vals = list(sector_strength_map.values())
    strength_threshold = (
        float(np.quantile(np.array(strength_vals, dtype=float), q))
        if strength_vals
        else 0.0
    )

    # 小而强板块免死阈值：强度进 Top X% 时放宽数量门槛，防止概念主线被大行业吞没。
    super_q = float(getattr(cfg, "sector_super_strength_quantile", 0.90))
    super_q = min(max(super_q, 0.0), 1.0)
    super_strength_threshold = (
        float(np.quantile(np.array(strength_vals, dtype=float), super_q))
        if strength_vals
        else 0.0
    )

    keep_sectors: list[str] = []
    for s, c in ranked:
        pass_r = pass_ratio_map.get(s, 0.0)
        str_val = sector_strength_map.get(s, 0.0)
        normal_pass = (
            c >= threshold
            and pass_r >= pass_threshold
            and str_val >= strength_threshold
        )
        super_pass = c >= min_count and str_val >= super_strength_threshold
        if normal_pass or super_pass:
            keep_sectors.append(s)
    if not keep_sectors:
        # 极端场景兜底：至少保留样本最多的行业，避免空集。
        max_count = int(size_arr.max()) if size_arr.size > 0 else 0
        keep_sectors = [s for s, c in ranked if c == max_count]

    # Top 行业按强度排序展示，而非按数量排序，提升“主线识别”灵敏度。
    keep_sectors_sorted = sorted(
        keep_sectors,
        key=lambda s: (
            -sector_strength_map.get(s, 0.0),
            -pass_ratio_map.get(s, 0.0),
            -counts.get(s, 0),
            s,
        ),
    )
    top_n = max(int(cfg.top_n_sectors), 0)
    top_sectors = keep_sectors_sorted[:top_n] if top_n > 0 else keep_sectors_sorted

    # L3 板块共振过滤：保留 Top 行业内的股票 + 强势个股通配。
    # 三级放行机制：核心热门板块直通 → 次优板块需个股强度 ≥60% → 强势个股(Top20%)无视板块。
    # 门槛已放宽以适配 A 股板块快速轮动特征，减少好股票因板块切换被误杀。
    top_sector_set = set(top_sectors)
    keep_sector_set = set(keep_sectors_sorted)
    filtered: list[str] = []
    for sym in symbols:
        sector = sector_map.get(sym, "")
        sym_strength = strength_map.get(sym, 0.0)
        if sector in top_sector_set:
            # 核心热门板块：直接保留
            filtered.append(sym)
        elif sector in keep_sector_set and sym_strength >= 0.60:
            # 次优板块 + 个股强度 60%+：有条件保留
            filtered.append(sym)
        elif sym_strength >= 0.80:
            # 强势个股通配：无论板块，个股强度 Top 20% 可绕过
            filtered.append(sym)

    # 安全兜底：避免极端行情下池子被清空
    if len(filtered) < 3:
        filtered = list(symbols)

    return filtered, top_sectors


# Layer 4: 威科夫狙击


def _is_trading_range_context(
    zone: pd.DataFrame, cfg: FunnelConfig, df_full: pd.DataFrame = None
) -> bool:
    """
    Spring 必须先发生在可接受的交易区间（TR）内。
    使用 ATR_pct 动态计算可接受的合理振幅。
    """
    if zone is None or zone.empty:
        return False
    high = pd.to_numeric(zone.get("high"), errors="coerce")
    low = pd.to_numeric(zone.get("low"), errors="coerce")
    close = pd.to_numeric(zone.get("close"), errors="coerce")
    if high.isna().all() or low.isna().all() or close.isna().all():
        return False

    high_max = float(high.max())
    low_min = float(low.min())
    if low_min <= 0:
        return False
    range_pct = (high_max - low_min) / low_min * 100.0

    # --- 动态 ATR 振幅阈值计算 ---
    max_allowed_range_pct = cfg.spring_tr_max_range_pct  # 兜底默认值 30.0
    if df_full is not None and len(df_full) > getattr(cfg, "spring_tr_atr_window", 20):
        h = pd.to_numeric(df_full["high"], errors="coerce")
        l = pd.to_numeric(df_full["low"], errors="coerce")
        c = pd.to_numeric(df_full["close"], errors="coerce")
        prev_c = c.shift(1)

        # 真实波动幅度 True Range
        tr1 = h - l
        tr2 = (h - prev_c).abs()
        tr3 = (l - prev_c).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(getattr(cfg, "spring_tr_atr_window", 20)).mean()

        last_atr = float(atr.iloc[-1])
        last_c = float(c.iloc[-1])
        if pd.notna(last_atr) and pd.notna(last_c) and last_c > 0:
            atr_pct = (last_atr / last_c) * 100.0
            max_allowed_range_pct = atr_pct * getattr(
                cfg, "spring_tr_atr_max_multiple", 4.0
            )
            # 放松动态振幅：不再死板卡 15~45，而是最低保底为原始配置（通常是30%），最高可达 60%，给大蓝筹和大盘股透气
            max_allowed_range_pct = min(
                max(max_allowed_range_pct, float(cfg.spring_tr_max_range_pct)), 60.0
            )

    if range_pct > max_allowed_range_pct:
        return False

    c_start = float(close.iloc[0])
    c_end = float(close.iloc[-1])
    if c_start <= 0:
        return False
    drift_pct = abs((c_end - c_start) / c_start * 100.0)
    if drift_pct > cfg.spring_tr_max_drift_pct:
        return False
    return True


def _detect_spring(
    df: pd.DataFrame,
    cfg: FunnelConfig,
    *,
    bundle: SymbolFeatureBundle | None = None,
) -> float | None:
    """
    Spring（终极震仓）：允许“前一日或当日盘中”跌破近 N 日支撑位，且当日收盘收回并放量。
    返回 score（收回幅度%）或 None。
    """
    if len(df) < cfg.spring_support_window + 2:
        return None
    df_s = bundle.df if bundle is not None else _sorted_if_needed(df)
    # 修正：支撑位不能包含正在进行跌破测试的前一日（prev）
    support_zone = df_s.iloc[-(cfg.spring_support_window + 2) : -2]
    # 调用时把历史前序 df_full 传进去计算 ATR
    if not _is_trading_range_context(support_zone, cfg, df_full=df_s.iloc[:-2]):
        return None
    support_level = support_zone["close"].min()
    prev = df_s.iloc[-2]
    last = df_s.iloc[-1]

    # 允许单日盘中洗盘（长下影锤子线）：只要 prev/last 至少一日跌破即可。
    if (prev["low"] >= support_level) and (last["low"] >= support_level):
        return None
    if last["close"] <= support_level:
        return None
    vol_avg = df_s["volume"].tail(5).iloc[:-1].mean()
    if vol_avg <= 0 or last["volume"] < vol_avg * cfg.spring_vol_ratio:
        return None
    # 加入放量确认：收回时的成交量 > 下探时的成交量
    prev_vol = float(prev["volume"]) if pd.notna(prev["volume"]) else 0
    last_vol = float(last["volume"]) if pd.notna(last["volume"]) else 0
    if prev_vol > 0 and last_vol / prev_vol < cfg.spring_vol_expand_ratio:
        return None

    recovery = (last["close"] - support_level) / support_level * 100
    return float(recovery)


def _detect_lps(
    df: pd.DataFrame,
    cfg: FunnelConfig,
    *,
    bundle: SymbolFeatureBundle | None = None,
) -> float | None:
    """
    LPS（最后支撑点缩量）：近 N 日回踩 MA20 且缩量。
    返回 score（缩量比）或 None。
    """
    if len(df) < max(cfg.lps_vol_ref_window, cfg.lps_ma) + cfg.lps_lookback:
        return None
    df_s = bundle.df if bundle is not None else _sorted_if_needed(df)
    close = (
        bundle.close
        if bundle is not None
        else pd.to_numeric(df_s["close"], errors="coerce")
    )
    ma = (
        bundle.ma_hold
        if bundle is not None and int(cfg.lps_ma) == int(cfg.ma_hold)
        else close.rolling(cfg.lps_ma).mean()
    )
    last_ma = ma.iloc[-1]
    if pd.isna(last_ma) or last_ma <= 0:
        return None

    recent = df_s.tail(cfg.lps_lookback)
    last_close = close.iloc[-1]
    if last_close < last_ma:
        return None

    low_near_ma = recent["low"].min()
    if abs(low_near_ma - last_ma) / last_ma > cfg.lps_ma_tolerance:
        return None

    recent_max_vol = recent["volume"].max()
    # 修正：参考期应剥离当前考察期（recent）
    ref_window_df = df_s.tail(cfg.lps_vol_ref_window + cfg.lps_lookback).iloc[
        : -cfg.lps_lookback
    ]
    ref_max_vol = ref_window_df["volume"].max() if not ref_window_df.empty else 0
    if ref_max_vol <= 0:
        return None
    vol_ratio = recent_max_vol / ref_max_vol
    if vol_ratio > cfg.lps_vol_dry_ratio:
        return None
    return float(vol_ratio)


def _detect_evr(
    df: pd.DataFrame,
    cfg: FunnelConfig,
    *,
    bundle: SymbolFeatureBundle | None = None,
) -> float | None:
    """
    Effort vs Result（努力无结果）：
    仅识别“相对低位的巨量滞涨/抗跌”，排除高位派发。
    返回 score（量比）或 None。
    """
    min_required = cfg.evr_vol_window + 2 + max(int(cfg.evr_confirm_days), 0)
    if len(df) < min_required:
        return None
    df_s = bundle.df if bundle is not None else _sorted_if_needed(df)

    close = (
        bundle.close
        if bundle is not None
        else pd.to_numeric(df_s["close"], errors="coerce")
    )
    low = (
        bundle.low
        if bundle is not None
        else pd.to_numeric(df_s["low"], errors="coerce")
    )
    volume = (
        bundle.volume
        if bundle is not None
        else pd.to_numeric(df_s["volume"], errors="coerce")
    )
    pct_chg = (
        bundle.pct_chg
        if bundle is not None
        else pd.to_numeric(df_s["pct_chg"], errors="coerce")
    )
    if (
        close.isna().all()
        or low.isna().all()
        or volume.isna().all()
        or pct_chg.isna().all()
    ):
        return None

    # 位阶保护：高位放量优先按派发处理，避免 EVR 误判
    ma200 = (
        bundle.ma_long
        if bundle is not None and int(cfg.ma_long) == 200
        else close.rolling(200).mean()
    )
    ma200_last = ma200.iloc[-1]
    close_last = close.iloc[-1]
    if pd.notna(ma200_last) and pd.notna(close_last) and float(ma200_last) > 0:
        bias_200 = (float(close_last) - float(ma200_last)) / float(ma200_last) * 100.0
        if bias_200 > float(cfg.evr_max_bias_200):
            return None

    # 基准量能取“最近窗口但剔除最后两天”，避免当前异动污染基线
    vol_ref = volume.tail(cfg.evr_vol_window).iloc[:-2]
    vol_ref_avg = float(vol_ref.mean()) if not vol_ref.empty else 0.0
    if vol_ref_avg <= 0:
        return None

    confirm_days = max(int(cfg.evr_confirm_days), 0)
    candidate_idx = (-2,) if confirm_days > 0 else (-1, -2)

    # 默认要求“放量滞涨”后至少 1 天确认，不再当日立即上报。
    for idx in candidate_idx:
        vol_ratio = float(volume.iloc[idx] / vol_ref_avg) if vol_ref_avg > 0 else 0.0
        if vol_ratio < cfg.evr_vol_ratio:
            continue

        day_pct = pct_chg.iloc[idx]
        if pd.isna(day_pct):
            continue

        # 结果约束：剔除大阴线/大阳线，保留“努力无结果”的滞涨/抗跌
        if float(day_pct) < -cfg.evr_max_drop or float(day_pct) > 3.0:
            continue

        # 换手率过滤：剔除全天死水里的相对放量假象，但阈值保持保守。
        if "turnover" in df_s.columns and float(cfg.evr_min_turnover) > 0:
            turnover_series = (
                bundle.turnover
                if bundle is not None
                else pd.to_numeric(df_s["turnover"], errors="coerce")
            )
            day_turnover = turnover_series.iloc[idx]
            if pd.notna(day_turnover) and float(day_turnover) < float(
                cfg.evr_min_turnover
            ):
                continue

        # 结构约束：最新收盘不能明显弱于三天前（防止下跌中继）
        if len(close) >= 4:
            close_3d_ago = close.iloc[-4]
            if (
                pd.notna(close_3d_ago)
                and float(close_last) < float(close_3d_ago) * 0.98
            ):
                continue

        if confirm_days > 0:
            event_pos = len(df_s) + idx
            confirm_start = event_pos + 1
            confirm_end = confirm_start + confirm_days
            if confirm_end > len(df_s):
                continue
            event_low = low.iloc[idx]
            confirm_close = close.iloc[confirm_start:confirm_end]
            if pd.isna(event_low) or confirm_close.empty or confirm_close.isna().all():
                continue
            min_confirm_close = float(confirm_close.min())
            allow_break = max(float(cfg.evr_confirm_allow_break_pct), 0.0) / 100.0
            if min_confirm_close < float(event_low) * (1.0 - allow_break):
                continue
        return vol_ratio

    return None


def _detect_sos(
    df: pd.DataFrame,
    cfg: FunnelConfig,
    *,
    bundle: SymbolFeatureBundle | None = None,
) -> float | None:
    """
    Sign of Strength (SOS) / Jump Across the Creek (JAC):
    点火标志。特征为低位脱盘、放量大阳线，破除重要阻力或近期高点。
    返回 score（量比）或 None。
    """
    if len(df) < max(cfg.sos_vol_window, cfg.sos_breakout_window, 200) + 2:
        # Fallback to a smaller necessary length if 200 is too strict, but MA200 needs 200 days
        # We handle MA200 dynamically inside
        pass

    if len(df) < max(cfg.sos_vol_window, cfg.sos_breakout_window) + 2:
        return None

    df_s = bundle.df if bundle is not None else _sorted_if_needed(df)

    close = (
        bundle.close
        if bundle is not None
        else pd.to_numeric(df_s["close"], errors="coerce")
    )
    volume = (
        bundle.volume
        if bundle is not None
        else pd.to_numeric(df_s["volume"], errors="coerce")
    )
    pct_chg = (
        bundle.pct_chg
        if bundle is not None
        else pd.to_numeric(df_s["pct_chg"], errors="coerce")
    )
    high = (
        bundle.high
        if bundle is not None
        else pd.to_numeric(df_s["high"], errors="coerce")
    )

    if close.isna().all() or volume.isna().all() or pct_chg.isna().all():
        return None

    # 位阶保护：高位爆量很大可能是 Buying Climax（派发），排除极大乖离
    close_last = close.iloc[-1]
    if len(close) >= 200:
        ma200 = (
            bundle.ma_long
            if bundle is not None and int(cfg.ma_long) == 200
            else close.rolling(200).mean()
        )
        ma200_last = ma200.iloc[-1]
        if pd.notna(ma200_last) and pd.notna(close_last) and float(ma200_last) > 0:
            bias_200 = (
                (float(close_last) - float(ma200_last)) / float(ma200_last) * 100.0
            )
            if bias_200 > float(cfg.sos_max_bias_200):
                return None

    # 只看当天（威科夫点火通常是当天的明显大阳线）
    day_pct = float(pct_chg.iloc[-1])
    if pd.isna(day_pct) or day_pct < cfg.sos_pct_min:
        return None

    # === 替换 _detect_sos 中的量能判断逻辑 ===

    # 量能要求：暴击量 (由绝对比例 2.0 改为滚动分位数极值验证)
    vol_window = getattr(cfg, "sos_vol_quantile_window", 60)
    vol_ref = volume.tail(vol_window + 1).iloc[:-1]

    if vol_ref.empty:
        return None

    vol_ref_avg = float(vol_ref.mean())
    if vol_ref_avg <= 0:
        return None

    vol_ratio = float(volume.iloc[-1]) / vol_ref_avg

    # 取消 95% 分位数极值爆量约束，回到常识性简单的放量倍数判断
    if vol_ratio < float(getattr(cfg, "sos_vol_ratio", 2.0)):
        return None

    # 结构突破要求：创N日新高，或强势穿透季线/半年线
    ma50 = (
        bundle.ma_short
        if bundle is not None and int(cfg.ma_short) == 50
        else close.rolling(50).mean()
    )
    ma50_last = ma50.iloc[-1] if not ma50.empty else None

    recent_highs = high.tail(cfg.sos_breakout_window + 1).iloc[:-1]
    max_recent_high = (
        float(recent_highs.max()) if not recent_highs.empty else float("inf")
    )

    # 改为 1% 容差（从 2% 改为 1%）
    breakout_tolerance = getattr(cfg, "sos_breakout_tolerance", 0.01)
    is_breakout = float(close_last) >= max_recent_high * (1.0 - breakout_tolerance)

    is_ma_crossover = False
    ma50_prev = ma50.iloc[-2] if len(ma50) >= 2 else None
    if (
        ma50_last is not None
        and pd.notna(ma50_last)
        and ma50_prev is not None
        and pd.notna(ma50_prev)
    ):
        prev_close = float(close.iloc[-2])
        # 修正：Lookahead 问题，昨天的收盘价比昨天的 MA50，今天收盘价比今天的 MA50
        if prev_close <= float(ma50_prev) and float(close_last) > float(ma50_last):
            is_ma_crossover = True

    if not (is_breakout or is_ma_crossover):
        return None

    return vol_ratio


def layer4_triggers(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
    channel_map: dict[str, str] | None = None,
    *,
    feature_map: dict[str, SymbolFeatureBundle] | None = None,
) -> dict[str, list[tuple[str, float]]]:
    """
    在最终候选集上运行 Spring / LPS / EffortVsResult 检测。
    如果 channel_map 中已经标记为"点火破局"，则跳过 SOS 检测（避免重复）。
    """
    results: dict[str, list[tuple[str, float]]] = {
        "sos": [],
        "spring": [],
        "lps": [],
        "evr": [],
    }
    if channel_map is None:
        channel_map = {}

    for sym in symbols:
        bundle = feature_map.get(sym) if feature_map else None
        df = bundle.df if bundle is not None else df_map.get(sym)
        if df is None or df.empty:
            continue
        score = _detect_spring(df, cfg, bundle=bundle)
        if score is not None:
            results["spring"].append((sym, score))
        score = _detect_lps(df, cfg, bundle=bundle)
        if score is not None:
            results["lps"].append((sym, score))
        if getattr(cfg, "enable_evr_trigger", False):
            score = _detect_evr(df, cfg, bundle=bundle)
            if score is not None:
                results["evr"].append((sym, score))

        # 修正：Layer 2 虽然可以去重，但由于下游高度依赖 results["sos"]，所以必须每次都计算或填充
        score = _detect_sos(df, cfg, bundle=bundle)
        if score is not None:
            results["sos"].append((sym, score))
    return results


# Layer 2.5: Markup 阶段识别


def _detect_markup_entry(df: pd.DataFrame, cfg: FunnelConfig) -> float | None:
    """
    Markup 阶段：MA50 从下穿上 MA200，且在上方保持 N 日，确认进入上升趋势。
    返回 score（确认天数占比）或 None。
    """
    if len(df) < max(cfg.ma_long, cfg.markup_ma_crossover_confirm_days) + 5:
        return None

    df_s = _sorted_if_needed(df)
    close = df_s["close"].astype(float)
    ma_short = close.rolling(cfg.ma_short).mean()
    ma_long = close.rolling(cfg.ma_long).mean()

    if (
        pd.isna(ma_short.iloc[-1])
        or pd.isna(ma_long.iloc[-1])
        or ma_short.iloc[-1] <= ma_long.iloc[-1]
    ):
        return None

    # 检查过去 N 日内 MA50 是否从下穿上 MA200
    lookback = max(int(cfg.markup_ma_crossover_confirm_days * 2), 10)
    if len(ma_short) < lookback:
        return None

    recent_ma_short = ma_short.tail(lookback).values
    recent_ma_long = ma_long.tail(lookback).values

    # 寻找穿过点
    crossover_found = False
    for i in range(1, len(recent_ma_short)):
        if (
            recent_ma_short[i - 1] <= recent_ma_long[i - 1]
            and recent_ma_short[i] > recent_ma_long[i]
        ):
            crossover_found = True
            break

    if not crossover_found:
        return None

    # 确认最近 N 日持续在 MA200 上方
    confirm_days = max(int(cfg.markup_ma_crossover_confirm_days), 1)
    recent_above = sum(
        1 for j in range(-confirm_days, 0) if ma_short.iloc[j] > ma_long.iloc[j]
    )

    if recent_above < confirm_days:
        return None

    # 计算 MA50 的角度（过去 5 日的变化率）
    ma_short_recent = ma_short.tail(6).values
    if len(ma_short_recent) < 2:
        return None

    angle = (ma_short_recent[-1] - ma_short_recent[0]) / ma_short_recent[0] * 100.0
    if angle < cfg.markup_ma_angle_min:
        return None

    return float(recent_above / confirm_days)


def detect_markup_stage(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
    *,
    feature_map: dict[str, SymbolFeatureBundle] | None = None,
) -> list[str]:
    """
    返回已进入 Markup 阶段的股票。
    """
    if not cfg.enable_markup_detection:
        return []

    markup: list[str] = []
    for sym in symbols:
        bundle = feature_map.get(sym) if feature_map else None
        df = bundle.df if bundle is not None else df_map.get(sym)
        if df is None or df.empty:
            continue
        score = _detect_markup_entry(df, cfg)
        if score is not None:
            markup.append(sym)

    return markup


# Layer 2 增强: Accumulation ABC 细化


def _analyze_accum_stage(df: pd.DataFrame, cfg: FunnelConfig) -> str | None:
    """
    分析 Accumulation 内部的三个子阶段：
    - A: 下跌停止，量能萎缩
    - B: 底部区间反复测试
    - C: 小幅下跌不破 A 低，量能再度萎缩

    返回 "Accum_A"、"Accum_B"、"Accum_C" 或 None。
    """
    if len(df) < max(
        cfg.accum_lookback_days, cfg.accum_vol_dry_ref_window, cfg.accum_range_window
    ):
        return None

    df_s = _sorted_if_needed(df)
    close = pd.to_numeric(df_s["close"], errors="coerce")
    low = pd.to_numeric(df_s["low"], errors="coerce")
    high = pd.to_numeric(df_s["high"], errors="coerce")
    volume = pd.to_numeric(df_s["volume"], errors="coerce")

    last_close = close.iloc[-1]

    # 条件 1: 低位区——现价在年内低点 +35% 以内
    lookback_w = max(int(cfg.accum_lookback_days), 2)
    period_low = float(low.tail(lookback_w).min())
    if period_low <= 0 or last_close > period_low * (
        1.0 + cfg.accum_price_from_low_max
    ):
        return None

    accum_base_low = period_low

    # 条件 2: 均线即将穿越（MA50 在 MA200 上下 5% 以内）
    ma_short = close.rolling(cfg.ma_short).mean()
    ma_long = close.rolling(cfg.ma_long).mean()
    last_ma_short = ma_short.iloc[-1]
    last_ma_long = ma_long.iloc[-1]

    if pd.isna(last_ma_short) or pd.isna(last_ma_long) or float(last_ma_long) <= 0:
        return None

    ma_gap_pct = (
        (float(last_ma_short) - float(last_ma_long)) / float(last_ma_long) * 100.0
    )
    ma_gap_limit = cfg.accum_ma_gap_max * 100.0  # 配置值为小数（如 0.06 → 6%）
    if not (-ma_gap_limit <= ma_gap_pct <= ma_gap_limit):
        return None

    # 条件 3: 量能萎缩
    dw = max(int(cfg.accum_vol_dry_window), 2)
    rfw = max(int(cfg.accum_vol_dry_ref_window), dw + 1)
    recent_vol_mean = float(volume.tail(dw).mean()) if len(volume) >= dw else 0.0
    ref_vol_mean = (
        float(volume.tail(rfw).iloc[:-dw].mean()) if len(volume) >= rfw else 0.0
    )

    if ref_vol_mean <= 0 or recent_vol_mean / ref_vol_mean >= cfg.accum_vol_dry_ratio:
        return None

    # 现在确定是 A、B 还是 C
    # B 阶段特征：近期有多次测试底部（高低点逐渐走高）
    rw = max(int(cfg.accum_range_window), 5)
    zone = df_s.tail(rw)
    zone_high = pd.to_numeric(zone.get("high"), errors="coerce")
    zone_low = pd.to_numeric(zone.get("low"), errors="coerce")

    if zone_high.empty or zone_low.empty:
        return "Accum_A"

    # 分割测试：最近 N 日内，有多少天的低点接近底部（±5%）
    test_count = sum(
        1 for l in zone_low.dropna() if abs(l - accum_base_low) / accum_base_low <= 0.05
    )

    if test_count >= cfg.accum_b_test_count:
        return "Accum_B"

    # C 阶段：最近有小幅下跌但不破底，且量能再度萎缩
    recent_lookback = min(20, len(df_s))
    recent = df_s.tail(recent_lookback)
    recent_low = pd.to_numeric(recent.get("low"), errors="coerce").min()

    c_stage_ok = recent_low >= accum_base_low * (1.0 - cfg.accum_c_max_drop_ratio)

    if c_stage_ok and recent_vol_mean < ref_vol_mean * cfg.accum_vol_dry_ratio:
        return "Accum_C"

    return "Accum_A"


def detect_accum_stage(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
    *,
    feature_map: dict[str, SymbolFeatureBundle] | None = None,
) -> dict[str, str]:
    """
    返回 symbol -> stage 的映射。
    """
    if not cfg.enable_accum_abc_detail:
        return {}

    result: dict[str, str] = {}
    for sym in symbols:
        bundle = feature_map.get(sym) if feature_map else None
        df = bundle.df if bundle is not None else df_map.get(sym)
        if df is None or df.empty:
            continue
        stage = _analyze_accum_stage(df, cfg)
        if stage is not None:
            result[sym] = stage

    return result


# Layer 5: Exit 策略


def _detect_distribution_start(df: pd.DataFrame, cfg: FunnelConfig) -> bool:
    """
    Distribution 阶段识别：高位缩量警告。
    触发条件：
    1. 价格相对 MA200 处于高位（>30%）
    2. 连续 N 日的成交量 < 参考均量的 50%
    """
    if len(df) < max(cfg.ma_long, cfg.dist_confirm_days) + 20:
        return False

    df_s = _sorted_if_needed(df)
    close = df_s["close"].astype(float)
    volume = df_s["volume"].astype(float)

    ma_long = close.rolling(cfg.ma_long).mean()
    last_ma_long = ma_long.iloc[-1]
    last_close = close.iloc[-1]

    if pd.isna(last_ma_long) or pd.isna(last_close) or last_ma_long <= 0:
        return False

    bias = (last_close - last_ma_long) / last_ma_long * 100.0
    if bias < cfg.dist_high_threshold_pct:
        return False

    # 检查近 N 日的缩量
    ref_vol = volume.tail(60).mean()
    recent_vol = volume.tail(cfg.dist_confirm_days).mean()

    if ref_vol <= 0:
        return False

    if recent_vol / ref_vol > cfg.dist_vol_dry_ratio:
        return False

    return True


def layer5_exit_signals(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    accum_stage_map: dict[str, str],
    cfg: FunnelConfig,
    *,
    feature_map: dict[str, SymbolFeatureBundle] | None = None,
) -> dict[str, dict]:
    """
    为 Accumulation 和 Markup 阶段股票生成静态参考 Exit 信号（实际实盘止损在 OMS 中独立维护）。
    统一采用：初始底线止损 + 动态跟踪止损(Trailing Stop) + 派发预警，让利润奔跑。
    返回 {symbol: {signal: ..., price: ..., reason: ...}}
    """
    if not cfg.enable_exit_signals:
        return {}

    signals: dict[str, dict] = {}

    for sym in symbols:
        bundle = feature_map.get(sym) if feature_map else None
        df = bundle.df if bundle is not None else df_map.get(sym)
        if df is None or df.empty:
            continue

        df_s = bundle.df if bundle is not None else _sorted_if_needed(df)
        close = (
            bundle.close
            if bundle is not None
            else pd.to_numeric(df_s["close"], errors="coerce")
        )
        low = (
            bundle.low
            if bundle is not None
            else pd.to_numeric(df_s["low"], errors="coerce")
        )
        high = (
            bundle.high
            if bundle is not None
            else pd.to_numeric(df_s["high"], errors="coerce")
        )

        if close.empty or low.empty or high.empty:
            continue

        last_close = float(close.iloc[-1])
        stage = accum_stage_map.get(sym, "Markup")  # 默认按主升处理

        stop_loss_price = None
        stop_reason = ""

        # 获取 MA50 作为动态生命线
        ma_short_series = (
            bundle.ma_short
            if bundle is not None
            else close.rolling(cfg.ma_short).mean()
        )
        ma_short = (
            float(ma_short_series.iloc[-1])
            if not ma_short_series.isna().all()
            else None
        )
        # 获取近期 60 日最高点
        recent_high = float(high.tail(60).max())

        if stage.startswith("Accum_"):
            # 对于吸筹股，锚定“年内最低点”作为原始护城河
            lookback_w = max(int(cfg.accum_lookback_days), 2)
            accum_low = float(low.tail(lookback_w).min())

            # 判断是否已经脱离底部成本区，激活移动跟踪止损
            trailing_active_pct = getattr(cfg, "exit_trailing_active_pct", 15.0) / 100.0
            if last_close >= accum_low * (1.0 + trailing_active_pct):
                # 已经大幅盈利，转换为跟踪防守（近期高点回撤 或 MA50，取最高值作为底线）
                drawdown_pct = getattr(cfg, "exit_trailing_drawdown_pct", -10.0) / 100.0
                trailing_price = recent_high * (1.0 + drawdown_pct)
                if ma_short is not None:
                    stop_loss_price = max(
                        trailing_price, float(ma_short) * 0.98
                    )  # MA50 容差2%
                else:
                    stop_loss_price = trailing_price
                stop_reason = "已脱离底部，触发利润保护(动态跟踪止损)"
            else:
                # 还在底部摩擦，执行严格的跌破成本区止损
                stop_loss_price = accum_low * (1.0 + cfg.exit_stop_loss_pct / 100.0)
                stop_reason = f"破位防守(跌破 {stage} 吸筹底线)"
        else:
            # 对于 Markup 强势主升股，直接采用高位跟踪止损
            drawdown_pct = getattr(cfg, "exit_trailing_drawdown_pct", -10.0) / 100.0
            trailing_price = recent_high * (1.0 + drawdown_pct)
            if ma_short is not None:
                stop_loss_price = max(trailing_price, float(ma_short) * 0.98)
            else:
                stop_loss_price = trailing_price
            stop_reason = "主升趋势破位(跌破MA50或高位回撤)"

        # 1. 检查是否触发止损/跟踪止损
        if stop_loss_price is not None and last_close <= stop_loss_price:
            signals[sym] = {
                "signal": "stop_loss",
                "price": stop_loss_price,
                "current": last_close,
                "reason": stop_reason,
            }
            continue

        # 2. 检查是否有 Distribution (派发) 警告 (高位放量滞涨或缩量)
        if _detect_distribution_start(df_s, cfg):
            signals[sym] = {
                "signal": "distribution_warning",
                "reason": "检测到高位 Distribution 阶段迹象（放量不涨/高位缩量），主力疑似派发",
            }

    return signals


# run_funnel: 串联 4 层


def run_funnel(
    all_symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame | None,
    name_map: dict[str, str],
    market_cap_map: dict[str, float],
    sector_map: dict[str, str],
    cfg: FunnelConfig | None = None,
) -> FunnelResult:
    if cfg is None:
        cfg = FunnelConfig()

    # 预先整理时序，避免各层重复 sort/copy 产生大量临时对象。
    prepared_df_map: dict[str, pd.DataFrame] = {
        sym: _sorted_if_needed(df)
        for sym, df in df_map.items()
        if df is not None and not df.empty
    }
    feature_map = build_feature_map(prepared_df_map, cfg)

    l1, l1_rejections = layer1_filter(
        all_symbols,
        name_map,
        market_cap_map,
        prepared_df_map,
        cfg,
        feature_map=feature_map,
        return_rejections=True,
    )
    l2, channel_map, l2_rejections, l2_decisions = layer2_strength_detailed(
        l1,
        prepared_df_map,
        bench_df,
        cfg,
        rps_universe=list(prepared_df_map.keys()),
        feature_map=feature_map,
        return_rejections=True,
        return_decisions=True,
    )
    l3, top_sectors = layer3_sector_resonance(
        l2,
        sector_map,
        cfg,
        base_symbols=l1,
        df_map=prepared_df_map,
    )
    triggers = layer4_triggers(
        l3,
        prepared_df_map,
        cfg,
        channel_map=channel_map,
        feature_map=feature_map,
    )

    # 阶段识别和退出信号
    markup_symbols = detect_markup_stage(
        l3, prepared_df_map, cfg, feature_map=feature_map
    )
    accum_stage_map = detect_accum_stage(
        l2, prepared_df_map, cfg, feature_map=feature_map
    )  # 对 L2 做细化分析

    # 构建完整的 stage_map（包括 Markup）
    stage_map: dict[str, str] = accum_stage_map.copy()
    for sym in markup_symbols:
        stage_map[sym] = "Markup"

    # 退出信号针对 L2 和 Markup 股票
    exit_signals = layer5_exit_signals(
        l2 + markup_symbols,
        prepared_df_map,
        accum_stage_map,
        cfg,
        feature_map=feature_map,
    )

    trigger_by_symbol: dict[str, list[str]] = {}
    for trigger_name, rows in triggers.items():
        for sym, _score in rows:
            trigger_by_symbol.setdefault(str(sym), []).append(str(trigger_name))

    explanations: dict[str, dict] = {}
    focus_symbols = list(dict.fromkeys(l1 + l2 + l3 + markup_symbols))
    for sym in focus_symbols:
        bundle = feature_map.get(sym)
        avg_amount = None
        if bundle is not None:
            avg_amt = bundle.amount.tail(cfg.amount_avg_window).mean()
            avg_amount = float(avg_amt) if pd.notna(avg_amt) else None
        explanations[sym] = {
            "passed_layers": {
                "layer1": sym in l1,
                "layer2": sym in l2,
                "layer3": sym in l3,
                "markup": sym in markup_symbols,
            },
            "layer1_rejection": l1_rejections.get(sym),
            "layer2_rejection": l2_rejections.get(sym),
            "channel": str(channel_map.get(sym, "") or ""),
            "layer2_decision": (
                {
                    "passed": l2_decisions[sym].passed,
                    "selected_track": l2_decisions[sym].selected_track,
                    "selected_score": l2_decisions[sym].selected_score,
                    "track_scores": l2_decisions[sym].track_scores,
                    "old_channels": l2_decisions[sym].old_channels,
                    "reasons": l2_decisions[sym].reasons,
                }
                if sym in l2_decisions
                else None
            ),
            "stage": str(stage_map.get(sym, "") or ""),
            "triggers": trigger_by_symbol.get(sym, []),
            "exit_signal": (exit_signals.get(sym, {}) or {}).get("signal"),
            "avg_amount_20": avg_amount,
        }

    return FunnelResult(
        layer1_symbols=l1,
        layer2_symbols=l2,
        layer3_symbols=l3,
        top_sectors=top_sectors,
        triggers=triggers,
        stage_map=stage_map,
        markup_symbols=markup_symbols,
        exit_signals=exit_signals,
        channel_map=channel_map,
        explanations=explanations,
        layer1_rejections=l1_rejections,
        layer2_rejections=l2_rejections,
    )


def allocate_ai_candidates(
    result: FunnelResult,
    l3_ranked_symbols: list[str],
    regime: str,
    override_total_cap: int = -1,
    sector_map: dict[str, str] | None = None,
    max_per_sector: int = 2,
) -> tuple[list[str], list[str], dict[str, float]]:
    """
    根据大盘政权和各轨配额，计算优先级得分，输出 (trend_selected, accum_selected, score_map)
    """
    policy = resolve_ai_candidate_policy(regime, override_total_cap=override_total_cap)
    total_cap = int(policy["total_cap"])
    trend_quota = int(policy["trend_quota"])
    accum_quota = int(policy["accum_quota"])
    max_trend_l3_fill = int(policy["max_trend_l3_fill"])
    max_accum_l3_fill = int(policy["max_accum_l3_fill"])

    trend_channel_tags = {"主升通道", "点火破局", "主升确认", "启动确认"}
    accum_channel_tags = {"潜伏通道", "吸筹通道", "地量蓄势", "暗中护盘", "吸筹改善"}

    def _channel_tags(code: str) -> set[str]:
        raw = str(result.channel_map.get(code, "")).strip()
        if not raw:
            return set()
        return {x.strip() for x in raw.split("+") if x.strip()}

    def _is_trend_track(code: str) -> bool:
        return bool(_channel_tags(code) & trend_channel_tags)

    def _is_accum_track(code: str) -> bool:
        return bool(_channel_tags(code) & accum_channel_tags)

    def _dedup_order(codes: list[str]) -> list[str]:
        out = []
        seen = set()
        for c in codes:
            c = str(c).strip()
            if c and c not in seen:
                seen.add(c)
                out.append(c)
        return out

    sos_hit_set = set(str(c).strip() for c, _ in result.triggers.get("sos", []))
    spring_hit_set = set(str(c).strip() for c, _ in result.triggers.get("spring", []))
    lps_hit_set = set(str(c).strip() for c, _ in result.triggers.get("lps", []))
    # evr_hit_set = set(str(c).strip() for c, _ in result.triggers.get("evr", []))
    blocked_exit_signals = {"stop_loss", "distribution_warning"}

    def _stage_name(code: str) -> str:
        return result.stage_map.get(code, "")

    def _is_blocked_exit(code: str) -> bool:
        sig = str((result.exit_signals.get(code, {}) or {}).get("signal", "")).strip()
        return sig in blocked_exit_signals

    def _is_accum_stage_candidate(code: str) -> bool:
        return _stage_name(code) in {"Accum_B", "Accum_C"}

    def _calc_priority_score(code: str, is_trend_side: bool) -> float:
        score = 0.0
        stage_name = _stage_name(code)

        if code in result.markup_symbols:
            score += 100.0
        if stage_name == "Accum_C":
            score += (
                15.0 if not is_trend_side else 5.0
            )  # 回测显示 Accum 胜率仅 31.8%，降权
        elif stage_name == "Accum_B":
            score += 8.0 if not is_trend_side else 3.0
        elif stage_name == "Accum_A":
            score += 3.0 if not is_trend_side else 0.0

        if code in sos_hit_set:
            score += 50.0
        if code in spring_hit_set:
            score += 45.0
        if code in lps_hit_set:
            score += 40.0
        if is_trend_side and code in sos_hit_set:
            score += 10.0
        if (not is_trend_side) and (code in spring_hit_set or code in lps_hit_set):
            score += 10.0

        exit_sig = result.exit_signals.get(code, {})
        if exit_sig.get("signal") == "stop_loss":
            score -= 100.0
        elif exit_sig.get("signal") == "distribution_warning":
            score -= 20.0

        return score

    trend_candidates_with_score: list[tuple[str, float, bool]] = []
    accum_candidates_with_score: list[tuple[str, float, bool]] = []

    markup_trend_candidates = [
        c for c in result.markup_symbols if _is_trend_track(c) or c in sos_hit_set
    ]
    for code in _dedup_order(markup_trend_candidates):
        trend_candidates_with_score.append(
            (code, _calc_priority_score(code, True), False)
        )

    sos_hit_codes = [
        str(c).strip()
        for c, _ in sorted(
            result.triggers.get("sos", []),
            key=lambda x: -float(x[1] if x[1] is not None else 0.0),
        )
        if str(c).strip()
    ]
    for code in _dedup_order(sos_hit_codes):
        if code not in [c[0] for c in trend_candidates_with_score]:
            trend_candidates_with_score.append(
                (code, _calc_priority_score(code, True), False)
            )

    # Compute `sorted_codes` implicitly from triggers like funnel does
    all_triggers = []
    for k, v in result.triggers.items():
        all_triggers.extend(v)
    sorted_codes = [
        c
        for c, _ in sorted(
            all_triggers, key=lambda x: -float(x[1] if x[1] is not None else 0.0)
        )
    ]
    sorted_codes = _dedup_order(sorted_codes)

    for code in sorted_codes + l3_ranked_symbols:
        if not _is_trend_track(code) or _is_blocked_exit(code):
            continue
        if code in [c[0] for c in trend_candidates_with_score]:
            continue
        if code in result.markup_symbols or code in sos_hit_set:
            trend_candidates_with_score.append(
                (code, _calc_priority_score(code, True), False)
            )
            continue
        # 移除 L3 filler 逻辑: 宁缺毋滥，如果只有几个好标的，就只送这几个给 AI

    accum_hit_candidates = result.triggers.get("spring", []) + result.triggers.get(
        "lps", []
    )
    for code, _ in sorted(
        accum_hit_candidates, key=lambda x: -float(x[1] if x[1] is not None else 0.0)
    ):
        code = str(code).strip()
        if _is_blocked_exit(code):
            continue
        accum_candidates_with_score.append(
            (code, _calc_priority_score(code, False), False)
        )

    for code in _dedup_order(l3_ranked_symbols):
        if not _is_accum_track(code) or _is_blocked_exit(code):
            continue
        if code in [c[0] for c in accum_candidates_with_score]:
            continue
        if _stage_name(code) == "Accum_C":
            accum_candidates_with_score.append(
                (code, _calc_priority_score(code, False), False)
            )
            continue
        # 移除 L3 filler 逻辑: 宁缺毋滥

    trend_candidates_with_score.sort(key=lambda x: (-x[1], x[2]))
    accum_candidates_with_score.sort(key=lambda x: (-x[1], x[2]))

    trend_candidates = _dedup_order(
        [c[0] for c in trend_candidates_with_score if not _is_blocked_exit(c[0])]
    )
    accum_candidates = _dedup_order(
        [c[0] for c in accum_candidates_with_score if not _is_blocked_exit(c[0])]
    )

    if total_cap <= 0:
        score_map = {}
        for c, s, _ in trend_candidates_with_score:
            score_map[c] = s
        for c, s, _ in accum_candidates_with_score:
            score_map[c] = max(score_map.get(c, -9999.0), s)
        return ([], [], score_map)

    selected_seen = set()
    trend_selected = []
    accum_selected = []
    trend_l3_fill_used = 0
    accum_l3_fill_used = 0
    trend_fill_map = {code: is_fill for code, _, is_fill in trend_candidates_with_score}
    accum_fill_map = {code: is_fill for code, _, is_fill in accum_candidates_with_score}

    sector_counts: dict[str, int] = {}

    def _add_to_selected(code: str, track_name: str) -> bool:
        nonlocal trend_l3_fill_used, accum_l3_fill_used
        if total_cap > 0 and len(selected_seen) >= total_cap:
            return False
        if code in selected_seen:
            return False

        sector = ""
        if sector_map and max_per_sector > 0:
            sector = sector_map.get(code, "").strip()
            if sector and sector_counts.get(sector, 0) >= max_per_sector:
                return False

        if track_name == "Trend":
            if (
                trend_fill_map.get(code, False)
                and trend_l3_fill_used >= max_trend_l3_fill
            ):
                return False
            if len(trend_selected) >= trend_quota:
                return False
            trend_selected.append(code)
            if trend_fill_map.get(code, False):
                trend_l3_fill_used += 1
        else:
            if (
                accum_fill_map.get(code, False)
                and accum_l3_fill_used >= max_accum_l3_fill
            ):
                return False
            if len(accum_selected) >= accum_quota:
                return False
            accum_selected.append(code)
            if accum_fill_map.get(code, False):
                accum_l3_fill_used += 1

        if sector:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
        selected_seen.add(code)
        return True

    trend_idx = 0
    accum_idx = 0

    while (
        len(selected_seen) < total_cap
        and (len(trend_selected) < trend_quota or len(accum_selected) < accum_quota)
        and (trend_idx < len(trend_candidates) or accum_idx < len(accum_candidates))
    ):
        progressed = False

        while len(trend_selected) < trend_quota and trend_idx < len(trend_candidates):
            code = trend_candidates[trend_idx]
            trend_idx += 1
            if code in selected_seen:
                continue
            progressed = _add_to_selected(code, "Trend") or progressed
            break

        if len(selected_seen) >= total_cap:
            break

        while len(accum_selected) < accum_quota and accum_idx < len(accum_candidates):
            code = accum_candidates[accum_idx]
            accum_idx += 1
            if code in selected_seen:
                continue
            progressed = _add_to_selected(code, "Accum") or progressed
            break

        if not progressed:
            break

    score_map = {}
    for c, s, _ in trend_candidates_with_score:
        score_map[c] = s
    for c, s, _ in accum_candidates_with_score:
        score_map[c] = max(score_map.get(c, -9999.0), s)

    return trend_selected, accum_selected, score_map
