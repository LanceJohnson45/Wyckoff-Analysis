#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import warnings

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from scipy.signal import find_peaks
from scipy.stats import linregress
import yfinance as yf

"""
========================================================
  威科夫量价分析工具 · Wyckoff Price-Volume Analyzer
  作者: AI Assistant
  依赖: pip install yfinance pandas numpy matplotlib scipy
========================================================

使用方法:
  1. 直接运行: python wyckoff_analyzer.py
  2. 修改下方 CONFIG 区域的 TICKER / PERIOD / INTERVAL
  3. 导入使用:
       from wyckoff_analyzer import WyckoffAnalyzer
       a = WyckoffAnalyzer("MSFT", "3mo").fetch_data() \
            .detect_support_resistance().detect_wyckoff_events().detect_phase()
       a.print_report()
       a.plot("chart.png")

威科夫阶段说明:
  Accumulation Phase A : SC (卖出密集区) 出现，下跌停止
  Accumulation Phase B : 震荡吸筹，消耗浮筹
  Accumulation Phase C : Spring (弹簧) 或 ST 测试
  Accumulation Phase D : SOS (强势信号) + LPS (末端支撑)
  Accumulation Phase E : 价格突破，进入 Markup 阶段
  Distribution        : BC 出现，顶部派发
  Markup              : 上升行情
  Markdown            : 下跌行情

关键事件标注:
  SC     卖出密集区 (Selling Climax)
  AR     自动反弹   (Automatic Rally)
  ST     二次测试   (Secondary Test)
  Spring 弹簧/假跌破
  SOS    强势信号   (Sign of Strength)
  LPS    末端支撑   (Last Point of Support)
  BC     买入密集区 (Buying Climax) — 顶部信号
  UT     上冲试压   (Upthrust)
"""


# ─────────────────────────────────────────────
#  CONFIG — 修改这里来分析不同股票
# ─────────────────────────────────────────────
CONFIG = {
    "TICKER": "MSFT",  # 股票代码: AAPL / TSLA / NVDA / 0700.HK
    "PERIOD": "1mo",  # 数据区间: 1mo 3mo 6mo 1y 2y
    "INTERVAL": "1d",  # K线周期: 1d(日线) 1wk(周线) 1h(时线)
    "SAVE_PATH": "wyckoff_analysis.png",
    "WINDOW": 5,  # 支撑阻力检测窗口（K线数）
    "N_LEVELS": 4,  # 最多展示几条支撑/阻力线
}
# ─────────────────────────────────────────────


class WyckoffAnalyzer:
    """威科夫量价分析主类"""

    # 每个威科夫事件的绘图样式
    _EVENT_STYLES = {
        "SC": {"color": "#ff4757", "marker": "v", "side": -1, "label": "SC 卖出密集区"},
        "AR": {"color": "#ffa502", "marker": "^", "side": +1, "label": "AR 自动反弹"},
        "ST": {"color": "#eccc68", "marker": "v", "side": -1, "label": "ST 二次测试"},
        "Spring": {
            "color": "#00d2d3",
            "marker": "v",
            "side": -1,
            "label": "Spring 弹簧",
        },
        "SOS": {"color": "#2ed573", "marker": "^", "side": +1, "label": "SOS 强势信号"},
        "LPS": {"color": "#1e90ff", "marker": "v", "side": -1, "label": "LPS 末端支撑"},
        "BC": {"color": "#ff6b81", "marker": "^", "side": +1, "label": "BC 买入密集区"},
        "UT": {"color": "#ff4757", "marker": "^", "side": +1, "label": "UT 上冲试压"},
    }

    def __init__(self, ticker: str, period: str = "3mo", interval: str = "1d"):
        self.ticker = ticker.upper()
        self.period = period
        self.interval = interval
        self.df: pd.DataFrame = None
        self.events: list = []
        self.support_levels: list = []
        self.resistance_levels: list = []
        self.phase = "未知"
        self.phase_desc = ""
        self.phase_color = "#ffa726"

    # ──────────────────────────────────────────
    #  Step 1: 获取数据 & 计算基础指标
    # ──────────────────────────────────────────
    def fetch_data(self) -> "WyckoffAnalyzer":
        """通过 yfinance 拉取 OHLCV 数据"""
        raw = yf.download(
            self.ticker,
            period=self.period,
            interval=self.interval,
            auto_adjust=True,
            progress=False,
        )
        if raw.empty:
            raise ValueError(f"无法获取 {self.ticker} 的数据，请检查 ticker 是否正确。")
        # 处理多级列名
        raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
        self.df = raw.dropna().copy()
        self._calc_indicators()
        print(
            f"[OK] 已获取 {self.ticker} 数据，共 {len(self.df)} 根K线 ({self.interval})"
        )
        return self

    def _calc_indicators(self):
        """计算量价分析所需衍生指标"""
        df = self.df

        # ── 成交量指标 ──────────────────────────
        df["vol_ma20"] = df["Volume"].rolling(20, min_periods=1).mean()
        df["vol_ma5"] = df["Volume"].rolling(5, min_periods=1).mean()
        df["vol_ratio"] = df["Volume"] / df["vol_ma20"]  # 量比

        # ── K线形态指标 ─────────────────────────
        df["body"] = (df["Close"] - df["Open"]).abs()
        df["range"] = df["High"] - df["Low"]
        hi_oc = df[["Open", "Close"]].max(axis=1)
        lo_oc = df[["Open", "Close"]].min(axis=1)
        df["upper_shadow"] = df["High"] - hi_oc
        df["lower_shadow"] = lo_oc - df["Low"]
        df["is_bullish"] = df["Close"] >= df["Open"]
        df["pct_change"] = df["Close"].pct_change()

        # ── ATR (14) ────────────────────────────
        tr_list = pd.concat(
            [
                df["High"] - df["Low"],
                (df["High"] - df["Close"].shift(1)).abs(),
                (df["Low"] - df["Close"].shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        df["atr14"] = tr_list.rolling(14, min_periods=1).mean()

        # ── 移动均线 ────────────────────────────
        df["ma20"] = df["Close"].rolling(20, min_periods=1).mean()
        df["ma50"] = df["Close"].rolling(50, min_periods=1).mean()

        # ── 10 日价格动量（线性斜率）─────────────
        def _slope10(arr):
            if len(arr) < 3:
                return 0.0
            return linregress(range(len(arr)), arr)[0]

        df["momentum10"] = (
            df["Close"].rolling(10, min_periods=3).apply(_slope10, raw=True)
        )

        self.df = df

    # ──────────────────────────────────────────
    #  Step 2: 支撑位 & 阻力位检测
    # ──────────────────────────────────────────
    def detect_support_resistance(
        self, window: int = 5, n_levels: int = 4, cluster_pct: float = 0.015
    ) -> "WyckoffAnalyzer":
        """
        用局部极值法检测支撑/阻力，并对相近价位做聚类合并。

        参数:
            window      : 极值检测的左右距离（K线数）
            n_levels    : 最终输出的级别数量
            cluster_pct : 聚类阈值（价格偏差百分比）
        """
        lows = self.df["Low"].values
        highs = self.df["High"].values

        min_idx, _ = find_peaks(-lows, distance=window)
        max_idx, _ = find_peaks(highs, distance=window)

        sup_raw = lows[min_idx]
        res_raw = highs[max_idx]

        def _cluster(prices, pct):
            if len(prices) == 0:
                return []
            arr = np.sort(prices)
            clusters, buf = [], [arr[0]]
            for p in arr[1:]:
                if (p - buf[-1]) / buf[-1] < pct:
                    buf.append(p)
                else:
                    clusters.append(float(np.mean(buf)))
                    buf = [p]
            clusters.append(float(np.mean(buf)))
            return clusters

        cur = float(self.df["Close"].iloc[-1])

        all_sup = _cluster(sup_raw, cluster_pct)
        all_res = _cluster(res_raw, cluster_pct)

        # 支撑位：当前价以下（含小幅容差 2%）
        self.support_levels = sorted(
            [s for s in all_sup if s < cur * 1.02], reverse=True
        )[:n_levels]

        # 阻力位：当前价以上（含小幅容差 2%）
        self.resistance_levels = sorted([r for r in all_res if r > cur * 0.98])[
            :n_levels
        ]

        return self

    # ──────────────────────────────────────────
    #  Step 3: 威科夫关键事件检测
    # ──────────────────────────────────────────
    def detect_wyckoff_events(self) -> "WyckoffAnalyzer":
        """
        按时序检测以下威科夫事件:
          SC → AR → ST → Spring → SOS → LPS（吸筹结构）
          BC → UT（派发结构）
        """
        df = self.df
        n = len(df)
        events = []

        def _has(typ):
            return any(e["type"] == typ for e in events)

        def _add(typ, i, price, extra=""):
            row = df.iloc[i]
            vr = row["vol_ratio"]
            events.append(
                {
                    "type": typ,
                    "index": i,
                    "date": df.index[i],
                    "price": price,
                    "vol_ratio": vr,
                    "extra": extra,
                }
            )

        # ── SC: 卖出密集区 ──────────────────────
        # 条件: ①量比>1.5 ②跌幅>2% ③下影线>实体50% ④前5日均价高于当日收盘
        for i in range(5, n):
            r = df.iloc[i]
            if _has("SC"):
                break
            prev_mean = df["Close"].iloc[max(0, i - 5) : i].mean()
            if (
                r["vol_ratio"] > 1.5
                and r["pct_change"] < -0.02
                and r["lower_shadow"] > r["body"] * 0.5
                and prev_mean > r["Close"]
            ):
                _add(
                    "SC",
                    i,
                    r["Low"],
                    f"量比{r['vol_ratio']:.1f}x | 跌{r['pct_change'] * 100:.1f}%",
                )

        # ── AR: 自动反弹 ────────────────────────
        # 条件: SC后10根内，涨幅>1.5%，量比>0.7
        for sc in [e for e in events if e["type"] == "SC"]:
            if _has("AR"):
                break
            for i in range(sc["index"] + 1, min(sc["index"] + 12, n)):
                r = df.iloc[i]
                if r["pct_change"] > 0.015 and r["vol_ratio"] > 0.7:
                    _add(
                        "AR",
                        i,
                        r["High"],
                        f"量比{r['vol_ratio']:.1f}x | 涨{r['pct_change'] * 100:.1f}%",
                    )
                    break

        # ── ST: 二次测试 ────────────────────────
        # 条件: AR后，价格回落至 SC 低点附近 ±5%，量能缩减
        for ar in [e for e in events if e["type"] == "AR"]:
            if _has("ST"):
                break
            sc = next((e for e in events if e["type"] == "SC"), None)
            if not sc:
                continue
            for i in range(ar["index"] + 1, min(ar["index"] + 25, n)):
                r = df.iloc[i]
                near = abs(r["Low"] - sc["price"]) / sc["price"] < 0.05
                if near and r["vol_ratio"] < 1.0:
                    _add("ST", i, r["Low"], f"量比{r['vol_ratio']:.1f}x | 接近SC低点")
                    break

        # ── Spring: 弹簧 ─────────────────────────
        # 条件: 跌破TR低点（SC/ST最低价）后次日收回，且量能不大
        sc_st = [e for e in events if e["type"] in ("SC", "ST")]
        if sc_st and not _has("Spring"):
            tr_low = min(e["price"] for e in sc_st)
            for i in range(1, n - 1):
                r = df.iloc[i]
                r_nx = df.iloc[i + 1]
                if (
                    r["Low"] < tr_low * 1.002
                    and r_nx["Close"] > tr_low
                    and r["vol_ratio"] < 1.3
                ):
                    _add("Spring", i, r["Low"], "跌破TR后迅速收回")
                    break

        # ── SOS: 强势信号 ───────────────────────
        # 条件: 阳线 + 量比>1.3 + 涨幅>2% + 前5日均价低于当日
        for i in range(5, n):
            if _has("SOS"):
                break
            r = df.iloc[i]
            prev_mean = df["Close"].iloc[max(0, i - 5) : i].mean()
            if (
                r["is_bullish"]
                and r["vol_ratio"] > 1.3
                and r["pct_change"] > 0.02
                and prev_mean < r["Close"]
            ):
                _add(
                    "SOS",
                    i,
                    r["High"],
                    f"量比{r['vol_ratio']:.1f}x | 涨{r['pct_change'] * 100:.1f}%",
                )

        # ── LPS: 末端支撑 ───────────────────────
        # 条件: SOS后，阴线且量比<0.85（缩量回调）
        for sos in [e for e in events if e["type"] == "SOS"]:
            if _has("LPS"):
                break
            for i in range(sos["index"] + 1, min(sos["index"] + 15, n)):
                r = df.iloc[i]
                if not r["is_bullish"] and r["vol_ratio"] < 0.85:
                    _add("LPS", i, r["Low"], f"量比{r['vol_ratio']:.1f}x | 缩量回调")
                    break

        # ── BC: 买入密集区（派发顶部）──────────
        # 条件: 大阳线 + 量比>1.8 + 上影线明显
        for i in range(5, n):
            if _has("BC"):
                break
            r = df.iloc[i]
            prev_mean = df["Close"].iloc[max(0, i - 5) : i].mean()
            if (
                r["vol_ratio"] > 1.8
                and r["pct_change"] > 0.02
                and r["upper_shadow"] > r["body"] * 0.3
                and prev_mean < r["Close"]
            ):
                _add("BC", i, r["High"], f"量比{r['vol_ratio']:.1f}x | 顶部放量")

        # ── UT: 上冲试压（派发）────────────────
        # 条件: BC后，收盘低于前期高点，量能缩减
        for bc in [e for e in events if e["type"] == "BC"]:
            if _has("UT"):
                break
            for i in range(bc["index"] + 1, min(bc["index"] + 12, n)):
                r = df.iloc[i]
                if (
                    r["High"] >= bc["price"] * 0.99
                    and r["Close"] < bc["price"]
                    and r["vol_ratio"] < bc["vol_ratio"]
                ):
                    _add("UT", i, r["High"], f"量比{r['vol_ratio']:.1f}x | 冲高回落")
                    break

        self.events = sorted(events, key=lambda e: e["index"])
        print(
            f"[OK] 共识别威科夫事件 {len(self.events)} 个: "
            + ", ".join(e["type"] for e in self.events)
        )
        return self

    # ──────────────────────────────────────────
    #  Step 4: 当前阶段判断
    # ──────────────────────────────────────────
    def detect_phase(self) -> "WyckoffAnalyzer":
        """综合事件序列 + 价格趋势判断当前威科夫阶段"""
        df = self.df
        n = len(df)
        recent = df.iloc[-20:] if n >= 20 else df

        slope, _, r2, _, _ = linregress(range(len(recent)), recent["Close"].values)
        vol_slope, *_ = linregress(range(len(recent)), recent["Volume"].values)

        has = lambda t: any(e["type"] == t for e in self.events)

        # ── 派发阶段 ────────────────────────────
        if has("BC") and slope < 0:
            self._set_phase(
                "派发阶段 (Distribution)",
                "#e74c3c",
                "主力高位向散户派发筹码。价格高位震荡放量却不涨，随时可能转入 Markdown 下跌。"
                "关注 UT 上冲试压确认，破 TR 低点即入场做空信号。",
            )
        # ── Markup 上升 ──────────────────────────
        elif slope > 0 and abs(r2) > 0.65 and has("SOS"):
            self._set_phase(
                "上升阶段 (Markup)",
                "#27ae60",
                "吸筹完成，价格已突破交易区间进入上升通道。量价配合，多头主导。"
                "回调至 LPS/支撑位可逢低做多，止损设于 TR 顶部之下。",
            )
        # ── Markdown 下跌 ────────────────────────
        elif slope < -0.15 and abs(r2) > 0.6 and not has("SC"):
            self._set_phase(
                "下跌阶段 (Markdown)",
                "#c0392b",
                "价格处于明显下跌趋势，做空力量主导。下跌放量、反弹缩量为典型空头结构。"
                "等待 SC 卖出密集区出现，再考虑底部布局。",
            )
        # ── 吸筹 D-E 期 ─────────────────────────
        elif has("SC") and has("AR") and has("SOS") and has("LPS"):
            self._set_phase(
                "吸筹阶段 D-E 期 (Accumulation D-E)",
                "#3498db",
                "SOS + LPS 均已确认，吸筹接近完成。机构即将拉升，是较高确定性的多头买入窗口。"
                "等待量能配合的突破 TR 上沿，加仓做多。",
            )
        # ── 吸筹 D 期 ───────────────────────────
        elif has("SC") and has("AR") and has("SOS"):
            self._set_phase(
                "吸筹阶段 D 期 (Accumulation D)",
                "#2980b9",
                "强势信号 SOS 已出现，需求压制供应。等待缩量回调形成 LPS，再确认做多。",
            )
        # ── 吸筹 B-C 期 ─────────────────────────
        elif has("SC") and has("AR"):
            spring_st = (
                "（已出现 Spring）" if has("Spring") else "（等待 Spring 或二次低点）"
            )
            self._set_phase(
                f"吸筹阶段 B-C 期 (Accumulation B-C) {spring_st}",
                "#8e44ad",
                "SC + AR 已现，正处于震荡吸筹区间内，主力持续消化浮筹。"
                "关注 Spring/缩量回调，或 SOS 放量突破作为入场信号。",
            )
        # ── 吸筹 A 期 ───────────────────────────
        elif has("SC"):
            self._set_phase(
                "吸筹阶段 A 期 (Accumulation A)",
                "#9b59b6",
                "恐慌抛售 SC 已出现，卖方力量逐渐衰竭。"
                "等待 AR 自动反弹确认底部范围，区间尚未建立，暂观察为主。",
            )
        # ── 趋势未知 ─────────────────────────────
        else:
            if slope > 0:
                self._set_phase(
                    "上升趋势 / 等待信号",
                    "#f39c12",
                    "价格处于上升趋势，但尚未出现明显威科夫事件。"
                    "关注是否出现 BC 放量顶部，或继续持有多仓。",
                )
            else:
                self._set_phase(
                    "下跌趋势 / 等待 SC",
                    "#e67e22",
                    "价格仍在下跌，尚无 SC 确认。"
                    "等待天量+下影线的 SC 信号，不建议过早抄底。",
                )
        print(f"[OK] 当前阶段: {self.phase}")
        return self

    def _set_phase(self, name, color, desc):
        self.phase = name
        self.phase_color = color
        self.phase_desc = desc

    # ──────────────────────────────────────────
    #  Step 5: 绘图
    # ──────────────────────────────────────────
    def plot(self, save_path: str = "wyckoff_analysis.png", dpi: int = 150) -> None:
        """
        绘制四层图表:
          ① K线 + 均线 + 支撑阻力 + 威科夫事件标注
          ② 成交量柱 + 20日均量线
          ③ 量比柱（红=超量 橙=放量 灰=缩量）
          ④ 阶段说明文字
        """
        df = self.df
        n = len(df)
        x = np.arange(n)

        # ── 暗色主题 ────────────────────────────
        plt.rcParams.update(
            {
                "font.family": [
                    "DejaVu Sans",
                    "Microsoft YaHei",
                    "PingFang SC",
                    "Arial Unicode MS",
                ],
                "axes.facecolor": "#12121f",
                "figure.facecolor": "#0a0a14",
                "text.color": "#d4d4d4",
                "axes.labelcolor": "#a0a0b0",
                "xtick.color": "#606070",
                "ytick.color": "#606070",
                "grid.color": "#1e1e30",
                "grid.linewidth": 0.8,
                "axes.spines.top": False,
                "axes.spines.right": False,
                "axes.spines.left": False,
                "axes.spines.bottom": False,
            }
        )

        fig = plt.figure(figsize=(20, 15))
        gs = gridspec.GridSpec(
            4, 1, figure=fig, hspace=0.06, height_ratios=[3.8, 1.3, 1.0, 0.9]
        )
        ax_k = fig.add_subplot(gs[0])
        ax_vol = fig.add_subplot(gs[1], sharex=ax_k)
        ax_vr = fig.add_subplot(gs[2], sharex=ax_k)
        ax_info = fig.add_subplot(gs[3])
        ax_info.axis("off")

        # ── ① K线实体 ───────────────────────────
        for i in range(n):
            o, h, l, c = (
                df["Open"].iloc[i],
                df["High"].iloc[i],
                df["Low"].iloc[i],
                df["Close"].iloc[i],
            )
            col = "#ef5350" if c >= o else "#26a69a"
            ax_k.plot([x[i], x[i]], [l, h], color=col, lw=0.9, alpha=0.85)
            rect = plt.Rectangle(
                (x[i] - 0.38, min(o, c)),
                0.76,
                max(abs(c - o), 0.05),
                fc=col,
                ec=col,
                alpha=0.92,
                zorder=3,
            )
            ax_k.add_patch(rect)

        # ── MA20 ────────────────────────────────
        ma20_valid = df["ma20"].notna()
        ax_k.plot(
            x[ma20_valid],
            df["ma20"][ma20_valid].values,
            color="#ffa726",
            lw=1.1,
            alpha=0.8,
            label="MA20",
            zorder=4,
        )

        # ── 支撑位 ──────────────────────────────
        sup_colors = ["#42a5f5", "#64b5f6", "#90caf9", "#bbdefb"]
        for idx, lv in enumerate(self.support_levels):
            cc = sup_colors[idx % len(sup_colors)]
            ax_k.axhline(lv, color=cc, lw=0.9, ls="--", alpha=0.75, zorder=2)
            ax_k.text(
                n + 0.3,
                lv,
                f" S{idx + 1}  ${lv:.1f}",
                color=cc,
                fontsize=8.5,
                va="center",
                bbox=dict(fc="#0a0a14", ec=cc, pad=2, alpha=0.85),
            )

        # ── 阻力位 ──────────────────────────────
        res_colors = ["#ef9a9a", "#e57373", "#ef5350", "#f44336"]
        for idx, lv in enumerate(self.resistance_levels):
            cc = res_colors[idx % len(res_colors)]
            ax_k.axhline(lv, color=cc, lw=0.9, ls="--", alpha=0.75, zorder=2)
            ax_k.text(
                n + 0.3,
                lv,
                f" R{idx + 1}  ${lv:.1f}",
                color=cc,
                fontsize=8.5,
                va="center",
                bbox=dict(fc="#0a0a14", ec=cc, pad=2, alpha=0.85),
            )

        # ── TR 区间背景 ──────────────────────────
        if self.support_levels and self.resistance_levels:
            ax_k.axhspan(
                self.support_levels[0],
                self.resistance_levels[0],
                alpha=0.04,
                color="#42a5f5",
                zorder=1,
            )

        # ── 威科夫事件标注 ───────────────────────
        atr_arr = df["atr14"].values
        for ev in self.events:
            st = self._EVENT_STYLES.get(
                ev["type"], {"color": "#ffffff", "marker": "o", "side": 0}
            )
            ei = ev["index"]
            atr = atr_arr[ei] if not np.isnan(atr_arr[ei]) else 2.0
            ep = ev["price"]
            offset = st["side"] * atr * 1.2

            ax_k.scatter(
                x[ei],
                ep + offset,
                marker=st["marker"],
                color=st["color"],
                s=130,
                zorder=6,
                ec="none",
            )
            ax_k.annotate(
                ev["type"],
                xy=(x[ei], ep + offset),
                xytext=(x[ei], ep + offset * 2.8),
                fontsize=8.5,
                color=st["color"],
                fontweight="bold",
                ha="center",
                va="bottom" if st["side"] > 0 else "top",
                arrowprops=dict(arrowstyle="->", color=st["color"], lw=1.0),
                bbox=dict(fc="#0a0a14", ec=st["color"], pad=1.5, alpha=0.9),
                zorder=7,
            )

        # ── 坐标轴 ──────────────────────────────
        p_lo = df["Low"].min()
        p_hi = df["High"].max()
        pad = (p_hi - p_lo) * 0.14
        ax_k.set_ylim(p_lo - pad, p_hi + pad * 2.2)
        ax_k.set_xlim(-0.5, n + 5)
        ax_k.set_ylabel("价格 (USD)", fontsize=10, labelpad=8)
        ax_k.set_title(
            f"{self.ticker}  ·  威科夫量价分析  ·  阶段: {self.phase}",
            color="#e8e8e8",
            fontsize=12,
            fontweight="bold",
            pad=14,
        )
        ax_k.grid(True, alpha=0.5)
        ax_k.legend(loc="upper left", fontsize=9, framealpha=0.2, facecolor="#0a0a14")

        # ── ② 成交量 ────────────────────────────
        vol_bar_colors = [
            "rgba(239,83,80,0.6)"
            if (df["Close"].iloc[i] >= df["Open"].iloc[i])
            else "rgba(38,166,154,0.6)"
            for i in range(n)
        ]
        # 用 matplotlib 颜色（hex + alpha通过facecolor实现）
        vbc = [
            "#ef535099" if df["Close"].iloc[i] >= df["Open"].iloc[i] else "#26a69a99"
            for i in range(n)
        ]
        ax_vol.bar(x, df["Volume"] / 1e6, color=vbc, width=0.72, zorder=3)
        ax_vol.plot(
            x,
            df["vol_ma20"] / 1e6,
            color="#ffa726",
            lw=1.1,
            alpha=0.85,
            label=f"均量 {df['vol_ma20'].iloc[-1] / 1e6:.1f}M",
            zorder=4,
        )
        ax_vol.set_ylabel("成交量 M", fontsize=9, labelpad=8)
        ax_vol.grid(True, alpha=0.5)
        ax_vol.legend(
            loc="upper left", fontsize=8.5, framealpha=0.2, facecolor="#0a0a14"
        )

        # ── ③ 量比 ──────────────────────────────
        vr = df["vol_ratio"].values
        vr_colors = [
            "#ef5350" if v > 1.5 else "#ffa726" if v > 1.0 else "#546e7a" for v in vr
        ]
        ax_vr.bar(x, vr, color=vr_colors, width=0.72, alpha=0.9, zorder=3)
        ax_vr.axhline(1.0, color="#ffa726", lw=1.0, ls="--", alpha=0.7)
        ax_vr.axhline(1.5, color="#ef5350", lw=0.8, ls=":", alpha=0.55)
        ax_vr.set_ylabel("量比", fontsize=9, labelpad=8)
        ax_vr.grid(True, alpha=0.5)

        # ── X 轴日期 ────────────────────────────
        step = max(1, n // 14)
        ticks = x[::step]
        labels = [str(df.index[i])[:10] for i in range(0, n, step)]
        ax_vr.set_xticks(ticks)
        ax_vr.set_xticklabels(labels, rotation=30, ha="right", fontsize=8)
        plt.setp(ax_k.get_xticklabels(), visible=False)
        plt.setp(ax_vol.get_xticklabels(), visible=False)

        # ── ④ 阶段说明 ──────────────────────────
        ax_info.text(
            0.01,
            0.72,
            f"  阶段:  {self.phase}",
            transform=ax_info.transAxes,
            fontsize=10.5,
            color=self.phase_color,
            fontweight="bold",
            va="top",
            bbox=dict(
                fc="#0d1117",
                ec=self.phase_color,
                pad=4,
                alpha=0.92,
                boxstyle="round,pad=0.4",
            ),
        )
        ax_info.text(
            0.01,
            0.35,
            f"  {self.phase_desc}",
            transform=ax_info.transAxes,
            fontsize=9.2,
            color="#b0b0c0",
            va="top",
            wrap=True,
        )

        # 事件图例
        legend_x = 0.62
        shown = {}
        for ev in self.events:
            if ev["type"] not in shown:
                st = self._EVENT_STYLES.get(
                    ev["type"], {"color": "#fff", "label": ev["type"]}
                )
                ax_info.scatter(
                    [], [], marker="o", color=st["color"], s=55, label=st["label"]
                )
                shown[ev["type"]] = True
        if shown:
            ax_info.legend(
                loc="lower right",
                ncol=min(4, len(shown)),
                fontsize=8.5,
                framealpha=0.25,
                facecolor="#0a0a14",
                edgecolor="#303040",
            )

        # 底部数据摘要
        cur = df["Close"].iloc[-1]
        chg = (cur - df["Close"].iloc[0]) / df["Close"].iloc[0] * 100
        summary = (
            f"  当前价: ${cur:.2f}   区间: {chg:+.1f}%   "
            f"最高量: {df['Volume'].max() / 1e6:.1f}M   "
            f"均量: {df['Volume'].mean() / 1e6:.1f}M   "
            f"事件: {len(self.events)} 个   "
            f"支撑: {len(self.support_levels)} 位   "
            f"阻力: {len(self.resistance_levels)} 位"
        )
        ax_info.text(
            0.01,
            0.02,
            summary,
            transform=ax_info.transAxes,
            fontsize=8.5,
            color="#606078",
            va="bottom",
        )

        plt.savefig(
            save_path,
            dpi=dpi,
            bbox_inches="tight",
            facecolor="#0a0a14",
            edgecolor="none",
        )
        plt.close(fig)
        print(f"[OK] 图表已保存 → {save_path}")

    # ──────────────────────────────────────────
    #  打印文字报告
    # ──────────────────────────────────────────
    def print_report(self) -> None:
        cur = float(self.df["Close"].iloc[-1])
        w = 66
        print("\n" + "═" * w)
        print(
            f"  威科夫量价分析报告  ·  {self.ticker}  ({self.period} / {self.interval})"
        )
        print("═" * w)
        print(f"\n  当前价格 : ${cur:.2f}")
        print(f"  当前阶段 : {self.phase}")
        print(f"  阶段解读 : {self.phase_desc}\n")

        print("─" * w)
        print("  关键事件 (时序)")
        print("─" * w)
        if self.events:
            for e in self.events:
                date_str = str(e["date"])[:10]
                print(
                    f"  [{e['type']:6s}]  {date_str}  "
                    f"${e['price']:>8.2f}  量比{e['vol_ratio']:.1f}x  {e['extra']}"
                )
        else:
            print("  — 未检测到明显的威科夫事件，请扩大数据区间或调整参数 —")

        print("\n" + "─" * w)
        print("  支撑位")
        print("─" * w)
        if self.support_levels:
            for i, s in enumerate(self.support_levels):
                pct = (cur - s) / cur * 100
                bar = "█" * int(pct / 1.5)
                print(f"  S{i + 1}  ${s:>8.2f}   距今 {pct:5.1f}%  {bar}")
        else:
            print("  — 暂无支撑位 —")

        print("\n" + "─" * w)
        print("  阻力位")
        print("─" * w)
        if self.resistance_levels:
            for i, r in enumerate(self.resistance_levels):
                pct = (r - cur) / cur * 100
                bar = "█" * int(pct / 1.5)
                print(f"  R{i + 1}  ${r:>8.2f}   距今 {pct:5.1f}%  {bar}")
        else:
            print("  — 暂无阻力位 —")

        print("\n" + "─" * w)
        print("  量价统计")
        print("─" * w)
        df = self.df
        up_vol = df[df["is_bullish"]]["Volume"].mean() / 1e6
        dn_vol = df[~df["is_bullish"]]["Volume"].mean() / 1e6
        bias = "买方占优 ✦ 多头" if up_vol > dn_vol else "卖方占优 ✧ 空头"
        print(f"  上涨日均量 : {up_vol:.1f} M")
        print(f"  下跌日均量 : {dn_vol:.1f} M")
        print(f"  量价偏向   : {bias}")
        print("═" * w + "\n")


# ─────────────────────────────────────────────
#  主程序入口
# ─────────────────────────────────────────────
def main():
    cfg = CONFIG
    print(f"\n{'=' * 55}")
    print(
        f"  威科夫量价分析  ·  {cfg['TICKER']}  ({cfg['PERIOD']} / {cfg['INTERVAL']})"
    )
    print(f"{'=' * 55}\n")

    analyzer = WyckoffAnalyzer(
        ticker=cfg["TICKER"],
        period=cfg["PERIOD"],
        interval=cfg["INTERVAL"],
    )
    (
        analyzer.fetch_data()
        .detect_support_resistance(window=cfg["WINDOW"], n_levels=cfg["N_LEVELS"])
        .detect_wyckoff_events()
        .detect_phase()
    )

    analyzer.print_report()
    analyzer.plot(save_path=cfg["SAVE_PATH"])
    print(f"完成！图表保存为 → {cfg['SAVE_PATH']}\n")


if __name__ == "__main__":
    main()
