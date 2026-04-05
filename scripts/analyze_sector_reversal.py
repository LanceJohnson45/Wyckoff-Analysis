# -*- coding: utf-8 -*-
"""
板块"一日游"现象量化分析

用 tushare 拉取 2025-10 至 2026-04-03 的真实行业指数日线数据，
统计板块隔日反转、连续性等特征，评估当前板块选择策略是否适配。

用法:
    python3 scripts/analyze_sector_reversal.py
"""
from __future__ import annotations

import os
import sys
import time
from collections import defaultdict

import numpy as np
import pandas as pd

# Ensure project root is on sys.path for direct script invocation
if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrations.tushare_client import get_pro


# ── 申万一级行业指数代码 (tushare index_classify + index_daily) ──
# tushare 的行业指数用 SW (申万) 体系，代码格式为 8xxxxx.SI
# 我们直接用 tushare 的 index_classify 接口拉取申万一级行业列表

def fetch_sw_l1_members(pro) -> pd.DataFrame:
    """拉取申万一级行业分类列表"""
    df = pro.index_classify(level="L1", src="SW2021")
    if df is None or df.empty:
        raise RuntimeError("无法获取申万行业分类")
    return df  # columns: index_code, industry_name, ...


def fetch_sector_daily(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """拉取单个申万行业指数日线（使用 sw_daily 接口）"""
    df = pro.sw_daily(ts_code=ts_code, start_date=start, end_date=end)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.sort_values("trade_date").reset_index(drop=True)
    # sw_daily 的涨跌幅列叫 pct_change（不是 pct_chg）
    if "pct_change" in df.columns:
        df["pct_chg"] = pd.to_numeric(df["pct_change"], errors="coerce")
    elif "pct_chg" in df.columns:
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
    else:
        # 手动计算
        close = pd.to_numeric(df["close"], errors="coerce")
        df["pct_chg"] = close.pct_change() * 100
    return df


def main():
    pro = get_pro()
    if pro is None:
        print("✘ TUSHARE_TOKEN 未配置", file=sys.stderr)
        sys.exit(1)

    START = "20251001"
    END = "20260403"

    print("=" * 70)
    print("  A股板块「一日游」现象量化分析")
    print(f"  数据区间: {START[:4]}-{START[4:6]}-{START[6:]} → {END[:4]}-{END[4:6]}-{END[6:]}")
    print("=" * 70)

    # ── Step 1: 获取申万一级行业列表 ──
    print("\n📂 拉取申万一级行业列表...")
    sw_df = fetch_sw_l1_members(pro)
    sectors = list(zip(sw_df["index_code"], sw_df["industry_name"]))
    print(f"  共 {len(sectors)} 个一级行业")

    # ── Step 2: 逐个拉取行业指数日线 ──
    print("\n📊 拉取行业指数日线数据...")
    sector_data: dict[str, pd.DataFrame] = {}
    for ts_code, name in sectors:
        try:
            df = fetch_sector_daily(pro, ts_code, START, END)
            if not df.empty and len(df) > 10:
                sector_data[name] = df
                print(f"  ✔ {name} ({ts_code}): {len(df)} 条")
            else:
                print(f"  ✘ {name} ({ts_code}): 数据不足")
        except Exception as e:
            print(f"  ✘ {name} ({ts_code}): {e}")
        time.sleep(6.5)  # tushare sw_daily 限速: 10次/分钟

    if not sector_data:
        print("✘ 未拉取到任何行业数据", file=sys.stderr)
        sys.exit(1)

    print(f"\n  成功拉取 {len(sector_data)} 个行业")

    # ── Step 3: 核心分析 — 板块隔日反转 ──
    print("\n" + "=" * 70)
    print("  分析一: 板块隔日反转率 (今日涨幅 Top → 次日涨跌)")
    print("=" * 70)

    # 构建所有行业的日收益率矩阵
    all_dates = set()
    for name, df in sector_data.items():
        all_dates.update(df["trade_date"].tolist())
    all_dates = sorted(all_dates)

    # 日收益率矩阵: date × sector
    ret_matrix = pd.DataFrame(index=all_dates)
    for name, df in sector_data.items():
        df_indexed = df.set_index("trade_date")
        ret_matrix[name] = df_indexed["pct_chg"]
    ret_matrix = ret_matrix.dropna(how="all").sort_index()

    total_days = len(ret_matrix)
    print(f"\n  有效交易日: {total_days}")
    print(f"  行业数: {len(ret_matrix.columns)}")

    # ── 分析3.1: 每日涨幅 Top5 板块，次日表现 ──
    top_n_list = [3, 5]
    for top_n in top_n_list:
        reversal_count = 0
        continuation_count = 0
        total_valid = 0
        next_day_returns = []

        for i in range(len(ret_matrix) - 1):
            today = ret_matrix.iloc[i].dropna()
            tomorrow = ret_matrix.iloc[i + 1]

            # 今日涨幅 Top N
            top_sectors = today.nlargest(top_n).index.tolist()

            for sec in top_sectors:
                if sec in tomorrow.index and pd.notna(tomorrow[sec]):
                    today_ret = today[sec]
                    tmr_ret = tomorrow[sec]
                    next_day_returns.append({
                        "date": ret_matrix.index[i],
                        "sector": sec,
                        "today_ret": today_ret,
                        "next_ret": tmr_ret,
                    })
                    total_valid += 1
                    if today_ret > 1.0 and tmr_ret < 0:
                        reversal_count += 1
                    if today_ret > 1.0 and tmr_ret > 0:
                        continuation_count += 1

        ndr = pd.DataFrame(next_day_returns)
        if not ndr.empty:
            # 只看今日涨幅 >1% 的 (有意义的上涨)
            strong_up = ndr[ndr["today_ret"] > 1.0]
            if not strong_up.empty:
                avg_next = strong_up["next_ret"].mean()
                median_next = strong_up["next_ret"].median()
                pct_negative = (strong_up["next_ret"] < 0).mean() * 100
                pct_drop_1 = (strong_up["next_ret"] < -1.0).mean() * 100

                print(f"\n  ── 每日涨幅 Top{top_n}，今日涨>1% 的板块 (共{len(strong_up)}例) ──")
                print(f"  次日平均收益:     {avg_next:+.2f}%")
                print(f"  次日中位数收益:   {median_next:+.2f}%")
                print(f"  次日下跌概率:     {pct_negative:.1f}%")
                print(f"  次日跌>1%概率:    {pct_drop_1:.1f}%")

            # 今日涨>2% 的强势板块
            very_strong = ndr[ndr["today_ret"] > 2.0]
            if not very_strong.empty:
                avg_next2 = very_strong["next_ret"].mean()
                pct_neg2 = (very_strong["next_ret"] < 0).mean() * 100
                pct_drop2 = (very_strong["next_ret"] < -1.0).mean() * 100
                print(f"\n  ── 每日涨幅 Top{top_n}，今日涨>2% 的板块 (共{len(very_strong)}例) ──")
                print(f"  次日平均收益:     {avg_next2:+.2f}%")
                print(f"  次日下跌概率:     {pct_neg2:.1f}%")
                print(f"  次日跌>1%概率:    {pct_drop2:.1f}%")

    # ── 分析3.2: 板块连涨持续性 ──
    print(f"\n{'=' * 70}")
    print("  分析二: 板块连涨持续性")
    print("=" * 70)

    streak_stats = defaultdict(list)
    for name in ret_matrix.columns:
        series = ret_matrix[name].dropna()
        streak = 0
        for val in series:
            if val > 0:
                streak += 1
            else:
                if streak > 0:
                    streak_stats[name].append(streak)
                streak = 0
        if streak > 0:
            streak_stats[name].append(streak)

    all_streaks = []
    for name, streaks in streak_stats.items():
        for s in streaks:
            all_streaks.append(s)

    if all_streaks:
        all_s = pd.Series(all_streaks)
        print(f"\n  所有板块连涨天数统计:")
        print(f"  平均连涨天数:   {all_s.mean():.1f}")
        print(f"  中位数连涨天数: {all_s.median():.0f}")
        print(f"  连涨≥3天占比:   {(all_s >= 3).mean() * 100:.1f}%")
        print(f"  连涨≥5天占比:   {(all_s >= 5).mean() * 100:.1f}%")
        print(f"  连涨=1天占比:   {(all_s == 1).mean() * 100:.1f}%  ← '一日游'")

    # ── 分析3.3: 板块轮动速度 — 每日Top3的重叠率 ──
    print(f"\n{'=' * 70}")
    print("  分析三: 板块轮动速度 (每日Top3重叠率)")
    print("=" * 70)

    overlap_ratios = []
    for i in range(1, len(ret_matrix)):
        prev_top3 = set(ret_matrix.iloc[i - 1].dropna().nlargest(3).index)
        curr_top3 = set(ret_matrix.iloc[i].dropna().nlargest(3).index)
        if prev_top3 and curr_top3:
            overlap = len(prev_top3 & curr_top3) / 3.0
            overlap_ratios.append(overlap)

    if overlap_ratios:
        or_s = pd.Series(overlap_ratios)
        print(f"\n  连续两日 Top3 板块重叠率:")
        print(f"  平均重叠率:   {or_s.mean():.1%}")
        print(f"  完全不重叠:   {(or_s == 0).mean() * 100:.1f}% (板块完全切换)")
        print(f"  重叠≥2/3:     {(or_s >= 0.66).mean() * 100:.1f}% (板块延续)")

    # ── 分析3.4: 月度分解 — 看趋势变化 ──
    print(f"\n{'=' * 70}")
    print("  分析四: 月度「一日游」趋势")
    print("=" * 70)

    ret_matrix_copy = ret_matrix.copy()
    ret_matrix_copy.index = pd.to_datetime(ret_matrix_copy.index, format="%Y%m%d")
    ret_matrix_copy["month"] = ret_matrix_copy.index.to_period("M")

    print(f"\n  {'月份':<10} {'交易日':>5} {'Top3涨>1%例':>10} {'次日跌概率':>10} {'次日均收益':>10} {'Top3重叠率':>10}")
    print("  " + "-" * 60)

    for month, group in ret_matrix_copy.groupby("month"):
        g = group.drop(columns=["month"])
        month_reversal = []
        month_overlap = []

        for i in range(len(g) - 1):
            today = g.iloc[i].dropna()
            tomorrow = g.iloc[i + 1]
            top3 = today.nlargest(3)

            for sec in top3.index:
                if sec in tomorrow.index and pd.notna(tomorrow[sec]) and top3[sec] > 1.0:
                    month_reversal.append(tomorrow[sec])

            prev_set = set(today.nlargest(3).index)
            curr_set = set(g.iloc[i + 1].dropna().nlargest(3).index) if i + 1 < len(g) else set()
            if prev_set and curr_set:
                month_overlap.append(len(prev_set & curr_set) / 3.0)

        if month_reversal:
            mr_s = pd.Series(month_reversal)
            neg_pct = (mr_s < 0).mean() * 100
            avg_ret = mr_s.mean()
        else:
            neg_pct = 0
            avg_ret = 0

        overlap_avg = pd.Series(month_overlap).mean() if month_overlap else 0

        print(f"  {str(month):<10} {len(g):>5}日 {len(month_reversal):>10} {neg_pct:>9.1f}% {avg_ret:>+9.2f}% {overlap_avg:>9.1%}")

    # ── 分析3.5: CONSENSUS_CLIMAX 板块的后续表现 ──
    print(f"\n{'=' * 70}")
    print("  分析五: 模拟 CONSENSUS_CLIMAX 判定 — 高潮板块后续3日表现")
    print("=" * 70)

    # 简化版 CONSENSUS_CLIMAX: 10日涨≥8%, 3日涨≥2%
    climax_next3d = []
    for name in ret_matrix.columns:
        series = ret_matrix[name].dropna()
        if len(series) < 15:
            continue
        for i in range(10, len(series) - 3):
            ret_10d = (1 + series.iloc[i - 9:i + 1] / 100).prod() - 1
            ret_3d_val = (1 + series.iloc[i - 2:i + 1] / 100).prod() - 1
            if ret_10d * 100 >= 8.0 and ret_3d_val * 100 >= 2.0:
                # 后续3日收益
                next_3d = (1 + series.iloc[i + 1:i + 4] / 100).prod() - 1
                climax_next3d.append({
                    "sector": name,
                    "date": series.index[i],
                    "ret_10d": ret_10d * 100,
                    "ret_3d": ret_3d_val * 100,
                    "next_3d_ret": next_3d * 100,
                })

    if climax_next3d:
        cdf = pd.DataFrame(climax_next3d)
        print(f"\n  触发 CLIMAX 条件的事件: {len(cdf)} 次")
        print(f"  后续3日平均收益:   {cdf['next_3d_ret'].mean():+.2f}%")
        print(f"  后续3日中位数收益: {cdf['next_3d_ret'].median():+.2f}%")
        print(f"  后续3日下跌概率:   {(cdf['next_3d_ret'] < 0).mean() * 100:.1f}%")
        print(f"  后续3日跌>2%概率:  {(cdf['next_3d_ret'] < -2).mean() * 100:.1f}%")

    # ── 分析3.6: DISAGREEMENT_PULLBACK 后续表现 ──
    print(f"\n{'=' * 70}")
    print("  分析六: 模拟 DISAGREEMENT_PULLBACK 判定 — 分歧回撤板块后续3日表现")
    print("=" * 70)

    pullback_next3d = []
    for name in ret_matrix.columns:
        series = ret_matrix[name].dropna()
        if len(series) < 15:
            continue
        for i in range(10, len(series) - 3):
            ret_10d = (1 + series.iloc[i - 9:i + 1] / 100).prod() - 1
            ret_3d_val = (1 + series.iloc[i - 2:i + 1] / 100).prod() - 1
            if ret_10d * 100 >= 4.0 and ret_3d_val * 100 <= -0.8:
                next_3d = (1 + series.iloc[i + 1:i + 4] / 100).prod() - 1
                pullback_next3d.append({
                    "sector": name,
                    "date": series.index[i],
                    "ret_10d": ret_10d * 100,
                    "ret_3d": ret_3d_val * 100,
                    "next_3d_ret": next_3d * 100,
                })

    if pullback_next3d:
        pdf = pd.DataFrame(pullback_next3d)
        print(f"\n  触发 PULLBACK 条件的事件: {len(pdf)} 次")
        print(f"  后续3日平均收益:   {pdf['next_3d_ret'].mean():+.2f}%")
        print(f"  后续3日中位数收益: {pdf['next_3d_ret'].median():+.2f}%")
        print(f"  后续3日上涨概率:   {(pdf['next_3d_ret'] > 0).mean() * 100:.1f}%")
        print(f"  后续3日涨>2%概率:  {(pdf['next_3d_ret'] > 2).mean() * 100:.1f}%")

    # ── 综合结论 ──
    print(f"\n{'=' * 70}")
    print("  综合结论")
    print("=" * 70)

    if overlap_ratios:
        avg_overlap = pd.Series(overlap_ratios).mean()
        strong_up_df = pd.DataFrame(next_day_returns)
        strong_up_df = strong_up_df[strong_up_df["today_ret"] > 1.0]

        if not strong_up_df.empty:
            reversal_rate = (strong_up_df["next_ret"] < 0).mean()
        else:
            reversal_rate = 0

        print(f"""
  1. 板块轮动速度: Top3 日均重叠率 {avg_overlap:.1%}
     {'→ 轮动极快（<30%），板块延续性差' if avg_overlap < 0.30 else '→ 轮动中等' if avg_overlap < 0.50 else '→ 轮动较慢，板块有延续性'}

  2. 板块"一日游"程度: 涨幅Top板块次日反转率 {reversal_rate:.1%}
     {'→ 反转率极高（>50%），追涨板块风险很大' if reversal_rate > 0.50 else '→ 反转率中等' if reversal_rate > 0.35 else '→ 反转率尚可，板块有一定延续'}

  3. 连涨持续性: 平均连涨 {pd.Series(all_streaks).mean():.1f} 天
     {'→ 大部分板块涨1天就结束，一日游严重' if pd.Series(all_streaks).mean() < 2.0 else '→ 有一定连涨惯性'}
""")

    # ── 策略建议 ──
    print("  当前策略评估:")
    print("  " + "-" * 50)
    print("""
  ✦ SECTOR_STATE_SCORE_BONUS 评估:
    DISAGREEMENT_PULLBACK +0.06 (最高) — """, end="")
    if pullback_next3d:
        pb_win = (pd.DataFrame(pullback_next3d)["next_3d_ret"] > 0).mean()
        print(f"后续3日胜率{pb_win:.0%}，", end="")
        print("逻辑合理" if pb_win > 0.50 else "需要审视")
    else:
        print("样本不足")

    print(f"""
    CONSENSUS_CLIMAX -0.04 (惩罚) — """, end="")
    if climax_next3d:
        cx_loss = (pd.DataFrame(climax_next3d)["next_3d_ret"] < 0).mean()
        print(f"后续3日跌概率{cx_loss:.0%}，", end="")
        print("惩罚力度可能不够" if cx_loss > 0.55 else "惩罚力度合理")
    else:
        print("样本不足")

    print("""
  ✦ layer3_sector_resonance 权重评估:
    20日动量 0.7 + 5日动量 0.3 选热门板块
    → 如果板块一日游严重，20日动量可能太滞后
    → 建议: 增加 3日动量权重，降低 20日权重，捕捉快速轮动

  ✦ 策略改进方向:
    1. 考虑加入板块"轮动速度"指标，快轮动期降低板块权重
    2. CONSENSUS_CLIMAX 惩罚可能需要加大（-0.04 → -0.08 或更多）
    3. 引入"板块连续性因子": 只有连涨≥2日的板块才给正向加分
    4. layer3 的 20d 权重在快轮动期过高，可动态调整
""")


if __name__ == "__main__":
    main()
