#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单股威科夫诊断脚本

用法:
  # A股
  python scripts/diagnose_single.py 000001
  python scripts/diagnose_single.py 600519 --cost 180.5
  
  # 美股
  python scripts/diagnose_single.py AAPL --market us
  python scripts/diagnose_single.py AAPL --market us --cost 150.0
  
  # 港股
  python scripts/diagnose_single.py 00700 --market hk
  python scripts/diagnose_single.py 09988 --market hk --cost 120.0
"""
import argparse
import sys
from pathlib import Path

# 添加项目根目录到 sys.path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datetime import date, timedelta
from integrations.stock_hist_repository import get_stock_hist
from core.holding_diagnostic import diagnose_one_stock, format_diagnostic_text
from core.stock_cache import _COL_MAP


def _code_to_name(code: str) -> str:
    """简易代码转名称"""
    try:
        from integrations.data_source import search_stock_by_name
        results = search_stock_by_name(code, limit=1)
        if results:
            return results[0].get("name", code)
    except Exception:
        pass
    return code


def main():
    parser = argparse.ArgumentParser(description="单股威科夫诊断")
    parser.add_argument("code", help="股票代码 (A股6位数字 / 美股ticker / 港股5位数字)")
    parser.add_argument("--market", default="cn", choices=["cn", "us", "hk"], help="市场: cn(A股) / us(美股) / hk(港股), 默认 cn")
    parser.add_argument("--cost", type=float, default=0.0, help="持仓成本价 (可选)")
    parser.add_argument("--days", type=int, default=320, help="分析天数, 默认 320")
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"🔍 威科夫诊断")
    print(f"{'='*60}")
    print(f"市场: {args.market.upper()}")
    print(f"代码: {args.code}")
    if args.cost > 0:
        print(f"成本: {args.cost:.2f}")
    print(f"{'='*60}\n")
    
    try:
        # 拉取行情数据
        end_date = date.today()
        start_date = end_date - timedelta(days=500)
        
        df = get_stock_hist(args.code, start_date, end_date, market=args.market)
        
        if df is None or df.empty:
            print(f"❌ 无法获取 {args.code} 的行情数据")
            sys.exit(1)
        
        # 列名标准化
        df = df.rename(columns=_COL_MAP)
        
        # 获取股票名称
        name = _code_to_name(args.code)
        
        # 执行诊断
        result = diagnose_one_stock(args.code, name, args.cost, df)
        text = format_diagnostic_text(result)
        
        # 输出诊断结果
        print(f"📊 {result.name} ({result.code})")
        print(f"\n健康度: {result.health}")
        
        if args.cost > 0:
            print(f"盈亏: {result.pnl_pct:+.2f}%")
        
        print(f"最新价: {result.latest_close:.2f}")
        print(f"均线结构: {result.ma_pattern}")
        print(f"L2通道: {result.l2_channel}")
        print(f"轨道: {result.track}")
        
        if result.accum_stage:
            print(f"吸筹阶段: {result.accum_stage}")
        
        if result.l4_triggers:
            print(f"L4信号: {', '.join(result.l4_triggers)}")
        
        if result.exit_signal:
            print(f"⚠️  退出信号: {result.exit_signal}")
        
        if result.stop_loss_status:
            print(f"🛑 止损状态: {result.stop_loss_status}")
        
        print(f"\n量比(20/60): {result.vol_ratio_20_60:.2f}")
        print(f"60日振幅: {result.range_60d_pct:.1f}%")
        
        # 趋势预判与操作建议
        if result.trend_outlook:
            print(f"\n{'='*60}")
            print("📈 趋势预判与操作建议")
            print(f"{'='*60}")
            print(f"趋势判断: {result.trend_outlook}")
            if result.next_resistance:
                print(f"下一压力: {result.next_resistance:.2f}")
            if result.next_support:
                print(f"下一支撑: {result.next_support:.2f}")
            print(f"\n💡 操作建议: {result.action_plan}")
            if result.action_condition:
                print(f"触发条件: {result.action_condition}")
        
        print(f"\n{'='*60}")
        print("📝 完整诊断:")
        print(f"{'='*60}")
        print(text)
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"❌ 执行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
