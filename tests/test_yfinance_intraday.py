#!/usr/bin/env python3
"""
测试 yfinance 是否能拉取到分钟级别的 K 线数据

注意：项目中的 TickFlow 日线 K 线限频：10/min（即每分钟最多 10 次请求），
但 yfinance 本身没有这样的配置。

yfinance period/interval 限制参考：
- 1d: 1m, 2m, 5m, 15m, 30m, 60m, 90m (max 7 days)
- 5d: 1m, 2m, 5m, 15m, 30m, 60m, 90m (max 60 days)  
- 1mo: 1m, 2m, 5m, 15m, 30m, 60m, 90m (max 5 years)

速率限制：当前网络环境下存在速率限制（"Too Many Requests. Rate limited"），
建议等待 5-10 分钟后重试。

yfinance_ticker_interfaces_probe.py 提供了更详细的测试。
"""

import yfinance as yf
from datetime import datetime


def test_chinese_stock():
    """测试 A 股（以 000001.SZ 为例）"""
    print("=" * 70)
    print("测试 A 股：000001.SZ (平安银行)")
    print("=" * 70)
    
    ticker = yf.Ticker("000001.SZ")
    
    # 获取最近 1 天的 1 分钟数据
    print("\n尝试获取最近 1 天的 1 分钟数据...")
    try:
        data_1m = ticker.history(period="1d", interval="1m")
        
        if not data_1m.empty:
            print(f"✓ 成功获取 {len(data_1m)} 条 1 分钟数据")
            
            # 只取最近 10 分钟
            last_10 = data_1m.tail(10)
            print(f"\n最近 10 分钟数据:")
            print(last_10)
            print(f"\n时间范围: {last_10.index[0]} ~ {last_10.index[-1]}")
        else:
            print("✗ 返回空数据")
            
    except Exception as e:
        print(f"✗ 错误: {e}")


def test_us_stock():
    """测试美股（以 AAPL 为例）"""
    print("\n" + "=" * 70)
    print("测试 美股：AAPL (Apple)")
    print("=" * 70)
    
    ticker = yf.Ticker("AAPL")
    
    # 获取最近 1 天的 1 分钟数据
    print("\n尝试获取最近 1 天的 1 分钟数据...")
    try:
        data_1m = ticker.history(period="1d", interval="1m")
        
        if not data_1m.empty:
            print(f"✓ 成功获取 {len(data_1m)} 条 1 分钟数据")
            
            # 只取最近 10 分钟
            last_10 = data_1m.tail(10)
            print(f"\n最近 10 分钟数据:")
            print(last_10)
            print(f"\n时间范围: {last_10.index[0]} ~ {last_10.index[-1]}")
        else:
            print("✗ 返回空数据")
            
    except Exception as e:
        print(f"✗ 错误: {e}")


def test_different_intervals():
    """测试不同时间间隔"""
    print("\n" + "=" * 70)
    print("测试不同时间间隔组合")
    print("=" * 70)
    
    test_cases = [
        ("AAPL", "US", "1d", "1m", "最近1天，1分钟"),
        ("AAPL", "US", "1d", "5m", "最近1天，5分钟"),
        ("000001.SZ", "CN", "1d", "5m", "最近1天，5分钟"),
        ("AAPL", "US", "5d", "5m", "最近5天，5分钟"),
    ]
    
    for symbol, market, period, interval, desc in test_cases:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            
            if not data.empty:
                print(f"{desc} ({symbol}, period={period}, interval={interval}): ✓ {len(data)} 条")
                if len(data) >= 10:
                    print(f"  最近 10 行时间: {data.index[-10]} ~ {data.index[-1]}")
            else:
                print(f"{desc} ({symbol}, period={period}, interval={interval}): ✗ 空数据")
        except Exception as e:
            print(f"{desc} ({symbol}, period={period}, interval={interval}): ✗ {e}")


if __name__ == "__main__":
    print(f"测试时间: {datetime.now()}")
    print(f"yfinance 版本: {yf.__version__}")
    print("\n注意：只拉取最近 10 分钟的数据，避免数据量过大。")
    print("速率限制说明：如果遇到 'Too Many Requests. Rate limited' 错误，请等待 5-10 分钟后重试。")
    
    test_chinese_stock()
    test_us_stock()
    test_different_intervals()
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)
