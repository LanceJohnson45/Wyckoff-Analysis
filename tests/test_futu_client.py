#!/usr/bin/env python3
"""
Futu Client 测试脚本

测试内容：
1. 连接 Futu OpenD
2. 订阅/取消订阅个股
3. 查询订阅状态
4. 拉取实时数据
5. 实时数据推送测试
"""

from __future__ import annotations

import argparse
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrations.futu_client import (
    FutuClient,
    FutuConfig,
    FutuDataHandler,
    PrintDataHandler,
    create_futu_client,
)


class CustomDataHandler(FutuDataHandler):
    """自定义数据处理器"""
    
    def on_quote(self, data):
        print(f"[CustomHandler] 收到报价: {data.code} = {data.price:.2f}")
    
    def on_rt_data(self, data):
        print(f"[CustomHandler] 收到分时: {data.get('code', 'N/A')} - {data.get('data', [])[:3]}")


def test_basic_operations(host: str, port: int, code: str, firm: str | None):
    """测试基本操作"""
    print("=" * 70)
    print("测试基本操作")
    print("=" * 70)
    
    client = FutuClient(
        config=FutuConfig(
            opend_host=host,
            opend_port=port,
            security_firm=firm,
        ),
        data_handler=PrintDataHandler(),
    )
    
    # 连接
    if not client.connect():
        print("连接失败")
        return False
    
    try:
        # 订阅
        print(f"\n订阅 {code}...")
        client.subscribe(code, ["QUOTE", "RT_DATA"])
        
        # 查询状态
        status = client.query_subscription()
        print(f"\n订阅状态:")
        print(f"  已订阅数量: {status.get('subscribed_count')}")
        print(f"  股票列表: {status.get('subscribed_codes')}")
        
        # 主动拉取报价
        quote = client.get_quote(code)
        if quote:
            print(f"\n实时报价:")
            print(f"  代码: {quote.get('code')}")
            print(f"  名称: {quote.get('name')}")
            print(f"  价格: {quote.get('price')}")
            print(f"  时间: {quote.get('time')}")
        
        # 主动拉取分时
        rt_data = client.get_rt_data(code)
        if rt_data:
            print(f"\n分时数据: {len(rt_data)} 条")
        
        # 取消订阅
        print(f"\n取消订阅 {code}...")
        client.unsubscribe(code)
        
        # 再次查询
        status = client.query_subscription()
        print(f"\n取消后状态: {status.get('subscribed_count')} 个订阅")
        
        return True
        
    finally:
        client.disconnect()


def test_multiple_stocks(host: str, port: int, codes: list[str], firm: str | None):
    """测试多股票订阅"""
    print("\n" + "=" * 70)
    print("测试多股票订阅")
    print("=" * 70)
    
    client = create_futu_client(host, port, firm)
    
    if not client.connect():
        print("连接失败")
        return False
    
    try:
        # 批量订阅
        print("\n批量订阅...")
        for code in codes:
            client.subscribe(code, ["QUOTE", "ORDER_BOOK"])
        
        # 查询状态
        status = client.query_subscription()
        print(f"\n订阅状态: {status.get('subscribed_count')} 个股票")
        
        # 取消所有
        print("\n取消所有订阅...")
        client.unsubscribe_all()
        
        return True
        
    finally:
        client.disconnect()


def test_custom_handler(host: str, port: int, code: str, firm: str | None):
    """测试自定义数据处理器"""
    print("\n" + "=" * 70)
    print("测试自定义数据处理器")
    print("=" * 70)
    
    client = create_futu_client(host, port, firm, CustomDataHandler())
    
    if not client.connect():
        print("连接失败")
        return False
    
    try:
        print(f"\n订阅 {code} (使用自定义处理器)...")
        client.subscribe(code, ["QUOTE"], subscribe_push=True)
        
        print("\n等待 5 秒接收推送...")
        client.start_push(duration=5)
        
        return True
        
    finally:
        client.disconnect()


def test_push_with_print_handler(host: str, port: int, code: str, firm: str | None):
    """测试推送（使用默认处理器）"""
    print("\n" + "=" * 70)
    print("测试推送（默认处理器）")
    print("=" * 70)
    
    client = create_futu_client(host, port, firm)
    
    if not client.connect():
        print("连接失败")
        return False
    
    try:
        print(f"\n订阅 {code} 并启动推送...")
        client.subscribe(code, ["QUOTE", "ORDER_BOOK", "RT_DATA"], subscribe_push=True)
        
        print("\n启动推送 10 秒...")
        client.start_push(duration=10)
        
        return True
        
    finally:
        client.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Futu Client 测试")
    parser.add_argument("--host", default="127.0.0.1", help="OpenD 主机")
    parser.add_argument("--port", type=int, default=11111, help="OpenD 端口")
    parser.add_argument("--firm", default=None, help="券商标识 (FUTUSECURITIES/FUTUINC/FUTUSG)")
    parser.add_argument("--code", default="US.AAPL", help="测试股票代码")
    parser.add_argument("--codes", default="US.AAPL,US.MSFT,US.GOOG", help="多股票测试代码（逗号分隔）")
    parser.add_argument("--test", choices=["basic", "multi", "custom", "push", "all"], 
                       default="all", help="测试类型")
    
    args = parser.parse_args()
    
    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    
    if args.test == "basic" or args.test == "all":
        test_basic_operations(args.host, args.port, args.code, args.firm)
    
    if args.test == "multi" or args.test == "all":
        test_multiple_stocks(args.host, args.port, codes, args.firm)
    
    if args.test == "custom" or args.test == "all":
        test_custom_handler(args.host, args.port, args.code, args.firm)
    
    if args.test == "push" or args.test == "all":
        test_push_with_print_handler(args.host, args.port, args.code, args.firm)
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)


if __name__ == "__main__":
    main()
