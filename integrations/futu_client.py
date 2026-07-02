# -*- coding: utf-8 -*-
"""
Futu OpenAPI 行情客户端集成

提供实时行情数据订阅功能，支持：
- 动态订阅/取消订阅个股
- 实时报价/K线/买卖盘/逐笔/分时数据推送
- 订阅状态管理
"""

from __future__ import annotations

import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from futu import (
    OpenQuoteContext,
    SubType,
    SecurityFirm,
    RET_OK,
    KLType,
)


# ============================================================
# 配置类
# ============================================================

@dataclass
class FutuConfig:
    """Futu OpenAPI 配置"""
    opend_host: str = "127.0.0.1"
    opend_port: int = 11111
    security_firm: Optional[str] = None
    trd_env: str = "SIMULATE"
    ai_type: int = 1  # SDK >= 10.4.6408 支持


def get_futu_config() -> FutuConfig:
    """从环境变量获取配置"""
    return FutuConfig(
        opend_host=os.getenv("FUTU_OPEND_HOST", "127.0.0.1"),
        opend_port=int(os.getenv("FUTU_OPEND_PORT", "11111")),
        security_firm=os.getenv("FUTU_SECURITY_FIRM") or None,
        trd_env=os.getenv("FUTU_TRD_ENV", "SIMULATE"),
    )


# ============================================================
# 实时数据回调处理
# ============================================================

@dataclass
class QuoteData:
    """报价数据"""
    code: str
    name: str
    price: float
    open: float
    high: float
    low: float
    last_close: float
    volume: int
    amount: float
    timestamp: datetime
    ask_price: List[float] = field(default_factory=list)
    ask_volume: List[int] = field(default_factory=list)
    bid_price: List[float] = field(default_factory=list)
    bid_volume: List[int] = field(default_factory=list)


@dataclass
class KlineData:
    """K线数据"""
    code: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    ktype: str


class FutuDataHandler:
    """数据处理回调基类"""
    
    def on_quote(self, data: QuoteData) -> None:
        """报价数据回调"""
        pass
    
    def on_kline(self, data: KlineData) -> None:
        """K线数据回调"""
        pass
    
    def on_orderbook(self, data: Dict[str, Any]) -> None:
        """买卖盘数据回调"""
        pass
    
    def on_ticker(self, data: Dict[str, Any]) -> None:
        """逐笔数据回调"""
        pass
    
    def on_rt_data(self, data: Dict[str, Any]) -> None:
        """分时数据回调"""
        pass


class PrintDataHandler(FutuDataHandler):
    """打印数据的默认处理器"""
    
    def on_quote(self, data: QuoteData) -> None:
        print(f"[{data.timestamp}] {data.code} {data.name} "
              f"价:{data.price:.2f} 开:{data.open:.2f} 高:{data.high:.2f} 低:{data.low:.2f} "
              f"量:{data.volume:,} 成交额:{data.amount:,.2f}")
    
    def on_kline(self, data: KlineData) -> None:
        print(f"[{data.timestamp}] {data.code} {data.ktype} "
              f"O:{data.open:.2f} H:{data.high:.2f} L:{data.low:.2f} C:{data.close:.2f} "
              f"V:{data.volume:,}")
    
    def on_orderbook(self, data: Dict[str, Any]) -> None:
        print(f"[ORDERBOOK] {data}")
    
    def on_ticker(self, data: Dict[str, Any]) -> None:
        print(f"[TICKER] {data}")
    
    def on_rt_data(self, data: Dict[str, Any]) -> None:
        print(f"[RT_DATA] {data}")


# ============================================================
# Futu 客户端核心类
# ============================================================

class FutuClient:
    """Futu OpenAPI 行情客户端"""
    
    def __init__(
        self,
        config: Optional[FutuConfig] = None,
        data_handler: Optional[FutuDataHandler] = None,
    ):
        """
        初始化 Futu 客户端
        
        Args:
            config: 配置对象，不传则使用环境变量
            data_handler: 数据处理回调，不传则使用默认打印处理器
        """
        self.config = config or get_futu_config()
        self.handler = data_handler or PrintDataHandler()
        
        self._context: Optional[OpenQuoteContext] = None
        self._subscribed: Set[str] = set()  # 已订阅的股票代码
        self._subscribed_types: Dict[str, Set[SubType]] = {}  # 每只股票的订阅类型
        self._lock = threading.RLock()
        self._running = False
        
        # 初始化订阅类型映射
        self._subtype_map = {
            "QUOTE": SubType.QUOTE,
            "ORDER_BOOK": SubType.ORDER_BOOK,
            "TICKER": SubType.TICKER,
            "RT_DATA": SubType.RT_DATA,
            "K_1M": SubType.K_1M,
            "K_5M": SubType.K_5M,
            "K_15M": SubType.K_15M,
            "K_30M": SubType.K_30M,
            "K_60M": SubType.K_60M,
            "K_DAY": SubType.K_DAY,
        }
    
    def _create_context(self) -> OpenQuoteContext:
        """创建行情上下文"""
        host, port = self.config.opend_host, self.config.opend_port
        
        kwargs = {
            "host": host,
            "port": port,
        }
        
        if self.config.security_firm:
            firm_enum = getattr(SecurityFirm, self.config.security_firm.upper(), None)
            if firm_enum:
                kwargs["security_firm"] = firm_enum
        
        try:
            ctx = OpenQuoteContext(**kwargs)
            print(f"[FutuClient] 连接成功: {host}:{port}")
            return ctx
        except Exception as e:
            raise RuntimeError(f"连接 Futu OpenD 失败 ({host}:{port}): {e}")
    
    def connect(self) -> bool:
        """建立连接"""
        if self._context is not None:
            print("[FutuClient] 连接已存在")
            return True
        
        try:
            self._context = self._create_context()
            return True
        except Exception as e:
            print(f"[FutuClient] 连接失败: {e}")
            return False
    
    def disconnect(self) -> None:
        """断开连接"""
        if self._context:
            try:
                self._context.close()
                print("[FutuClient] 连接已关闭")
            except Exception:
                pass
            finally:
                self._context = None
    
    def _ensure_connected(self) -> OpenQuoteContext:
        """确保已连接，否则抛出异常"""
        if self._context is None:
            raise RuntimeError("FutuClient 未连接，请先调用 connect()")
        return self._context
    
    # ============================================================
    # 订阅管理
    # ============================================================
    
    def subscribe(
        self,
        code: str,
        subtypes: List[str],
        is_first_push: bool = True,
        subscribe_push: bool = True,
    ) -> bool:
        """
        订阅股票行情
        
        Args:
            code: 股票代码 (如 US.AAPL, HK.00700)
            subtypes: 订阅类型列表 (如 ["QUOTE", "ORDER_BOOK", "RT_DATA"])
            is_first_push: 是否推送缓存数据
            subscribe_push: 是否启用实时推送
            
        Returns:
            bool: 订阅成功/失败
        """
        with self._lock:
            ctx = self._ensure_connected()
            
            # 解析订阅类型
            subtype_list = []
            for st in subtypes:
                st_upper = st.upper()
                if st_upper not in self._subtype_map:
                    print(f"[FutuClient] 不支持的订阅类型: {st}")
                    continue
                subtype_list.append(self._subtype_map[st_upper])
            
            if not subtype_list:
                print(f"[FutuClient] 无效的订阅类型列表: {subtypes}")
                return False
            
            # 执行订阅
            ret, msg = ctx.subscribe(
                [code],
                subtype_list,
                is_first_push=is_first_push,
                subscribe_push=subscribe_push,
            )
            
            if ret != RET_OK:
                print(f"[FutuClient] 订阅失败: {code} - {msg}")
                return False
            
            # 记录订阅状态
            self._subscribed.add(code)
            if code not in self._subscribed_types:
                self._subscribed_types[code] = set()
            self._subscribed_types[code].update(subtype_list)
            
            print(f"[FutuClient] 订阅成功: {code} - {subtypes}")
            return True
    
    def unsubscribe(
        self,
        code: str,
        subtypes: Optional[List[str]] = None,
    ) -> bool:
        """
        取消订阅
        
        Args:
            code: 股票代码
            subtypes: 要取消的类型列表，None 表示取消该股票的所有订阅
            
        Returns:
            bool: 取消成功/失败
        """
        with self._lock:
            ctx = self._ensure_connected()
            
            if code not in self._subscribed:
                print(f"[FutuClient] 未订阅该股票: {code}")
                return False
            
            if subtypes is None:
                # 取消该股票的所有订阅
                subtype_list = list(self._subscribed_types.get(code, set()))
                ret, msg = ctx.unsubscribe([code], subtype_list)
                if ret == RET_OK:
                    del self._subscribed_types[code]
                    self._subscribed.discard(code)
                    print(f"[FutuClient] 取消所有订阅: {code}")
            else:
                # 取消指定类型
                subtype_list = []
                for st in subtypes:
                    st_upper = st.upper()
                    if st_upper in self._subtype_map:
                        subtype_list.append(self._subtype_map[st_upper])
                
                if subtype_list:
                    ret, msg = ctx.unsubscribe([code], subtype_list)
                    if ret == RET_OK:
                        self._subscribed_types[code] -= set(subtype_list)
                        if not self._subscribed_types[code]:
                            del self._subscribed_types[code]
                            self._subscribed.discard(code)
                        print(f"[FutuClient] 取消订阅: {code} - {subtypes}")
            
            return ret == RET_OK
    
    def unsubscribe_all(self) -> bool:
        """取消所有订阅"""
        with self._lock:
            ctx = self._ensure_connected()
            
            if not self._subscribed:
                print("[FutuClient] 没有已订阅的股票")
                return True
            
            ret, msg = ctx.unsubscribe_all()
            if ret == RET_OK:
                self._subscribed.clear()
                self._subscribed_types.clear()
                print(f"[FutuClient] 已取消所有 {len(self._subscribed)} 个订阅")
            
            return ret == RET_OK
    
    def query_subscription(self) -> Dict[str, Any]:
        """查询当前订阅状态"""
        with self._lock:
            ctx = self._ensure_connected()
            
            ret, msg = ctx.query_subscription()
            if ret != RET_OK:
                return {"error": msg}
            
            return {
                "subscribed_count": len(self._subscribed),
                "subscribed_codes": list(self._subscribed),
                "subscribed_types": {k: [str(s) for s in v] 
                                   for k, v in self._subscribed_types.items()},
                "api_response": msg,
            }
    
    # ============================================================
    # 实时数据推送处理
    # ============================================================
    
    def start_push(self, duration: int = 60) -> None:
        """
        启动实时数据推送（阻塞式）
        
        Args:
            duration: 持续时间（秒）
        """
        print(f"[FutuClient] 启动推送 (持续 {duration} 秒)，按 Ctrl+C 停止")
        self._running = True
        start_time = time.time()
        
        try:
            while self._running and (time.time() - start_time) < duration:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[FutuClient] 推送已停止")
        finally:
            self._running = False
    
    def stop_push(self) -> None:
        """停止推送"""
        self._running = False
    
    # ============================================================
    # 主动拉取数据
    # ============================================================
    
    def get_quote(self, code: str) -> Optional[Dict[str, Any]]:
        """获取实时报价"""
        ctx = self._ensure_connected()
        ret, data = ctx.get_stock_quote([code])
        if ret == RET_OK:
            return data.to_dict("records")[0] if not data.empty else None
        return None
    
    def get_orderbook(self, code: str) -> Optional[Dict[str, Any]]:
        """获取买卖盘"""
        ctx = self._ensure_connected()
        ret, data = ctx.get_order_book(code)
        if ret == RET_OK:
            return data.to_dict() if not data.empty else None
        return None
    
    def get_rt_data(self, code: str) -> Optional[Dict[str, Any]]:
        """获取分时数据"""
        ctx = self._ensure_connected()
        ret, data = ctx.get_rt_data(code)
        if ret == RET_OK:
            return data.to_dict("records") if not data.empty else None
        return None
    
    def get_kline(
        self,
        code: str,
        ktype: str = "K_DAY",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取K线数据"""
        ctx = self._ensure_connected()
        
        kl_type = self._subtype_map.get(ktype.upper())
        if kl_type is None:
            print(f"[FutuClient] 不支持的K线类型: {ktype}")
            return None
        
        ret, data = ctx.get_history_kline(
            code,
            kl_type,
            start=start_date,
            end=end_date,
        )
        
        if ret == RET_OK:
            return data.to_dict("records") if not data.empty else None
        return None
    
    # ============================================================
    # 上下文管理
    # ============================================================
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


# ============================================================
# 快捷函数
# ============================================================

def create_futu_client(
    host: str = "127.0.0.1",
    port: int = 11111,
    security_firm: Optional[str] = None,
    handler: Optional[FutuDataHandler] = None,
) -> FutuClient:
    """
    快速创建 FutuClient 实例
    
    Args:
        host: OpenD 主机
        port: OpenD 端口
        security_firm: 券商标识 (如 FUTUSECURITIES, FUTUINC)
        handler: 数据处理回调
        
    Returns:
        FutuClient: 客户端实例
    """
    config = FutuConfig(
        opend_host=host,
        opend_port=port,
        security_firm=security_firm,
    )
    return FutuClient(config=config, data_handler=handler)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Futu 客户端测试")
    parser.add_argument("--code", default="US.AAPL", help="测试股票代码")
    parser.add_argument("--host", default="127.0.0.1", help="OpenD 主机")
    parser.add_argument("--port", type=int, default=11111, help="OpenD 端口")
    parser.add_argument("--firm", default=None, help="券商标识")
    parser.add_argument("--duration", type=int, default=30, help="推送持续时间(秒)")
    
    args = parser.parse_args()
    
    # 创建客户端
    client = FutuClient(
        config=FutuConfig(
            opend_host=args.host,
            opend_port=args.port,
            security_firm=args.firm,
        ),
        data_handler=PrintDataHandler(),
    )
    
    # 连接
    if not client.connect():
        print("连接失败，退出")
        sys.exit(1)
    
    try:
        # 订阅
        print(f"\n订阅 {args.code} 的实时行情...")
        client.subscribe(args.code, ["QUOTE", "ORDER_BOOK", "RT_DATA"])
        
        # 查询订阅状态
        status = client.query_subscription()
        print(f"\n订阅状态: {status}")
        
        # 主动拉取数据
        print(f"\n拉取 {args.code} 的实时报价...")
        quote = client.get_quote(args.code)
        if quote:
            print(f"报价: {quote}")
        
        # 启动推送
        client.start_push(duration=args.duration)
        
    finally:
        # 断开连接
        client.disconnect()
