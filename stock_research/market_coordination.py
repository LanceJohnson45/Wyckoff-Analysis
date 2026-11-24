"""数据库范围的行情维护/写入/复制协议，与来源工作树独立发布。"""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

# 与旧入口的协议键一致，不能按日期、分支或连接别名拆开。
US_MARKET_JOB_KEY = int.from_bytes(b"WYKUS01", "big")
MARKET_DATA_KEY = int.from_bytes(b"WYKDAT1", "big")


async def protect_market_copy(connection: AsyncConnection) -> None:
    """任务锁与数据锁分开：更新父任务的并发写事务不会与自己死锁。"""
    for key in (US_MARKET_JOB_KEY, MARKET_DATA_KEY):
        if not await connection.scalar(select(func.pg_try_advisory_xact_lock(key))):
            raise ValueError("行情来源维护、写入或复制正在运行，请按原目标日重试")
