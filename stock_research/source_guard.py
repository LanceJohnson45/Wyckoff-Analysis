"""固定本进程已载入源码的版本，拒绝在编辑中把新磁盘摘要贴给旧代码。"""

import hashlib
from pathlib import Path

from stock_research.errors import ResearchSourceChanged
from stock_research.identity import digest
from stock_research.wyckoff import engine_hash


def source_identity() -> str:
    """递归相对路径避免嵌套schema漏扫或同名文件相互覆盖。"""
    root = Path(__file__).parent
    return digest(
        {
            "research": {
                str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(root.rglob("*.py"))
            },
            "production_engine": engine_hash(),
        }
    )


def ensure_source_current() -> None:
    """旧进程可以读事实及收尾，不能冻结或运行新的研究阶段。"""
    if source_identity() != LOADED_SOURCE_IDENTITY:
        raise ResearchSourceChanged("研究代码已更新，请重启研究API后重试；旧报告仍可读取")


# lint-redline:ignore PS-10 必须在模块载入时固定；请求时重算会把旧进程伪装成新源码版本。
LOADED_SOURCE_IDENTITY = source_identity()
