"""不可覆盖的 JSON 产物及协作式进程锁。"""

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

from stock_research.identity import digest


@contextmanager
def data_lock(root: Path, market: str, as_of: str) -> Iterator[None]:
    """同一运行目录和市场日期仅允许一个协作进程更新或固定数据。"""
    root.mkdir(parents=True, exist_ok=True)
    key = digest({"market": market, "as_of": as_of})
    with (root / f"{key}.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("相同市场和日期的数据任务正在运行") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def write_once(path: Path, content: str) -> None:
    """原子发布完整文件，已有同内容可重用，不同内容拒绝覆盖。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_text(encoding="utf-8") != content:
                    raise ValueError("产物已存在且内容不同，须使用新版本或新快照") from None
        finally:
            temporary.unlink(missing_ok=True)
