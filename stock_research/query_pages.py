"""外部MCP的完整JSON分片与有界暂存；续页永远不重新读取latest。"""

import asyncio
import json
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass

from pydantic import BaseModel, JsonValue

from stock_research.identity import digest, new_id
from stock_research.query_client import MAX_RESPONSE_BYTES, QueryError
from stock_research.schemas.query_pages import QueryChunk, QueryPage, QueryScope

MAX_PAGE_BYTES = 32 * 1024
MAX_CHUNK_BYTES = 8 * 1024
MAX_CACHED_BYTES = 8 * 1024 * 1024
MAX_CACHED_RESULTS = 8
RESULT_TTL_SECONDS = 300
STRING_CHARS = 1024
CURSOR_ERROR = "续页已失效或不属于本工具及原查询；请重新查询并核对报告/运行版本，不拼接旧页"


def chunks(value: JsonValue, path: tuple[str | int, ...] = ()) -> Iterator[QueryChunk]:
    """仅拆过大子树；保留空集合与标量类型，长文本不按字节切坏Unicode。"""
    item = QueryChunk(path=path, value=value)
    if len(item.model_dump_json(indent=2).encode()) <= MAX_CHUNK_BYTES:
        yield item
    elif isinstance(value, dict | list):
        for key, child in value.items() if isinstance(value, dict) else enumerate(value):
            yield from chunks(child, (*path, key))
    elif isinstance(value, str):
        for offset in range(0, len(value), STRING_CHARS):
            part = QueryChunk(
                path=path, value=value[offset : offset + STRING_CHARS], text_offset=offset, text_total=len(value)
            )
            if len(part.model_dump_json(indent=2).encode()) > MAX_CHUNK_BYTES:
                raise QueryError("查询字段路径过长，无法完整分页；请核对来源资料")
            yield part
    else:
        raise QueryError("查询字段过长，无法完整分页；请核对来源资料")


def build_pages(tool: str, model: BaseModel) -> tuple[QueryPage, ...]:
    """按最终SDK格式化文本计量，预留最长页号/游标后再填写实际总数。"""
    body = model.model_dump(mode="json")
    if not isinstance(body, dict) or len(json.dumps(body, ensure_ascii=False).encode()) > MAX_RESPONSE_BYTES:
        raise QueryError("查询结果超过完整分页上限，请缩小查询范围")
    run = body.get("run", body)
    scope = QueryScope.model_validate(
        {
            **{key: run[key] for key in QueryScope.model_fields if key in run},
            **{key: body[key] for key in ("report_id", "revision") if key in body},
        }
    )
    identifier = new_id()
    template = QueryPage(
        result_id=identifier,
        tool=tool,
        content_hash=digest(body),
        scope=scope,
        page=999999,
        total_pages=999999,
        next_cursor=f"{identifier}:999999",
        chunks=(),
    )
    groups, current = [], []
    # 根对象逐字段发送，避免只有单页时根路径没有可放回的位置。
    for key, value in body.items():
        for item in chunks(value, (key,)):
            candidate = template.model_copy(update={"chunks": (*current, item)})
            if len(candidate.model_dump_json(indent=2).encode()) > MAX_PAGE_BYTES:
                if not current:
                    raise QueryError("查询字段超过完整分页上限，请核对来源资料")
                groups.append(tuple(current))
                current = []
            current.append(item)
    groups.append(tuple(current))
    return tuple(
        template.model_copy(
            update={
                "page": index + 1,
                "total_pages": len(groups),
                "chunks": group,
                "next_cursor": f"{identifier}:{index + 2}" if index + 1 < len(groups) else None,
            }
        )
        for index, group in enumerate(groups)
    )


@dataclass
class CachedQuery:
    key: str
    expires: float
    size: int
    pages: tuple[QueryPage, ...]


class QueryPages:
    """进程退出、固定期限或容量淘汰后拒绝续页，不把新报告伪装成旧结果。"""

    def __init__(self) -> None:
        self.results: OrderedDict[str, CachedQuery] = OrderedDict()

    async def respond(
        self,
        tool: str,
        arguments: dict[str, JsonValue],
        operation: Callable[[], Awaitable[BaseModel]],
        cursor: str | None = None,
    ) -> QueryPage:
        """查询身份只负责首读；游标同时绑定工具和全部规范入参。"""
        self.results = OrderedDict((k, v) for k, v in self.results.items() if v.expires > time.monotonic())
        key = digest({"tool": tool, "arguments": arguments})
        if cursor is not None:
            return self.continue_result(key, cursor)
        pages = await asyncio.to_thread(build_pages, tool, await operation())
        size = sum(len(page.model_dump_json(indent=2).encode()) for page in pages)
        if size > MAX_CACHED_BYTES:
            raise QueryError("查询分页超过暂存上限，请缩小查询范围")
        while self.results and (
            len(self.results) >= MAX_CACHED_RESULTS
            or sum(v.size for v in self.results.values()) + size > MAX_CACHED_BYTES
        ):
            self.results.popitem(last=False)
        self.results[str(pages[0].result_id)] = CachedQuery(key, time.monotonic() + RESULT_TTL_SECONDS, size, pages)
        return pages[0]

    def continue_result(self, key: str, cursor: str) -> QueryPage:
        """拒绝非规范页码及任何工具/参数不匹配；重复读取不改变结果或期限。"""
        identifier, separator, number = cursor.partition(":")
        result = self.results.get(identifier)
        if not separator or result is None or result.key != key or not number.isascii() or not number.isdecimal():
            raise QueryError(CURSOR_ERROR)
        index = int(number) if len(number) <= 6 else 0
        if str(index) != number or not 1 <= index <= len(result.pages):
            raise QueryError(CURSOR_ERROR)
        self.results.move_to_end(identifier)
        return result.pages[index - 1]
