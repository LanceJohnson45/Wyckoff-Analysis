"""问答专用知识检索客户端；不做通用代理、写入或任意URL。"""

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from stock_research.knowledge_settings import KnowledgeSettings
from stock_research.query_client import QueryError

MAX_RESPONSE_BYTES = 1_000_000


class KnowledgeHit(BaseModel):
    """服务字段白名单；镜像后端的索引元数据不进入模型证据。"""

    model_config = ConfigDict(extra="ignore")

    document_id: str = Field(min_length=1, max_length=256)
    source_id: str = Field(min_length=1, max_length=256)
    revision: int = Field(ge=1)
    title: str = Field(min_length=1, max_length=1000)
    evidence: str = Field(max_length=10000)
    valid_from: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}T")


class KnowledgePage(BaseModel):
    """只保留引用所需的稳定版本与原文片段。"""

    model_config = ConfigDict(extra="ignore")

    results: tuple[KnowledgeHit, ...] = Field(max_length=50)
    backend: str = Field(min_length=1, max_length=64)


class KnowledgeClient:
    """每次读取独立客户端；作用域只由专用reader凭据决定。"""

    def __init__(self, settings: KnowledgeSettings, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.settings, self.transport = settings, transport

    async def search(self, query: str) -> KnowledgePage:
        """只转发查询词；重定向、跨网环境和超大响应都拒绝。"""
        try:
            async with httpx.AsyncClient(
                base_url=self.settings.endpoint,
                headers={"Authorization": "Bearer " + self.settings.token.get_secret_value()},
                timeout=15,
                follow_redirects=False,
                trust_env=False,
                transport=self.transport,
            ) as client:
                async with client.stream("GET", "/internal/knowledge/search", params={"q": query}) as response:
                    parts, size = [], 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_RESPONSE_BYTES:
                            raise QueryError("知识响应超过上限，请缩小查询范围")
                        parts.append(chunk)
                    raw = b"".join(parts)
                    if response.status_code in {401, 403}:
                        raise QueryError("知识检索身份未授权")
                    if response.status_code == 422:
                        raise QueryError("知识查询无效或超出服务限制")
                    if response.status_code != 200:
                        raise QueryError("知识服务暂不可用")
            return KnowledgePage.model_validate_json(raw)
        except (httpx.HTTPError, ValidationError):
            raise QueryError("知识连接或响应无效") from None
