"""只管理固定盘前任务的HTTP客户端，不提供普通研究或私人数据能力。"""

from typing import Literal
from uuid import UUID

import httpx
from pydantic import ValidationError

from stock_research.query_client import MAX_RESPONSE_BYTES
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.premarket import PremarketInput, PremarketReceipt
from stock_research.workflow_settings import WorkflowSettings


class WorkflowError(ValueError):
    """固定安全错误，不含令牌、响应正文或连接异常详情。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__("盘前请求未确认：" + code)


class PremarketClient:
    def __init__(
        self, settings: WorkflowSettings, data: PremarketInput, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.settings, self.data, self.transport = settings, data, transport

    async def call(
        self,
        method: Literal["GET", "POST"],
        *,
        job_id: UUID | None = None,
        action: Literal["retry", "cancel"] | None = None,
    ) -> PremarketReceipt:
        """路径仅由UUID和固定动作构造，正文只有原计划元数据。"""
        if job_id is not None and not isinstance(job_id, UUID):
            raise WorkflowError("workflow_action_invalid")
        if (job_id is None and (method != "POST" or action is not None)) or action not in {None, "retry", "cancel"}:
            raise WorkflowError("workflow_action_invalid")
        if job_id is not None and method != ("POST" if action else "GET"):
            raise WorkflowError("workflow_action_invalid")
        path = "/v1/premarket-jobs" + (f"/{job_id}" if job_id else "") + (f"/{action}" if action else "")
        try:
            async with httpx.AsyncClient(
                base_url=self.settings.endpoint,
                headers={"Authorization": "Bearer " + self.settings.token.get_secret_value()},
                trust_env=False,
                follow_redirects=False,
                timeout=130 if job_id is None else 20,
                transport=self.transport,
            ) as client:
                body = {"json": self.data.model_dump(mode="json")} if job_id is None else {}
                async with client.stream(method, path, **body) as response:
                    parts, size = [], 0
                    async for part in response.aiter_bytes():
                        size += len(part)
                        if size > MAX_RESPONSE_BYTES:
                            raise WorkflowError("response_too_large")
                        parts.append(part)
                    if response.status_code not in {200, 202}:
                        raise WorkflowError(
                            "request_conflict"
                            if response.status_code == 409
                            else "request_unavailable"
                            if response.status_code >= 500
                            else "request_denied"
                        )
            packet = ApiResponse[PremarketReceipt].model_validate_json(b"".join(parts))
            if packet.code != Code.OK or packet.data is None:
                raise WorkflowError("response_invalid")
            receipt = packet.data
            if receipt.plan.schedule != self.data.schedule or (job_id and receipt.job_id != job_id):
                raise WorkflowError("scope_mismatch")
            return receipt
        except httpx.HTTPError:
            raise WorkflowError("transport_unknown") from None
        except ValidationError:
            raise WorkflowError("response_invalid") from None
