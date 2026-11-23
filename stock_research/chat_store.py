"""私有问答的幂等、父锚点并发与可独立读回的执行状态。"""

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert

from stock_research.chat_runtime import chat_prompt_identity
from stock_research.errors import ResearchConflict, ResearchIntegrityError
from stock_research.identity import digest, new_id, now
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import CHAT_TURNS, RESEARCH_REPORTS
from stock_research.schemas.chat import ChatContext, ChatHistory, ChatResult, ChatTurnPage, ChatTurnView, CreateChatTurn
from stock_research.source_guard import LOADED_SOURCE_IDENTITY


def chat_view(row: dict[str, object]) -> ChatTurnView:
    """索引、摘要与公开白名单一起检查，不把私有回执路径带入HTTP。"""
    body = verified_payload(row)
    if digest(body["request"]) != row["request_hash"] or body["request"]["question"] != body["question"]:
        raise ResearchIntegrityError("问答请求与持久化摘要不一致")
    if (body["report_id"], body["conversation_id"], body["parent_turn_id"], body["revision"], body["status"]) != (
        row["report_uuid"],
        row["conversation_uuid"],
        row["parent_uuid"],
        row["revision"],
        row["status"],
    ):
        raise ResearchIntegrityError("问答状态索引与内容不一致")
    try:
        return ChatTurnView(
            **{key: value for key, value in body.items() if key not in {"receipt_path", "request", "attempts"}},
            attempts=[
                {
                    **{key: value for key, value in attempt.items() if key != "receipt_path"},
                    "receipt_available": bool(attempt["receipt_path"]),
                }
                for attempt in body.get("attempts", [])
            ],
            id=row["uuid"],
            receipt_available=bool(body["receipt_path"]),
            create_time=row["create_time"],
            update_time=row["update_time"],
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ResearchIntegrityError("已保存问答内容无效") from None


def replay_chat(row: dict[str, object], request: CreateChatTurn) -> ChatTurnView:
    """重放前核对原规范化请求，不允许换问题后复用已受理身份。"""
    body = request.model_dump(mode="json")
    if row["request_hash"] != digest(body) or verified_payload(row)["request"] != body:
        raise ResearchConflict("相同问答幂等键不能更换问题或父上下文")
    return chat_view(row)


class ChatStore:
    """一个workspace的operator私有问答，不是共享行情工具的读取范围。"""

    def __init__(self, store: RunStore) -> None:
        self.store = store

    async def read(self, identifier: str) -> ChatTurnView:
        """UUID查询仍必须带workspace，未知和越权使用相同缺失结果。"""
        return chat_view(await self.store.row(CHAT_TURNS, identifier))

    async def existing(self, key: str, request: CreateChatTurn) -> ChatTurnView | None:
        """已受理请求的查询不消耗新任务容量，也不重新调用模型。"""
        key_hash = digest({"workspace": self.store.workspace, "chat": key})
        async with self.store.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(CHAT_TURNS).where(
                            CHAT_TURNS.c.workspace == self.store.workspace,
                            CHAT_TURNS.c.key_hash == key_hash,
                            CHAT_TURNS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        return replay_chat(dict(row), request) if row else None

    async def create(self, key: str, request: CreateChatTurn) -> ChatTurnView:
        """相同键返回原问答，父锚点只允许一个后继且不能覆盖旧答案。"""
        body = request.model_dump(mode="json")
        key_hash, request_hash = digest({"workspace": self.store.workspace, "chat": key}), digest(body)
        await self.store.row(RESEARCH_REPORTS, str(request.report_id))
        identifier = new_id()
        async with self.store.engine.begin() as connection:
            await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
            await connection.execute(select(func.pg_advisory_xact_lock(int(key_hash[:16], 16) - (1 << 63))))
            existing = (
                (
                    await connection.execute(
                        select(CHAT_TURNS).where(
                            CHAT_TURNS.c.workspace == self.store.workspace,
                            CHAT_TURNS.c.key_hash == key_hash,
                            CHAT_TURNS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing:
                return replay_chat(dict(existing), request)
            parent = await self.read(str(request.parent_turn_id)) if request.parent_turn_id else None
            if parent and (parent.report_id != request.report_id or parent.status in {"pending", "running"}):
                raise ResearchConflict("父问答尚未结束或不属于本报告")
            payload = {
                "request": body,
                "report_id": str(request.report_id),
                "conversation_id": str(parent.conversation_id) if parent else identifier,
                "parent_turn_id": str(parent.id) if parent else None,
                "revision": parent.revision + 1 if parent else 1,
                "question": request.question,
                "status": "pending",
                "answer": None,
                "error_code": None,
                "model": None,
                "context_hash": None,
                "source_hash": LOADED_SOURCE_IDENTITY,
                "prompt_hash": chat_prompt_identity(),
                "tool_calls": 0,
                "receipt_path": None,
                "attempts": [],
            }
            created = await connection.scalar(
                insert(CHAT_TURNS)
                .values(
                    uuid=identifier,
                    workspace=self.store.workspace,
                    report_uuid=str(request.report_id),
                    conversation_uuid=payload["conversation_id"],
                    parent_uuid=payload["parent_turn_id"],
                    revision=payload["revision"],
                    status="pending",
                    key_hash=key_hash,
                    request_hash=request_hash,
                    payload=payload,
                    content_hash=digest(payload),
                )
                .on_conflict_do_nothing()
                .returning(CHAT_TURNS.c.uuid)
            )
            if not created:
                raise ResearchConflict("该父问答已有后续问题，请刷新后再续接")
        return await self.read(identifier)

    async def transition(
        self,
        identifier: str,
        status: str,
        *,
        model: str | None = None,
        result: ChatResult | None = None,
        error: str | None = None,
    ) -> ChatTurnView:
        """终态只提交一次；完成和回答、错误、回执在同一事务落库。"""
        row = await self.store.row(CHAT_TURNS, identifier)
        body = verified_payload(row)
        if body["status"] not in {"pending", "running"}:
            raise ResearchConflict("问答已结束，不能覆盖")
        payload = {
            **body,
            "status": status,
            "model": model or body["model"],
            "answer": result.answer.model_dump(mode="json") if result and result.answer else None,
            "context_hash": result.context_hash if result else body["context_hash"],
            "tool_calls": result.tool_calls if result and not body.get("attempts") else body["tool_calls"],
            "receipt_path": result.receipt_path if result else body["receipt_path"],
            "error_code": result.error_code if result else error,
        }
        if status == "completed" and (not result or result.status != "completed"):
            raise ResearchIntegrityError("问答尚无已验证的完成结果")
        if status in {"failed", "cancelled"} and body.get("attempts") and body["attempts"][-1]["status"] == "running":
            payload["attempts"] = [
                *body["attempts"][:-1],
                {**body["attempts"][-1], "status": status, "error_code": error, "finished_time": now().isoformat()},
            ]
        return await self.write_payload(row, payload)

    async def write_payload(self, row: dict[str, object], payload: dict[str, object]) -> ChatTurnView:
        """状态相同的两次内部推进也必须CAS摘要，避免覆盖已保存尝试。"""
        chat_view({**row, "status": payload["status"], "payload": payload, "content_hash": digest(payload)})
        async with self.store.engine.begin() as connection:
            changed = await connection.execute(
                update(CHAT_TURNS)
                .where(
                    CHAT_TURNS.c.uuid == row["uuid"],
                    CHAT_TURNS.c.workspace == self.store.workspace,
                    CHAT_TURNS.c.status == row["status"],
                    CHAT_TURNS.c.content_hash == row["content_hash"],
                )
                .values(status=payload["status"], payload=payload, content_hash=digest(payload))
            )
            if changed.rowcount != 1:
                raise ResearchConflict("问答状态已由其他执行者更新")
        return await self.read(row["uuid"])

    async def start_attempt(self, identifier: str, context: ChatContext) -> None:
        """落库后才调用模型；中断后的running尝试不会自动重放。"""
        row = await self.store.row(CHAT_TURNS, identifier)
        body = verified_payload(row)
        attempts = body.get("attempts", [])
        if (
            str(context.stage_id) != identifier
            or str(context.report_id) != body["report_id"]
            or context.workspace != self.store.workspace
            or context.question != body["question"]
        ):
            raise ResearchIntegrityError("问答尝试不属于原问题或报告")
        if body["status"] != "running" or len(attempts) >= 2 or (attempts and attempts[-1]["status"] == "running"):
            raise ResearchConflict("问答不能开始下一次尝试")
        attempt = {
            "number": len(attempts) + 1,
            "status": "running",
            "error_code": None,
            "tool_calls": None,
            "tool_budget": context.tool_budget,
            "context_hash": context.content_hash,
            "deadline": context.deadline.isoformat(),
            "started_time": now().isoformat(),
            "finished_time": None,
            "receipt_path": None,
        }
        await self.write_payload(row, {**body, "attempts": [*attempts, attempt]})

    async def finish_attempt(self, identifier: str, result: ChatResult) -> ChatTurnView:
        """失败回执先提交；成功尝试与答案同事务，随后取消不能抹去回答。"""
        row = await self.store.row(CHAT_TURNS, identifier)
        body = verified_payload(row)
        attempts = body.get("attempts", [])
        if body["status"] != "running" or not attempts or attempts[-1]["status"] != "running":
            raise ResearchConflict("问答没有运行中的尝试")
        if result.context_hash != attempts[-1]["context_hash"]:
            raise ResearchIntegrityError("问答结果与本次尝试不一致")
        attempt = {
            **attempts[-1],
            "status": result.status,
            "error_code": result.error_code,
            "tool_calls": result.tool_calls,
            "finished_time": now().isoformat(),
            "receipt_path": result.receipt_path,
        }
        payload = {
            **body,
            "attempts": [*attempts[:-1], attempt],
            "tool_calls": body["tool_calls"] + result.tool_calls,
            "context_hash": result.context_hash,
            "receipt_path": result.receipt_path,
        }
        if result.status == "completed":
            payload.update(status="completed", answer=result.answer.model_dump(mode="json"), error_code=None)
        return await self.write_payload(row, payload)

    async def history(self, turn: ChatTurnView) -> tuple[ChatHistory, ...]:
        """仅取父链最近六问且完整消息合计有界，不接收浏览器伪造的历史。"""
        result, parent, size, revision = [], turn.parent_turn_id, 0, turn.revision
        while parent and len(result) < 6:
            item = await self.read(str(parent))
            if (
                item.report_id != turn.report_id
                or item.conversation_id != turn.conversation_id
                or item.revision != revision - 1
            ):
                raise ResearchIntegrityError("问答父链不一致")
            history = ChatHistory(question=item.question, status=item.status, answer=item.answer)
            size += len(history.model_dump_json())
            if size > 24000:
                break
            result.append(history)
            parent = item.parent_turn_id
            revision = item.revision
        return tuple(reversed(result))

    async def page(self, report_id: str, page: int, page_size: int, conversation_id: str | None = None) -> ChatTurnPage:
        """报告与会话过滤、确定排序和分页均由SQL执行。"""
        await self.store.row(RESEARCH_REPORTS, report_id)
        predicate = [
            CHAT_TURNS.c.workspace == self.store.workspace,
            CHAT_TURNS.c.report_uuid == report_id,
            CHAT_TURNS.c.is_deleted.is_(False),
        ]
        if conversation_id:
            predicate.append(CHAT_TURNS.c.conversation_uuid == conversation_id)
        async with self.store.engine.connect() as connection:
            total = await connection.scalar(select(func.count()).select_from(CHAT_TURNS).where(*predicate))
            rows = (
                (
                    await connection.execute(
                        select(CHAT_TURNS)
                        .where(*predicate)
                        .order_by(CHAT_TURNS.c.create_time.desc(), CHAT_TURNS.c.uuid.desc())
                        .limit(page_size)
                        .offset((page - 1) * page_size)
                    )
                )
                .mappings()
                .all()
            )
        return ChatTurnPage(
            items=tuple(chat_view(dict(row)) for row in rows), total=total, page=page, page_size=page_size
        )
