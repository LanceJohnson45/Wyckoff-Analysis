"""主题人工确认与manual自选追加发布，不将建议自动变为生效配置。"""

from datetime import date
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchInputError, ResearchIntegrityError
from stock_research.identity import digest, new_id, now
from stock_research.input_configuration import apply_input_publications, input_baseline
from stock_research.input_records import (
    proposal_payload,
    publication_head,
    publication_select,
    publication_view,
    publications_for_date,
    target_lock,
)
from stock_research.run_store import RunStore
from stock_research.schema import INPUT_PUBLICATIONS, THEME_PROPOSALS
from stock_research.schemas.input_maintenance import (
    ConfirmTheme,
    CreateManualOverride,
    InputPublication,
    ManualDefinition,
    ThemeDefinition,
    ThemeProposal,
)


def input_key(workspace: str, operation: str, key: str) -> str:
    """不同维护动作的幂等身份隔离，原始外部键不落库。"""
    return digest({"workspace": workspace, "operation": operation, "key": key})


def effective_date_allowed(effective: date) -> None:
    """操作时间由服务端产生，人工确认不能回填到更早纽约日期。"""
    if effective < now().astimezone(ZoneInfo("America/New_York")).date():
        raise ResearchInputError("生效日期不能早于确认时的纽约日期，请创建当前或未来版本")


async def append_publication(
    connection: AsyncConnection,
    workspace: str,
    *,
    kind: str,
    target: str,
    head: InputPublication | None,
    definition: ThemeDefinition | ManualDefinition,
    request: dict[str, object],
    key_hash: str,
    effective: date,
    proposal_id: str | None = None,
) -> str:
    """调用方持有目标锁，发布正文与请求、修订索引同一事务提交。"""
    identifier = new_id()
    payload = {
        "kind": kind,
        "target": target,
        "revision": head.revision + 1 if head else 1,
        "previous_hash": head.content_hash if head else None,
        "proposal_id": proposal_id,
        "effective_date": effective.isoformat(),
        "confirmed_time": now().isoformat(),
        "definition": definition.model_dump(mode="json"),
        "request": request,
    }
    try:
        InputPublication(
            **{key: value for key, value in payload.items() if key != "request"},
            id=identifier,
            content_hash=digest(payload),
        )
    except ValueError:
        raise ResearchInputError("人工发布内容或生效日期无效，请重新复核") from None
    await connection.execute(
        insert(INPUT_PUBLICATIONS).values(
            uuid=identifier,
            workspace=workspace,
            kind=kind,
            target=target,
            revision=payload["revision"],
            effective_date=effective,
            proposal_uuid=proposal_id,
            key_hash=key_hash,
            request_hash=digest(request),
            payload=payload,
            content_hash=digest(payload),
        )
    )
    return identifier


class InputMaintenance:
    """只操作当前workspace；临时文件及冻结消费由输入解析器负责。"""

    def __init__(self, store: RunStore, settings: Settings) -> None:
        self.store, self.settings = store, settings

    async def publication(self, identifier: str) -> InputPublication:
        async with self.store.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        publication_select().where(
                            INPUT_PUBLICATIONS.c.uuid == identifier,
                            INPUT_PUBLICATIONS.c.workspace == self.store.workspace,
                            INPUT_PUBLICATIONS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            from stock_research.errors import ResearchNotFound

            raise ResearchNotFound("人工输入发布不存在或不属于当前workspace")
        return publication_view(dict(row))

    async def proposal(self, identifier: str) -> ThemeProposal:
        row = await self.store.row(THEME_PROPOSALS, identifier)
        body = proposal_payload(row)
        async with self.store.engine.connect() as connection:
            head = await publication_head(connection, self.store.workspace, "theme", row["name"])
            confirmed = (
                (
                    await connection.execute(
                        publication_select().where(
                            INPUT_PUBLICATIONS.c.workspace == self.store.workspace,
                            INPUT_PUBLICATIONS.c.proposal_uuid == identifier,
                            INPUT_PUBLICATIONS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        publication = publication_view(dict(confirmed)) if confirmed else None
        try:
            return ThemeProposal(
                id=identifier,
                **body,
                current_revision=head.revision if head else 0,
                publication=publication,
                status="confirmed" if publication else "pending",
                create_time=row["create_time"],
            )
        except ValueError:
            raise ResearchIntegrityError("主题提议与确认记录不一致") from None

    async def propose(self, key: str, definition: ThemeDefinition) -> ThemeProposal:
        request = definition.model_dump(mode="json")
        key_hash = input_key(self.store.workspace, "theme-proposal", key)
        async with self.store.engine.begin() as connection:
            await target_lock(connection, self.store.workspace, "proposal-key", key_hash)
            existing = (
                (
                    await connection.execute(
                        select(THEME_PROPOSALS).where(
                            THEME_PROPOSALS.c.workspace == self.store.workspace, THEME_PROPOSALS.c.key_hash == key_hash
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing:
                body = proposal_payload(dict(existing))
                if body["definition"] != request:
                    raise ResearchConflict("相同主题提议幂等键不能更换内容")
                identifier = existing["uuid"]
            else:
                baseline = await input_baseline(self.settings)
                head = await publication_head(connection, self.store.workspace, "theme", definition.name)
                previous = (
                    head.definition.members
                    if head
                    else tuple(
                        sorted(record.symbol for record in baseline[1].records if definition.name in record.themes)
                    )
                )
                body = {
                    "definition": request,
                    "base_revision": head.revision if head else 0,
                    "base_universe_hash": digest(baseline[0].model_dump(mode="json")),
                    "base_classification_hash": digest(baseline[1].model_dump(mode="json")),
                    "previous_members": list(previous),
                }
                identifier = new_id()
                await connection.execute(
                    insert(THEME_PROPOSALS).values(
                        uuid=identifier,
                        workspace=self.store.workspace,
                        name=definition.name,
                        key_hash=key_hash,
                        request_hash=digest(request),
                        payload=body,
                        content_hash=digest(body),
                    )
                )
        return await self.proposal(identifier)

    async def validate_theme(self, proposal: ThemeProposal, effective: date) -> None:
        """人工确认时成员必须有有效归属/行业；基线变化须重新提议。"""
        baseline = await input_baseline(self.settings)
        if (digest(baseline[0].model_dump(mode="json")), digest(baseline[1].model_dump(mode="json"))) != (
            proposal.base_universe_hash,
            proposal.base_classification_hash,
        ):
            raise ResearchConflict("operator股票池或分类基线已变化，请重新提议并复核")
        publications = await publications_for_date(self.store, effective)
        universe, classifications = apply_input_publications(*baseline, publications, effective)
        active, records = universe.active(effective), {row.symbol: row for row in classifications.records}
        if any(
            symbol not in active
            or symbol not in records
            or records[symbol].effective_date > effective
            or not records[symbol].industry
            for symbol in proposal.definition.members
        ):
            raise ResearchInputError("主题成员须在生效股票池内且有有效行业分类，请先补齐资料")

    async def confirm(self, identifier: str, request: ConfirmTheme) -> InputPublication:
        proposal = await self.proposal(identifier)
        body = {"proposal_id": identifier, **request.model_dump(mode="json")}
        if proposal.publication:
            row = await self.store.row(INPUT_PUBLICATIONS, str(proposal.publication.id))
            if row["payload"]["request"] != body:
                raise ResearchConflict("该提议已用不同确认请求发布，不能覆盖")
            return proposal.publication
        effective_date_allowed(request.effective_date)
        await self.validate_theme(proposal, request.effective_date)
        async with self.store.engine.begin() as connection:
            await target_lock(connection, self.store.workspace, "theme", proposal.definition.name)
            existing = (
                (
                    await connection.execute(
                        publication_select().where(
                            INPUT_PUBLICATIONS.c.workspace == self.store.workspace,
                            INPUT_PUBLICATIONS.c.proposal_uuid == identifier,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing:
                if existing["payload"]["request"] != body:
                    raise ResearchConflict("该提议已用不同确认请求发布")
                return publication_view(dict(existing))
            head = await publication_head(connection, self.store.workspace, "theme", proposal.definition.name)
            revision = head.revision if head else 0
            if revision != request.expected_revision or revision != proposal.base_revision:
                raise ResearchConflict("主题修订已变化，请刷新并重新提议")
            publication_id = await append_publication(
                connection,
                self.store.workspace,
                kind="theme",
                target=proposal.definition.name,
                head=head,
                definition=proposal.definition,
                request=body,
                key_hash=input_key(self.store.workspace, "theme-confirm", identifier),
                effective=request.effective_date,
                proposal_id=identifier,
            )
        return await self.publication(publication_id)

    async def override(self, key: str, request: CreateManualOverride) -> InputPublication:
        body = request.model_dump(mode="json")
        key_hash = input_key(self.store.workspace, "manual", key)
        async with self.store.engine.begin() as connection:
            await target_lock(connection, self.store.workspace, "manual-key", key_hash)
            await target_lock(connection, self.store.workspace, "manual", request.symbol)
            existing = (
                (
                    await connection.execute(
                        publication_select().where(
                            INPUT_PUBLICATIONS.c.workspace == self.store.workspace,
                            INPUT_PUBLICATIONS.c.key_hash == key_hash,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing:
                result = publication_view(dict(existing))
                if existing["payload"]["request"] != body:
                    raise ResearchConflict("相同自选幂等键不能更换内容")
                return result
            effective_date_allowed(request.effective_date)
            head = await publication_head(connection, self.store.workspace, "manual", request.symbol)
            if (head.revision if head else 0) != request.expected_revision:
                raise ResearchConflict("自选修订已变化，请读取当前修订后再操作")
            definition = ManualDefinition.model_validate(
                request.model_dump(exclude={"expected_revision", "effective_date"})
            )
            identifier = await append_publication(
                connection,
                self.store.workspace,
                kind="manual",
                target=request.symbol,
                head=head,
                definition=definition,
                request=body,
                key_hash=key_hash,
                effective=request.effective_date,
            )
        return await self.publication(identifier)
