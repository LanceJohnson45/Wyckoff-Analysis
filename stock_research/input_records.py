"""人工输入维护的校验读取与目标串行锁，所有查询带workspace。"""

from datetime import date

from sqlalchemy import Select, and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from stock_research.errors import ResearchIntegrityError
from stock_research.identity import digest
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import INPUT_PUBLICATIONS, THEME_PROPOSALS
from stock_research.schemas.input_maintenance import InputPublication, ThemeDefinition


def proposal_payload(row: dict[str, object]) -> dict[str, object]:
    """索引、规范化输入与完整摘要一同核对，不把损坏持久事实当用户错误。"""
    body = verified_payload(row)
    try:
        definition = ThemeDefinition.model_validate(body["definition"])
        if definition.name != row["name"] or digest(body["definition"]) != row["request_hash"]:
            raise ValueError("提议索引不一致")
        if body["base_revision"] < 0 or tuple(sorted(set(body["previous_members"]))) != tuple(body["previous_members"]):
            raise ValueError("提议基准不一致")
    except (KeyError, TypeError, ValueError):
        raise ResearchIntegrityError("已保存主题提议无效") from None
    return body


def publication_select() -> Select:
    """主题发布必须绑定原提议正文；manual发布没有提议外键。"""
    return select(
        INPUT_PUBLICATIONS,
        THEME_PROPOSALS.c.payload.label("proposal_payload"),
        THEME_PROPOSALS.c.content_hash.label("proposal_hash"),
        THEME_PROPOSALS.c.request_hash.label("proposal_request_hash"),
        THEME_PROPOSALS.c.name.label("proposal_name"),
    ).select_from(
        INPUT_PUBLICATIONS.outerjoin(
            THEME_PROPOSALS,
            and_(
                INPUT_PUBLICATIONS.c.proposal_uuid == THEME_PROPOSALS.c.uuid,
                INPUT_PUBLICATIONS.c.workspace == THEME_PROPOSALS.c.workspace,
            ),
        )
    )


def publication_view(row: dict[str, object]) -> InputPublication:
    """类型与发布目标、时间、修订和提议必须全部指向同一份事实。"""
    body = verified_payload(row)
    try:
        result = InputPublication(
            **{key: value for key, value in body.items() if key != "request"},
            id=row["uuid"],
            content_hash=row["content_hash"],
        )
        if (
            result.kind,
            result.target,
            result.revision,
            result.effective_date,
            str(result.proposal_id) if result.proposal_id else None,
        ) != (row["kind"], row["target"], row["revision"], row["effective_date"], row["proposal_uuid"]) or digest(
            body["request"]
        ) != row["request_hash"]:
            raise ValueError("发布索引不一致")
        if result.kind == "theme":
            proposal = proposal_payload(
                {
                    "payload": row["proposal_payload"],
                    "content_hash": row["proposal_hash"],
                    "request_hash": row["proposal_request_hash"],
                    "name": row["proposal_name"],
                }
            )
            if body["definition"] != proposal["definition"] or body["request"] != {
                "proposal_id": str(result.proposal_id),
                "expected_revision": proposal["base_revision"],
                "effective_date": result.effective_date.isoformat(),
            }:
                raise ValueError("主题确认与原提议不一致")
        elif {
            key: value for key, value in body["request"].items() if key not in {"expected_revision", "effective_date"}
        } != body["definition"]:
            raise ValueError("自选发布与请求不一致")
        if result.revision != body["request"]["expected_revision"] + 1:
            raise ValueError("发布修订不连续")
    except (KeyError, TypeError, ValueError):
        raise ResearchIntegrityError("人工输入发布记录无效") from None
    return result


async def target_lock(connection: AsyncConnection, workspace: str, kind: str, target: str) -> None:
    """锁只覆盖本目标的读修订和追加，不将数据库连接跨快照计算持有。"""
    key = int(digest({"input_publication": workspace, "kind": kind, "target": target})[:16], 16) - (1 << 63)
    await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
    await connection.execute(select(func.pg_advisory_xact_lock(key)))


async def publication_head(
    connection: AsyncConnection, workspace: str, kind: str, target: str
) -> InputPublication | None:
    """CAS核对最新完整记录，未来生效修订同样属于已发布配置。"""
    row = (
        (
            await connection.execute(
                publication_select()
                .where(
                    INPUT_PUBLICATIONS.c.workspace == workspace,
                    INPUT_PUBLICATIONS.c.kind == kind,
                    INPUT_PUBLICATIONS.c.target == target,
                    INPUT_PUBLICATIONS.c.is_deleted.is_(False),
                )
                .order_by(INPUT_PUBLICATIONS.c.revision.desc())
                .limit(1)
            )
        )
        .mappings()
        .one_or_none()
    )
    return publication_view(dict(row)) if row else None


async def publications_for_date(store: RunStore, as_of: date) -> tuple[InputPublication, ...]:
    """单条MVCC语句固定所有目标各自已生效最高修订，不逐主题读取当前配置。"""
    table = INPUT_PUBLICATIONS
    latest = (
        select(table.c.uuid)
        .where(table.c.workspace == store.workspace, table.c.effective_date <= as_of, table.c.is_deleted.is_(False))
        .distinct(table.c.kind, table.c.target)
        .order_by(table.c.kind, table.c.target, table.c.revision.desc())
    )
    async with store.engine.connect() as connection:
        rows = (
            (
                await connection.execute(
                    publication_select()
                    .where(table.c.workspace == store.workspace, table.c.uuid.in_(latest))
                    .order_by(table.c.kind, table.c.target)
                )
            )
            .mappings()
            .all()
        )
    return tuple(publication_view(dict(row)) for row in rows)
