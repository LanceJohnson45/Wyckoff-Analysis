"""不可变参数版本及追加发布历史；版本名和CAS由数据库事务约束。"""

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from stock_research.errors import ResearchConflict, ResearchIntegrityError, ResearchNotFound
from stock_research.identity import digest, new_id
from stock_research.run_store import RunStore, put_artifact, verified_payload
from stock_research.schema import PARAMETER_ACTIVATIONS, PARAMETER_PROFILES, PARAMETER_SETS
from stock_research.schemas.api import Strategy
from stock_research.schemas.parameters import (
    CreateParameterSet,
    ParameterSetPage,
    ParameterSetView,
    StrategyParameterView,
)


def parameter_view(row: dict[str, object]) -> ParameterSetView:
    """持久化内容无效是存储完整性错误，不作为用户请求参数错误返回。"""
    payload = verified_payload(row)
    try:
        return ParameterSetView(
            **payload, id=row["uuid"], parameter_hash=digest(payload["parameters"]), create_time=row["create_time"]
        )
    except ValueError:
        raise ResearchIntegrityError("已保存参数产物无效") from None


def verify_profile(row: dict[str, object]) -> dict[str, object]:
    """注册名、外键与摘要共同校验，不能用另一参数的合法payload替代。"""
    payload = verified_payload(row)
    if payload != {"strategy": row["strategy"], "version": row["version"], "parameter_set_id": row["parameter_uuid"]}:
        raise ResearchIntegrityError("参数注册记录不一致")
    return payload


async def register_parameters(store: RunStore, data: CreateParameterSet) -> ParameterSetView:
    """相同版本名只允许同一份内容；冲突事务也回滚新建产物。"""
    body = data.model_dump(mode="json")
    async with store.engine.begin() as connection:
        parameter_id = await put_artifact(connection, PARAMETER_SETS, store.workspace, body)
        profile = {"strategy": data.strategy, "version": data.parameters.version, "parameter_set_id": parameter_id}
        await connection.execute(
            insert(PARAMETER_PROFILES)
            .values(
                uuid=new_id(),
                workspace=store.workspace,
                strategy=data.strategy,
                version=data.parameters.version,
                parameter_uuid=parameter_id,
                payload=profile,
                content_hash=digest(profile),
            )
            .on_conflict_do_nothing(index_elements=["workspace", "strategy", "version"])
        )
        row = (
            (
                await connection.execute(
                    select(PARAMETER_PROFILES).where(
                        PARAMETER_PROFILES.c.workspace == store.workspace,
                        PARAMETER_PROFILES.c.strategy == data.strategy,
                        PARAMETER_PROFILES.c.version == data.parameters.version,
                        PARAMETER_PROFILES.c.is_deleted.is_(False),
                    )
                )
            )
            .mappings()
            .one()
        )
        verify_profile(dict(row))
        if row["parameter_uuid"] != parameter_id:
            raise ResearchConflict("参数版本名已指向其他内容，请使用新版本名")
    return parameter_view(await store.row(PARAMETER_SETS, parameter_id))


async def list_parameters(store: RunStore, page: int, page_size: int, strategy: Strategy | None) -> ParameterSetPage:
    """注册版本SQL分页，不把研究期间全部临时参数产物当维护列表。"""
    predicate = [
        PARAMETER_PROFILES.c.workspace == store.workspace,
        PARAMETER_PROFILES.c.is_deleted.is_(False),
        PARAMETER_SETS.c.workspace == store.workspace,
        PARAMETER_SETS.c.is_deleted.is_(False),
    ]
    if strategy:
        predicate.append(PARAMETER_PROFILES.c.strategy == strategy)
    joined = PARAMETER_PROFILES.join(PARAMETER_SETS, PARAMETER_PROFILES.c.parameter_uuid == PARAMETER_SETS.c.uuid)
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(joined).where(*predicate))
        rows = (
            (
                await connection.execute(
                    select(
                        PARAMETER_SETS,
                        PARAMETER_PROFILES.c.payload.label("profile_payload"),
                        PARAMETER_PROFILES.c.content_hash.label("profile_hash"),
                        PARAMETER_PROFILES.c.strategy.label("profile_strategy"),
                        PARAMETER_PROFILES.c.version.label("profile_version"),
                    )
                    .select_from(joined)
                    .where(*predicate)
                    .order_by(PARAMETER_PROFILES.c.create_time.desc(), PARAMETER_PROFILES.c.uuid.desc())
                    .limit(page_size)
                    .offset((page - 1) * page_size)
                )
            )
            .mappings()
            .all()
        )
    items = []
    for row in rows:
        parameters = parameter_view(dict(row))
        profile = verify_profile(
            {
                "payload": row["profile_payload"],
                "content_hash": row["profile_hash"],
                "strategy": row["profile_strategy"],
                "version": row["profile_version"],
                "parameter_uuid": str(parameters.id),
            }
        )
        if profile["strategy"] != parameters.strategy or profile["version"] != parameters.parameters.version:
            raise ResearchIntegrityError("参数注册版本与产物不一致")
        items.append(parameters)
    return ParameterSetPage(items=tuple(items), total=total, page=page, page_size=page_size)


async def activate_parameters(
    store: RunStore, strategy: Strategy, parameter_id: str, expected: int
) -> StrategyParameterView:
    """发布按策略串行化并核对预期修订，旧发布行不更新。"""
    parameters = parameter_view(await store.row(PARAMETER_SETS, parameter_id))
    if parameters.strategy != strategy:
        raise ResearchConflict("参数版本不属于该策略")
    key = int(digest({"parameters": store.workspace, "strategy": strategy})[:16], 16) - (1 << 63)
    async with store.engine.begin() as connection:
        await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
        await connection.execute(select(func.pg_advisory_xact_lock(key)))
        registered = (
            (
                await connection.execute(
                    select(PARAMETER_PROFILES).where(
                        PARAMETER_PROFILES.c.workspace == store.workspace,
                        PARAMETER_PROFILES.c.strategy == strategy,
                        PARAMETER_PROFILES.c.parameter_uuid == parameter_id,
                        PARAMETER_PROFILES.c.is_deleted.is_(False),
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if not registered:
            raise ResearchNotFound("参数版本尚未注册")
        profile = verify_profile(dict(registered))
        if profile["version"] != parameters.parameters.version:
            raise ResearchIntegrityError("参数注册版本与产物不一致")
        revision = await latest_parameter_revision(connection, store.workspace, strategy)
        if revision != expected:
            raise ResearchConflict("参数发布修订已变化，请刷新后重试")
        payload = {"strategy": strategy, "revision": revision + 1, "parameter_set_id": parameter_id}
        await connection.execute(
            insert(PARAMETER_ACTIVATIONS).values(
                uuid=new_id(),
                workspace=store.workspace,
                strategy=strategy,
                revision=revision + 1,
                parameter_uuid=parameter_id,
                payload=payload,
                content_hash=digest(payload),
            )
        )
    return StrategyParameterView(
        strategy=strategy,
        parameters=parameters.parameters,
        source="published",
        revision=revision + 1,
        parameter_set_id=parameter_id,
        parameter_hash=parameters.parameter_hash,
    )


async def latest_parameter_revision(connection: AsyncConnection, workspace: str, strategy: Strategy) -> int:
    """CAS必须核对完整当前版本，不能在损坏的发布记录上继续追加。"""
    row = (
        (
            await connection.execute(
                select(PARAMETER_ACTIVATIONS)
                .where(
                    PARAMETER_ACTIVATIONS.c.workspace == workspace,
                    PARAMETER_ACTIVATIONS.c.strategy == strategy,
                    PARAMETER_ACTIVATIONS.c.is_deleted.is_(False),
                )
                .order_by(PARAMETER_ACTIVATIONS.c.revision.desc())
                .limit(1)
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return 0
    payload = verified_payload(dict(row))
    expected = {"strategy": row["strategy"], "revision": row["revision"], "parameter_set_id": row["parameter_uuid"]}
    if digest(payload) != digest(expected):
        raise ResearchIntegrityError("参数发布记录与修订不一致")
    return row["revision"]


async def published_parameters(store: RunStore) -> dict[str, StrategyParameterView]:
    """单个MVCC读语句取得两策略各自最新发布，不在计算中逐股读取当前配置。"""
    active = PARAMETER_ACTIVATIONS
    latest = (
        select(active.c.uuid)
        .where(active.c.workspace == store.workspace, active.c.is_deleted.is_(False))
        .distinct(active.c.strategy)
        .order_by(active.c.strategy, active.c.revision.desc())
    )
    statement = (
        select(
            PARAMETER_SETS,
            active.c.strategy.label("active_strategy"),
            active.c.revision,
            active.c.payload.label("active_payload"),
            active.c.content_hash.label("active_hash"),
        )
        .join(active, active.c.parameter_uuid == PARAMETER_SETS.c.uuid)
        .where(
            active.c.uuid.in_(latest),
            active.c.workspace == store.workspace,
            PARAMETER_SETS.c.workspace == store.workspace,
            PARAMETER_SETS.c.is_deleted.is_(False),
        )
    )
    async with store.engine.connect() as connection:
        rows = (await connection.execute(statement)).mappings().all()
    result = {}
    for row in rows:
        parameters = parameter_view(dict(row))
        metadata = verified_payload({"payload": row["active_payload"], "content_hash": row["active_hash"]})
        if parameters.strategy != row["active_strategy"] or digest(metadata) != digest(
            {
                "strategy": parameters.strategy,
                "revision": row["revision"],
                "parameter_set_id": str(parameters.id),
            }
        ):
            raise ResearchIntegrityError("参数发布记录与产物不一致")
        result[parameters.strategy] = StrategyParameterView(
            strategy=parameters.strategy,
            parameters=parameters.parameters,
            source="published",
            revision=row["revision"],
            parameter_set_id=parameters.id,
            parameter_hash=parameters.parameter_hash,
        )
    return result
