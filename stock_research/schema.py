"""研究数据阶段的受维护 PostgreSQL schema v1。"""

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    func,
)

SCHEMA_VERSION = 1
RESEARCH_METADATA = MetaData(schema="stock_research")


def audit_columns() -> list[Column]:
    """新表统一使用带时区的操作时间及显式软删除标志。"""
    return [
        Column("create_time", DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column("update_time", DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()),
        Column("delete_time", DateTime(timezone=True), nullable=True),
        Column("is_deleted", Boolean, nullable=False, default=False, server_default="false"),
    ]


def version_table(name: str) -> Table:
    """两个领域版本均以 workspace/version 唯一，保存完整出处。"""
    return Table(
        name,
        RESEARCH_METADATA,
        Column("uuid", String(36), primary_key=True),
        Column("workspace", String(128), nullable=False),
        Column("version", String(256), nullable=False),
        Column("content_hash", String(64), nullable=False),
        Column("payload", JSON, nullable=False),
        *audit_columns(),
        UniqueConstraint("workspace", "version"),
    )


# lint-redline:ignore PS-10 表对象依赖上方工厂，前移会产生 NameError。
UNIVERSES = version_table("universes")
# lint-redline:ignore PS-10 表对象依赖上方工厂。
CLASSIFICATION_VERSIONS = version_table("classification_versions")
# lint-redline:ignore PS-10 成员表依赖上方表对象与审计列工厂。
UNIVERSE_MEMBERS = Table(
    "universe_members",
    RESEARCH_METADATA,
    Column("uuid", String(36), primary_key=True),
    Column("universe_uuid", String(36), ForeignKey(UNIVERSES.c.uuid), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("index", String(32), nullable=False),
    Column("effective_date", Date, nullable=False),
    Column("expiry_date", Date, nullable=True),
    Column("payload", JSON, nullable=False),
    *audit_columns(),
    UniqueConstraint("universe_uuid", "symbol", "index", "effective_date"),
)
# lint-redline:ignore PS-10 成员表依赖上方表对象与审计列工厂。
GROUP_MEMBERS = Table(
    "group_members",
    RESEARCH_METADATA,
    Column("uuid", String(36), primary_key=True),
    Column("classification_uuid", String(36), ForeignKey(CLASSIFICATION_VERSIONS.c.uuid), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("name", String(256), nullable=False),
    Column("effective_date", Date, nullable=False),
    Column("payload", JSON, nullable=False),
    *audit_columns(),
    UniqueConstraint("classification_uuid", "symbol", "kind", "name"),
)
# lint-redline:ignore PS-10 快照表依赖上方表对象与审计列工厂。
MARKET_SNAPSHOTS = Table(
    "market_snapshots",
    RESEARCH_METADATA,
    Column("uuid", String(36), primary_key=True),
    Column("workspace", String(128), nullable=False),
    Column("market", String(8), nullable=False),
    Column("as_of", Date, nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("universe_uuid", String(36), ForeignKey(UNIVERSES.c.uuid), nullable=False),
    Column("classification_uuid", String(36), ForeignKey(CLASSIFICATION_VERSIONS.c.uuid), nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("payload", JSON, nullable=False),
    *audit_columns(),
    UniqueConstraint("workspace", "content_hash"),
)
Index("idx_market_snapshots_workspace_as_of", MARKET_SNAPSHOTS.c.workspace, MARKET_SNAPSHOTS.c.as_of)
