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
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable

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


def execution_table(name: str, *columns: Column | UniqueConstraint) -> Table:
    """执行事实统一拥有workspace和完整payload摘要，避免跨租户读取。"""
    return Table(
        name,
        RESEARCH_METADATA,
        Column("uuid", String(36), primary_key=True),
        Column("workspace", String(128), nullable=False),
        Column("content_hash", String(64), nullable=False),
        Column("payload", JSON, nullable=False),
        *columns,
        *audit_columns(),
    )


# lint-redline:ignore PS-10 表对象依赖上方工厂及已有快照表。
STRATEGY_VERSIONS = execution_table("strategy_versions", UniqueConstraint("workspace", "content_hash"))
# lint-redline:ignore PS-10 表对象依赖上方工厂。
PARAMETER_SETS = execution_table("parameter_sets", UniqueConstraint("workspace", "content_hash"))
# lint-redline:ignore PS-10 因子产物依赖上方版本表。
FACTOR_RESULTS = execution_table(
    "factor_results",
    Column("snapshot_uuid", String(36), ForeignKey(MARKET_SNAPSHOTS.c.uuid), nullable=False),
    Column("strategy_uuid", String(36), ForeignKey(STRATEGY_VERSIONS.c.uuid), nullable=False),
    Column("parameter_uuid", String(36), ForeignKey(PARAMETER_SETS.c.uuid), nullable=False),
    Column("strategy", String(32), nullable=False),
    UniqueConstraint("workspace", "content_hash"),
)
# lint-redline:ignore PS-10 运行表依赖已有快照及自身外键。
RESEARCH_RUNS = execution_table(
    "research_runs",
    Column("snapshot_uuid", String(36), ForeignKey(MARKET_SNAPSHOTS.c.uuid), nullable=False),
    Column("run_key", String(64), nullable=False),
    Column("status", String(32), nullable=False),
    Column("error_code", String(64), nullable=True),
    Column("parent_run_uuid", String(36), ForeignKey("stock_research.research_runs.uuid"), nullable=True),
    UniqueConstraint("workspace", "run_key"),
)
# lint-redline:ignore PS-10 请求幂等映射依赖运行表，NULL表示尚未完成冻结绑定。
API_REQUESTS = execution_table(
    "api_requests",
    Column("key_hash", String(64), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=True),
    UniqueConstraint("workspace", "key_hash"),
)
# lint-redline:ignore PS-10 输入准备可能尚无run，API关联请求，CLI无HTTP请求。
PREPARATION_ATTEMPTS = execution_table(
    "preparation_attempts",
    Column("target_session", Date, nullable=False),
    Column("status", String(32), nullable=False),
    Column("request_uuid", String(36), ForeignKey(API_REQUESTS.c.uuid), nullable=True),
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=True),
)
Index(
    "idx_preparations_workspace_session_created",
    PREPARATION_ATTEMPTS.c.workspace,
    PREPARATION_ATTEMPTS.c.target_session,
    PREPARATION_ATTEMPTS.c.create_time,
)
# lint-redline:ignore PS-10 阶段表依赖上方运行表。
STAGE_RUNS = execution_table(
    "stage_runs",
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=False),
    Column("stage_key", String(64), nullable=False),
    Column("strategy", String(32), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("status", String(32), nullable=False),
    Column("attempt_count", Integer, nullable=False, default=0),
    Column("target_selection_uuid", String(36), nullable=True),
    UniqueConstraint("run_uuid", "stage_key"),
)
# lint-redline:ignore PS-10 初选依赖运行和阶段表。
STRATEGY_SELECTIONS = execution_table(
    "strategy_selections",
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=False),
    Column("stage_uuid", String(36), ForeignKey(STAGE_RUNS.c.uuid), nullable=False, unique=True),
    Column("strategy", String(32), nullable=False),
    UniqueConstraint("run_uuid", "strategy"),
)
# lint-redline:ignore PS-10 初选条目依赖上方初选表。
SELECTION_ITEMS = execution_table(
    "selection_items",
    Column("selection_uuid", String(36), ForeignKey(STRATEGY_SELECTIONS.c.uuid), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("rank", Integer, nullable=False),
    UniqueConstraint("selection_uuid", "symbol"),
)
# lint-redline:ignore PS-10 互评依赖已提交初选版本，不与阶段形成DDL外键环。
CROSS_REVIEWS = execution_table(
    "cross_reviews",
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=False),
    Column("stage_uuid", String(36), ForeignKey(STAGE_RUNS.c.uuid), nullable=False, unique=True),
    Column("reviewer_strategy", String(32), nullable=False),
    Column("target_selection_uuid", String(36), ForeignKey(STRATEGY_SELECTIONS.c.uuid), nullable=False),
    UniqueConstraint("run_uuid", "reviewer_strategy", "target_selection_uuid"),
)
# lint-redline:ignore PS-10 报告修订依赖上方运行表。
RESEARCH_REPORTS = execution_table(
    "research_reports",
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=False),
    Column("revision", Integer, nullable=False),
    UniqueConstraint("workspace", "content_hash"),
    UniqueConstraint("run_uuid", "revision"),
)

# lint-redline:ignore PS-10 名单依赖执行账本表和审计列工厂。
WATCHLIST_ITEMS = execution_table(
    "watchlist_items",
    Column("strategy", String(32), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("target_session", Date, nullable=False),
    UniqueConstraint("workspace", "strategy", "symbol"),
)
# lint-redline:ignore PS-10 事件依赖执行账本表和审计列工厂。
WATCHLIST_EVENTS = execution_table(
    "watchlist_events",
    Column("strategy", String(32), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("target_session", Date, nullable=False),
    Column("event_kind", String(64), nullable=False),
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=False),
    UniqueConstraint("workspace", "strategy", "symbol", "target_session", "event_kind"),
)
# lint-redline:ignore PS-10 当日来源依赖执行账本表和审计列工厂。
WATCHLIST_DAYS = execution_table(
    "watchlist_days",
    Column("strategy", String(32), nullable=False),
    Column("target_session", Date, nullable=False),
    Column("run_uuid", String(36), ForeignKey(RESEARCH_RUNS.c.uuid), nullable=False),
    UniqueConstraint("workspace", "strategy", "target_session"),
)

# lint-redline:ignore PS-10 注册名引用已存在的不可变参数产物。
PARAMETER_PROFILES = execution_table(
    "parameter_profiles",
    Column("strategy", String(32), nullable=False),
    Column("version", String(256), nullable=False),
    Column("parameter_uuid", String(36), ForeignKey(PARAMETER_SETS.c.uuid), nullable=False),
    UniqueConstraint("workspace", "strategy", "version"),
)
# lint-redline:ignore PS-10 发布历史依赖已注册参数，不更新旧发布记录。
PARAMETER_ACTIVATIONS = execution_table(
    "strategy_parameter_activations",
    Column("strategy", String(32), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("parameter_uuid", String(36), ForeignKey(PARAMETER_SETS.c.uuid), nullable=False),
    UniqueConstraint("workspace", "strategy", "revision"),
)
# lint-redline:ignore PS-10 问答引用固定报告，根/父问答使用表内外键；根的parent为空。
CHAT_TURNS = execution_table(
    "chat_turns",
    Column("report_uuid", String(36), ForeignKey(RESEARCH_REPORTS.c.uuid), nullable=False),
    Column("conversation_uuid", String(36), ForeignKey("stock_research.chat_turns.uuid"), nullable=False),
    Column("parent_uuid", String(36), ForeignKey("stock_research.chat_turns.uuid"), nullable=True),
    Column("revision", Integer, nullable=False),
    Column("status", String(32), nullable=False),
    Column("key_hash", String(64), nullable=False),
    Column("request_hash", String(64), nullable=False),
    UniqueConstraint("workspace", "key_hash"),
    UniqueConstraint("workspace", "parent_uuid"),
    UniqueConstraint("conversation_uuid", "revision"),
)
Index(
    "idx_chat_turns_workspace_report_created",
    CHAT_TURNS.c.workspace,
    CHAT_TURNS.c.report_uuid,
    CHAT_TURNS.c.create_time,
)

# lint-redline:ignore PS-10 维护事实复用上方受维护表工厂，提议保持不可变。
THEME_PROPOSALS = execution_table(
    "theme_proposals",
    Column("name", String(512), nullable=False),
    Column("key_hash", String(64), nullable=False),
    Column("request_hash", String(64), nullable=False),
    UniqueConstraint("workspace", "key_hash"),
)
# lint-redline:ignore PS-10 人工发布追加修订，依赖提议外键；历史行不原地改写。
INPUT_PUBLICATIONS = execution_table(
    "input_publications",
    Column("kind", String(32), nullable=False),
    Column("target", String(512), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("effective_date", Date, nullable=False),
    Column("proposal_uuid", String(36), ForeignKey(THEME_PROPOSALS.c.uuid), nullable=True),
    Column("key_hash", String(64), nullable=False),
    Column("request_hash", String(64), nullable=False),
    UniqueConstraint("workspace", "kind", "target", "revision"),
    UniqueConstraint("workspace", "proposal_uuid"),
    UniqueConstraint("workspace", "key_hash"),
)


def schema_ddl() -> str:
    """从受维护的Python定义导出DDL，不在仓库保存独立SQL副本。"""
    statements = [CreateSchema("stock_research", if_not_exists=True)]
    statements.extend(CreateTable(table, if_not_exists=True) for table in RESEARCH_METADATA.sorted_tables)
    statements.extend(
        CreateIndex(index, if_not_exists=True)
        for table in RESEARCH_METADATA.sorted_tables
        for index in sorted(table.indexes, key=lambda value: value.name)
    )
    content = "\n\n".join(
        str(statement.compile(dialect=postgresql.dialect())).strip() + ";" for statement in statements
    )
    return "BEGIN;\nSET LOCAL TIME ZONE 'Asia/Shanghai';\n" + content + "\nCOMMIT;\n"
