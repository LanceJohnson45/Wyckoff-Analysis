"""固定workflow只读取自己的有限API身份，不加载研究或Personal服务端配置。"""

from pathlib import Path

from pydantic_settings import SettingsConfigDict

from stock_research.query_settings import QuerySettings

WORKFLOW_ENV_FILE = Path(".stock-research/workflow.env")


class WorkflowSettings(QuerySettings):
    """复用loopback/令牌校验，配置文件和环境键族独立于共享查询。"""

    model_config = SettingsConfigDict(env_file=WORKFLOW_ENV_FILE, env_prefix="STOCK_RESEARCH_WORKFLOW_", extra="forbid")


def load_workflow_identity(path: Path | None = None) -> WorkflowSettings:
    """显式路径来自operator固定argv；业务输入不能提供配置或身份路径。"""
    identity = (path or WORKFLOW_ENV_FILE).expanduser()
    if (
        (path is not None and not identity.exists())
        or identity.is_symlink()
        or (identity.exists() and (not identity.is_file() or identity.stat().st_mode & 0o077))
    ):
        raise ValueError("workflow身份文件须为0600普通文件")
    return WorkflowSettings(_env_file=identity)
