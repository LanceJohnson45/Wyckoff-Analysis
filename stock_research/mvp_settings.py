"""单用户盘前编排只持有操作路径，连接身份由各自子进程读取。"""

from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class MvpSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STOCK_RESEARCH_MVP_", extra="forbid")

    source_root: Path
    source_env_file: Path
    personal_root: Path
    personal_env_file: Path
    universe_path: Path
    workflow_identity_file: Path
    state_path: Path

    @field_validator("*")
    @classmethod
    def absolute_operator_path(cls, value: Path) -> Path:
        value = value.expanduser()
        if not value.is_absolute() or value.is_symlink():
            raise ValueError("MVP操作路径必须是非符号链接的绝对路径")
        return value


def load_mvp_settings(path: Path) -> MvpSettings:
    path = path.expanduser()
    if not path.is_file() or path.is_symlink() or path.stat().st_mode & 0o077:
        raise ValueError("MVP配置须为0600普通文件")
    return MvpSettings(_env_file=path)
