"""外部查询进程的最小配置，与PG和模型设置分开加载。"""

from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class QuerySettings(BaseSettings):
    """只使用专用只读身份；指定文件不含PG、模型或网关凭据。"""

    model_config = SettingsConfigDict(
        env_file=".stock-research/query.env", env_prefix="STOCK_RESEARCH_QUERY_", extra="forbid"
    )
    host: Literal["127.0.0.1"] = "127.0.0.1"
    port: int = Field(default=8788, ge=1, le=65535)
    token: SecretStr = Field(min_length=32)

    @field_validator("token")
    @classmethod
    def reject_whitespace(cls, value: SecretStr) -> SecretStr:
        """不尝试修补损坏凭据或悄悄使用空白身份。"""
        if value.get_secret_value() != value.get_secret_value().strip():
            raise ValueError("只读身份不能含首尾空白")
        return value

    @property
    def endpoint(self) -> str:
        """地址由受限离散配置构造，不接受任意URL。"""
        return f"http://{self.host}:{self.port}"
