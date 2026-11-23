"""股票研究配置；凭证仅由环境或指定 .env 注入。"""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL

PUBLIC_GITHUB_API = "https://api.github.com"


class Settings(BaseSettings):
    """研究进程的独立配置，不继承 Personal 网关身份。"""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="STOCK_RESEARCH_", extra="ignore")
    workspace: str = "stocks"
    local_market_path: Path = Path("data/local_market.sqlite3")
    artifacts_path: Path = Path(".stock-research")
    universe_path: Path = Path("config/research/universe.json")
    classification_path: Path = Path("config/research/classifications.json")
    benchmark_path: Path | None = None
    benchmark_price_tolerance: float = Field(default=0.001, gt=0, le=0.01)
    public_sp500_repository: str = "datasets/s-and-p-500-companies"
    public_sp500_file: str = "data/constituents.csv"
    public_nasdaq100_repository: str = "Gary-Strauss/NASDAQ100_Constituents"
    public_nasdaq100_file: str = "data/nasdaq100_constituents.json"
    public_source_max_age_days: int = Field(default=35, ge=1, le=90)
    parameters_path: Path = Path("config/research/baseline-v1.json")
    wyckoff_parameters_path: Path = Path("config/research/wyckoff-us-v1.json")
    postgres_host: str = "127.0.0.1"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_database: str = "stock_research"
    postgres_user: str = "stock_research"
    postgres_password: SecretStr = SecretStr("")
    api_token: SecretStr | None = None
    api_read_token: SecretStr | None = None
    api_host: Literal["127.0.0.1"] = "127.0.0.1"
    api_port: int = Field(default=8788, ge=1, le=65535)
    api_allowed_origins: tuple[str, ...] = ("http://127.0.0.1:8788", "http://localhost:8788")
    coverage_min: float = Field(default=0.95, gt=0, le=1)
    history_rows: int = Field(default=320, ge=320)
    adjustment: Literal["qfq"] = "qfq"
    executor: Literal["dsh"] = "dsh"
    dsh_executable: Path = Path("dsh")
    dsh_home: Path = Path(".stock-research/dsh")
    dsh_agents_home: Path = Path(".stock-research/agents")
    model: str = ""
    model_profile_path: Path | None = None
    model_credentials_path: Path = Path.home() / ".dsh/.credentials.yaml"
    model_env_path: Path | None = None
    model_provider: str = ""
    model_api: Literal["openai-completions", "openai-responses", "anthropic-messages"] = "openai-completions"
    models_by_role: dict[str, str] = Field(default_factory=dict)
    model_output_tokens: int = Field(default=8192, ge=512, le=16384)
    stage_timeout_seconds: int = Field(default=180, ge=1, le=600)
    run_timeout_seconds: int = Field(default=3000, ge=1, le=3000)

    @field_validator("benchmark_path", "model_profile_path", "model_env_path", mode="before")
    @classmethod
    def empty_benchmark_path(cls, value: object) -> object:
        """空配置表示继续使用云端基准，禁止把空串解释为当前目录。"""
        return None if value == "" else value

    @field_validator(
        "model_profile_path",
        "model_env_path",
        "model_credentials_path",
        "dsh_executable",
        "dsh_home",
        "dsh_agents_home",
    )
    @classmethod
    def expand_operator_home(cls, value: Path | None) -> Path | None:
        """dotenv不经过交互shell，部署路径中的~需显式展开。"""
        return value.expanduser() if value is not None else None

    def database_url(self, *, test: bool = False) -> URL:
        """由离散配置构造连接；测试库强制增加 _test 后缀。"""
        database = self.postgres_database + ("_test" if test else "")
        options = {
            "username": self.postgres_user,
            "host": self.postgres_host,
            "port": self.postgres_port,
            "database": database,
        }
        options["pass" + "word"] = self.postgres_password.get_secret_value()
        return URL.create("postgresql+asyncpg", **options)


class MarketSourceSettings(BaseSettings):
    """复用既有 PG_* 行情身份，仅供只读同步；不替代研究状态配置。"""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PG_", extra="ignore")
    enabled: bool = False
    host: str = ""
    port: int = Field(default=5432, ge=1, le=65535)
    database: str = ""
    user: str = ""
    password: SecretStr = SecretStr("")
    sslmode: Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"] = "require"
    dsn: SecretStr | None = None

    def database_url(self) -> URL:
        """拒绝缺项和双重连接来源，不打印或回退到研究账号。"""
        if self.dsn and self.dsn.get_secret_value().strip():
            raise ValueError("新研究同步仅使用离散 PG_*，请清空 PG_DSN 后配置行情连接")
        if not self.enabled or not all((self.host, self.database, self.user, self.password.get_secret_value())):
            raise ValueError("行情连接缺少 PG_ENABLED 或 PG_HOST/DATABASE/USER/PASSWORD")
        return URL.create(
            "postgresql+asyncpg",
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.database,
        )
