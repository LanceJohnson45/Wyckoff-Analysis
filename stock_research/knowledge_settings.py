"""研究问答可用的既有知识服务最小授权配置。"""

from urllib.parse import urlsplit

from pydantic import SecretStr, field_validator

from stock_research.models import FrozenModel


class KnowledgeSettings(FrozenModel):
    """凭据决定ACL作用域；工具进程只能访问本机服务入口。"""

    base_url: str
    token: SecretStr

    @field_validator("token")
    @classmethod
    def private_reader(cls, value: SecretStr) -> SecretStr:
        """不修补空白或过短凭据，也不把它回显到错误里。"""
        raw = value.get_secret_value()
        if len(raw) < 32 or raw != raw.strip():
            raise ValueError("知识检索身份无效")
        return value

    @property
    def endpoint(self) -> str:
        """只接受无路径、无附加身份的127.0.0.1 HTTP服务。"""
        url = urlsplit(self.base_url)
        if (
            url.scheme != "http"
            or url.hostname != "127.0.0.1"
            or url.path.rstrip("/") != ""
            or url.query
            or url.fragment
            or url.username
            or url.password
        ):
            raise ValueError("知识服务入口无效")
        return self.base_url.rstrip("/")
