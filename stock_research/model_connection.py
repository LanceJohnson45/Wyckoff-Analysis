"""受信模型连接读取；只把本次模型身份交给独立Harness进程。"""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import Field, SecretStr, field_validator

from stock_research.config import Settings
from stock_research.models import FrozenModel

MODEL_ENV_SCRIPT = """set -e
set -a
. "$1"
exec "$2" -c 'import json,os; keys=("BASE_URL","PROVIDER","API_KEY","ID","STRONG_ID"); print(json.dumps({k:os.environ.get("DSH_E2E_MODEL_"+k,"") for k in keys}))'
"""


class ModelConnection(FrozenModel):
    """可留档配置与不可序列化凭据分离，拒绝带身份信息的URL。"""

    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    base_url: str
    api: Literal["openai-completions", "openai-responses", "anthropic-messages"] = "openai-completions"
    api_key: SecretStr = Field(exclude=True, repr=False)
    output_tokens: int = Field(default=8192, ge=512, le=16384)
    context_window: int = Field(default=262144, ge=32768)
    available_models: tuple[str, ...] = Field(default=(), exclude=True, repr=False)

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        """凭据只能由独立环境键承载，不能进入可留档URL。"""
        url = urlsplit(value)
        if (
            url.scheme not in {"https", "http"}
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError("模型地址须为无凭据、查询串或fragment的HTTP(S)地址")
        return value.rstrip("/")


def primary_model(settings: Settings) -> ModelConnection:
    """选择operator的单一可信连接，选股与交互共享相同来源及输出上限。"""
    if bool(settings.model_env_path) == bool(settings.model_profile_path):
        raise ValueError("须选择一个可信模型配置来源")
    connection = (
        load_profile_connection(
            settings.model_profile_path,
            settings.model_credentials_path,
            provider=settings.model_provider,
            model=settings.model,
        )
        if settings.model_profile_path
        else load_model_connection(settings.model_env_path, model=settings.model, api=settings.model_api)
    )
    return connection.model_copy(update={"output_tokens": settings.model_output_tokens})


def load_model_connection(path: Path, *, model: str = "", api: str = "openai-completions") -> ModelConnection:
    """读取operator已有可信shell配置，固定argv且有界；不执行任何模型提供的路径。"""
    if not path.is_file():
        raise ValueError("未配置可信模型连接文件")
    process = subprocess.Popen(
        ["/bin/bash", "-c", MODEL_ENV_SCRIPT, "research-model", str(path.resolve()), sys.executable],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
        env={key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR") if key in os.environ},
    )
    try:
        stdout, _stderr = process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate(timeout=2)
        raise ValueError("可信模型连接读取超时") from None
    if process.returncode or len(stdout) > 65536:
        raise ValueError("可信模型连接读取失败")
    try:
        values = json.loads(stdout)
        selected = model or values["ID"]
        if selected not in {values["ID"], values["STRONG_ID"]} or not values["API_KEY"].strip():
            raise ValueError("模型或凭据缺失")
        return ModelConnection(
            provider=values["PROVIDER"],
            model=selected,
            base_url=values["BASE_URL"],
            api=api,
            api_key=values["API_KEY"].strip(),
            available_models=tuple(value for value in (values["ID"], values["STRONG_ID"]) if value),
        )
    except (ValueError, KeyError, TypeError):
        raise ValueError("可信模型连接格式、模型或凭据无效") from None


def load_profile_connection(
    path: Path, credentials_path: Path, *, provider: str = "", model: str = ""
) -> ModelConnection:
    """只提取静态模型连接，不加载已有profile的插件、会话或其他凭据。"""

    class Loader(yaml.SafeLoader):
        """动态JS配置不能作为可复制的模型连接，其他YAML类型仍按安全加载。"""

    Loader.add_constructor("tag:yaml.org,2002:js", lambda loader, node: None)
    try:
        rows = yaml.load(path.read_text(encoding="utf-8"), Loader=Loader)
        credentials = yaml.safe_load(credentials_path.read_text())
    except yaml.YAMLError:
        raise ValueError("可信模型配置或凭据存储格式无效") from None
    if (
        not isinstance(rows, list)
        or any(not isinstance(row, dict) for row in rows)
        or not isinstance(credentials, dict)
    ):
        raise ValueError("可信模型配置或凭据存储结构无效")
    configs = [row for row in rows if isinstance(row.get("config"), dict)]
    default = next((row["config"] for row in reversed(configs) if row.get("id") == "agent-default-model"), {})
    selected_provider, selected_model = provider or default.get("provider"), model or default.get("model")
    route = next(
        (
            row.get("config", {}).get("providers", {})[selected_provider]
            for row in reversed(configs)
            if isinstance(row["config"].get("providers"), dict) and selected_provider in row["config"]["providers"]
        ),
        None,
    )
    models = route.get("models") if isinstance(route, dict) else None
    if (
        not isinstance(models, list)
        or any(not isinstance(item, dict) or not isinstance(item.get("id"), str) for item in models)
        or selected_model not in {item["id"] for item in models}
        or not isinstance(selected_provider, str)
    ):
        raise ValueError("指定provider/model未在可信profile中静态注册")
    reference = route.get("apiKeyEnv")
    if not isinstance(reference, str) or not reference:
        raise ValueError("可信profile缺少静态凭据引用")
    references = credentials.get("refs", {})
    if not isinstance(references, dict):
        raise ValueError("可信凭据引用存储结构无效")
    key = os.environ.get(reference, "") or references.get(reference, "")
    if not isinstance(key, str) or not key.strip():
        raise ValueError("可信profile所引用的模型凭据不可用")
    return ModelConnection(
        provider=selected_provider,
        model=selected_model,
        base_url=route.get("baseURL"),
        api=route.get("api"),
        api_key=key.strip(),
        available_models=tuple(item["id"] for item in route["models"]),
    )
