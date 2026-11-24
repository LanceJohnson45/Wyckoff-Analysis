"""与Vite代理共用的浏览器资源白名单，不开放任意API或目标地址。"""

import json
import re
from importlib.resources import files

CONTRACT = json.loads(files("stock_research").joinpath("web_routes.json").read_text(encoding="utf-8"))
BROWSER_BODY_LIMITS = CONTRACT["body_limits"]
ROUTES = {
    method: re.compile(
        "(?:"
        + "|".join(path.replace("{id}", CONTRACT["uuid"]).replace("{symbol}", CONTRACT["symbol"]) for path in paths)
        + ")"
    )
    for method, paths in CONTRACT.items()
    if method in {"GET", "POST"}
}


def allowed_browser_route(method: str, path: str) -> bool:
    """完整匹配，路径相似或额外尾段不能扩大浏览器权限。"""
    rule = ROUTES.get(method)
    return rule is not None and rule.fullmatch(path) is not None
