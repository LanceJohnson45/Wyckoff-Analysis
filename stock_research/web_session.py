"""本机浏览器短期会话；签名密钥仅在当前API进程内。"""

import hashlib
import hmac
import re
import secrets
from time import time

SESSION_SECONDS = 12 * 60 * 60


class BrowserSessions:
    """按进程轮换的签名会话，不复用API或模型凭据。"""

    def __init__(self, port: int) -> None:
        self.name = f"sr_web_{port}"
        self.secret = secrets.token_bytes(32)

    def issue(self) -> str:
        """每次入口生成新随机数，旧会话在原有效期内仍可并发使用。"""
        payload = f"{int(time())}.{secrets.token_urlsafe(24)}"
        signature = hmac.new(self.secret, payload.encode(), hashlib.sha256).hexdigest()
        return payload + "." + signature

    def valid(self, token: str) -> bool:
        """期限、结构和签名都必须有效，重启不会继承旧签名密钥。"""
        if not re.fullmatch(r"[0-9]{1,12}\.[A-Za-z0-9_-]{32}\.[0-9a-f]{64}", token):
            return False
        payload, signature = token.rsplit(".", 1)
        issued = int(payload.split(".", 1)[0])
        expected = hmac.new(self.secret, payload.encode(), hashlib.sha256).hexdigest()
        return 0 <= time() - issued <= SESSION_SECONDS and hmac.compare_digest(signature, expected)
