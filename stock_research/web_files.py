"""只服务专用研究构建的入口与资产，不兜底任意文件。"""

import re
from pathlib import Path

from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, Response
from starlette.staticfiles import StaticFiles
from starlette.types import Scope


class ResearchWebFiles:
    """显式产物目录，禁止源码入口、隐藏文件和目录外符号链接。"""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("研究静态资源须配置绝对目录")
        self.root = root.resolve(strict=True)
        self.entry = (self.root / "research.html").resolve(strict=True)
        assets = (self.root / "assets").resolve(strict=True)
        if not self.entry.is_relative_to(self.root) or not assets.is_relative_to(self.root) or not assets.is_dir():
            raise ValueError("研究静态资源不能引用目录外文件")
        if self.entry.stat().st_size > 65536:
            raise ValueError("研究HTML产物越界")
        content = self.entry.read_text(encoding="utf-8")
        if (
            '<meta name="stock-research-entry" content="1"' not in content
            or not re.search(r'src="/assets/[A-Za-z0-9_.-]+\.js"', content)
            or "/src/" in content
        ):
            raise ValueError("研究入口须使用build:research的专用构建")
        self.assets = StaticFiles(directory=self.root, follow_symlink=False)

    def html(self) -> FileResponse:
        """HTML不使用条件缓存，续会话请求必须取得完整入口。"""
        entry = self.entry.resolve(strict=True)
        if not entry.is_relative_to(self.root):
            raise HTTPException(404)
        return FileResponse(entry, media_type="text/html", headers={"Cache-Control": "no-store"})

    async def asset(self, path: str, scope: Scope) -> Response:
        """只允许构建资产类型，StaticFiles再核对真实路径边界。"""
        if not re.fullmatch(r"assets/[A-Za-z0-9_][A-Za-z0-9_.-]*\.(?:js|css|png|svg|jpe?g|webp|woff2?)", path):
            raise HTTPException(404)
        response = await self.assets.get_response(path, scope)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response
