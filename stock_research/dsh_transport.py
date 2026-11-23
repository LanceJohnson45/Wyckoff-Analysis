"""受限NDJSON SDK传输；顺序请求、有限输出及整组子进程回收。"""

import asyncio
import json
import os
import signal
from pathlib import Path

from stock_research.dsh_profile import ALLOWED_TOOLS


class DshProtocolError(RuntimeError):
    """Harness协议、事件来源或预算不满足执行契约。"""


def reject_json_constant(value: str) -> None:
    """JSON-RPC不接受NaN/Infinity等非JSON常量。"""
    raise ValueError("非有限JSON常量")


class DshTransport:
    """单阶段拥有一个Harness进程和一个SDK会话。"""

    def __init__(self, process: asyncio.subprocess.Process, session_id: str) -> None:
        self.process = process
        self.session_id = session_id
        self.events: list[dict[str, object]] = []
        self.requests: list[dict[str, object]] = []
        self.responses: list[dict[str, object]] = []
        self.stderr = bytearray()
        self.request_id = 0
        self.bytes_read = 0
        self.tool_calls = 0
        self.stderr_task = asyncio.create_task(self.read_stderr())

    @classmethod
    async def start(cls, executable: Path, root: Path, environment: dict[str, str], session_id: str) -> "DshTransport":
        """argv不经shell，独立进程组便于取消时回收MCP后代。"""
        process = await asyncio.create_subprocess_exec(
            str(executable),
            "--profile",
            "research",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=root,
            env=environment,
            start_new_session=True,
            limit=2 * 1024 * 1024,
        )
        return cls(process, session_id)

    async def read_stderr(self) -> None:
        """诊断也限额，不能让未消费的stderr堵住模型进程。"""
        while chunk := await self.process.stderr.read(4096):
            self.stderr.extend(chunk)
            if len(self.stderr) > 1024 * 1024:
                self.kill_group(signal.SIGKILL)
                return

    async def request(self, method: str, params: dict[str, object] | None = None) -> object:
        """等待当前响应时完整接收通知，拒绝日志污染和异阶段事件。"""
        self.request_id += 1
        packet = {"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params}
        self.requests.append(packet)
        self.process.stdin.write((json.dumps(packet) + "\n").encode())
        await self.process.stdin.drain()
        while True:
            try:
                line = await self.process.stdout.readline()
                item = json.loads(line, parse_constant=reject_json_constant)
            except ValueError:
                raise DshProtocolError("Harness输出不是有界JSON-RPC帧") from None
            if not isinstance(item, dict):
                raise DshProtocolError("Harness帧须为对象")
            self.bytes_read += len(line)
            if self.bytes_read > 32 * 1024 * 1024 or item.get("jsonrpc") != "2.0":
                raise DshProtocolError("Harness输出超限或协议无效")
            if "method" in item:
                self.record_event(item)
            elif item.get("id") == self.request_id:
                self.responses.append(item)
                if "error" in item:
                    raise DshProtocolError("Harness请求失败")
                return item.get("result")
            else:
                raise DshProtocolError("Harness响应身份不匹配")

    def record_event(self, item: dict[str, object]) -> None:
        """未知工具或超预算立即终止，不能等待模型自行纠正边界。"""
        self.events.append(item)
        params = item.get("params", {})
        if not isinstance(params, dict) or not isinstance(params.get("event", {}), dict):
            raise DshProtocolError("Harness事件结构无效")
        if (
            item["method"] in {"subagent.started", "subagent.finished"}
            or params.get("sessionId", self.session_id) != self.session_id
        ):
            raise DshProtocolError("Harness产生了越界会话")
        event = params.get("event", {})
        if event.get("type") == "tool/call":
            self.tool_calls += 1
            data = event.get("data")
            if not isinstance(data, dict) or data.get("name") not in ALLOWED_TOOLS or self.tool_calls > 20:
                raise DshProtocolError("Harness工具调用越界或超限")

    def kill_group(self, signum: int) -> None:
        """只操作本实例创建的独立进程组，进程已退出属正常收尾。"""
        try:
            os.killpg(self.process.pid, signum)
        except ProcessLookupError:
            pass

    async def close(self) -> None:
        """先请求SDK关闭，所有分支均限时并最终回收整组进程。"""
        try:
            if self.process.returncode is None:
                try:
                    await asyncio.wait_for(self.request("shutdown"), 2)
                except (TimeoutError, DshProtocolError, BrokenPipeError, ConnectionResetError):
                    self.kill_group(signal.SIGTERM)
                try:
                    await asyncio.wait_for(self.process.wait(), 2)
                except TimeoutError:
                    self.kill_group(signal.SIGKILL)
                    await asyncio.wait_for(self.process.wait(), 2)
        finally:
            self.kill_group(signal.SIGKILL)
            self.stderr_task.cancel()
            await asyncio.gather(self.stderr_task, return_exceptions=True)
