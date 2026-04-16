# -*- coding: utf-8 -*-
"""终端 UI — Claude Code 风格的精致 TUI。"""
from __future__ import annotations

import os

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import InMemoryHistory
from rich.console import Console
from rich.live import Live
from rich.markdown import Markdown
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

console = Console()

# prompt_toolkit 会话
_commands = WordCompleter(
    ["/help", "/clear", "/new", "/quit", "/exit", "/q", "/model", "/login", "/logout"],
    sentence=True,
)
_session: PromptSession | None = None


def _get_session() -> PromptSession:
    global _session
    if _session is None:
        _session = PromptSession(
            history=InMemoryHistory(),
            completer=_commands,
        )
    return _session


# ---------------------------------------------------------------------------
# 交互式输入辅助
# ---------------------------------------------------------------------------

def _prompt(label: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    try:
        val = console.input(f"  [dim]{label}{suffix}:[/dim] ").strip()
        return val or default
    except (EOFError, KeyboardInterrupt):
        return default


def _prompt_secret(label: str, current: str = "") -> str:
    import getpass
    masked = ""
    if current:
        masked = current[:6] + "..." + current[-4:] if len(current) > 12 else "***"
    suffix = f" [{masked}]" if masked else ""
    try:
        val = getpass.getpass(f"  {label}{suffix}: ").strip()
        return val or current
    except (EOFError, KeyboardInterrupt):
        return current


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

def print_banner(email: str = "", model: str = "") -> None:
    # 左侧：Logo + 身份
    left = Text()
    left.append("  ╦ ╦╦ ╦╔═╗╦╔═╔═╗╔═╗╔═╗\n", style="bold white")
    left.append("  ║║║╚╦╝║  ╠╩╗║ ║╠╣ ╠╣\n", style="bold white")
    left.append("  ╚╩╝ ╩ ╚═╝╩ ╩╚═╝╚  ╚\n", style="bold white")
    left.append("\n")
    left.append("  终端读盘室", style="bold blue")

    # 右侧：快速开始 + 状态
    right = Text()
    right.append("快速开始\n", style="bold yellow")
    if email:
        right.append("✓ ", style="green")
        right.append(f"{email}\n", style="dim")
    else:
        right.append("运行 ")
        right.append("/login", style="bold cyan")
        right.append(" 登录账号\n")
    if model:
        right.append("✓ ", style="green")
        right.append(f"{model}\n", style="dim")
    else:
        right.append("运行 ")
        right.append("/model", style="bold cyan")
        right.append(" 配置模型\n")
    right.append("\n")
    right.append("快捷命令\n", style="bold yellow")
    right.append("/login", style="cyan")
    right.append("  登录  ")
    right.append("/logout", style="cyan")
    right.append("  登出  ")
    right.append("/model", style="cyan")
    right.append("  模型\n")
    right.append("/clear", style="cyan")
    right.append("  新对话  ")
    right.append("/help", style="cyan")
    right.append("  帮助  ")
    right.append("/quit", style="cyan")
    right.append("  退出")

    layout = Table.grid(padding=(0, 3))
    layout.add_column(width=30)
    layout.add_column()
    layout.add_row(left, right)

    console.print()
    console.print(Panel(layout, title="[dim]Wyckoff CLI[/dim]", border_style="blue", padding=(1, 2)))
    console.print()


# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

def print_help() -> None:
    console.print()
    console.print("  [bold]命令[/bold]")
    console.print("  [cyan]/login[/cyan]   登录（邮箱密码，打通持仓和凭证）")
    console.print("  [cyan]/logout[/cyan]  登出")
    console.print("  [cyan]/model[/cyan]   配置模型（Provider / API Key / 模型名）")
    console.print("  [cyan]/clear[/cyan]   清空对话，开始新对话")
    console.print("  [cyan]/help[/cyan]    显示此帮助")
    console.print("  [cyan]/quit[/cyan]    退出")
    console.print()
    console.print("  [bold]直接输入问题开始对话：[/bold]")
    console.print("  [dim]帮我看看宁德时代 / 大盘现在什么水温 / 我的持仓还安全吗[/dim]")
    console.print()


# ---------------------------------------------------------------------------
# /login 交互式登录
# ---------------------------------------------------------------------------

def login_prompt() -> tuple[str, str] | None:
    """交互式输入邮箱密码。返回 (email, password) 或 None（取消）。"""
    console.print()
    email = _prompt("邮箱")
    if not email:
        return None
    password = _prompt_secret("密码")
    if not password:
        return None
    return email, password


# ---------------------------------------------------------------------------
# /model 交互式配置
# ---------------------------------------------------------------------------

PROVIDER_CHOICES = {
    "1": "gemini",
    "2": "claude",
    "3": "openai",
}

KEY_ENV_MAP = {
    "gemini": "GEMINI_API_KEY",
    "claude": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
}

DEFAULT_MODELS = {
    "gemini": "gemini-2.5-flash",
    "claude": "claude-sonnet-4-20250514",
    "openai": "gpt-4o",
}


def _save_to_dotenv(key: str, value: str) -> None:
    from dotenv import set_key
    env_path = os.path.join(os.getcwd(), ".env")
    set_key(env_path, key, value)
    os.environ[key] = value


def configure_model(state: dict) -> dict | None:
    console.print()
    console.print("  [bold]选择 Provider[/bold]")
    console.print("  [cyan]1[/cyan]) Gemini   [cyan]2[/cyan]) Claude   [cyan]3[/cyan]) OpenAI（含兼容端点）")
    console.print()

    cur_provider = state.get("provider_name", "")
    cur_num = ""
    for k, v in PROVIDER_CHOICES.items():
        if v == cur_provider:
            cur_num = k
            break

    choice = _prompt("输入编号", cur_num or "1")
    provider_name = PROVIDER_CHOICES.get(choice)
    if not provider_name:
        print_error(f"无效选项: {choice}")
        return None

    # --- API Key ---
    env_key = KEY_ENV_MAP.get(provider_name, "")
    env_val = os.getenv(env_key, "").strip() if env_key else ""

    if env_val:
        masked = env_val[:6] + "..." + env_val[-4:] if len(env_val) > 12 else "***"
        console.print(f"  [green]+[/green] {env_key} [dim]({masked})[/dim]")
        api_key = env_val
    else:
        api_key = _prompt_secret(f"输入 {env_key}")
        if not api_key:
            print_error("API Key 不能为空。")
            return None
        _save_to_dotenv(env_key, api_key)
        console.print(f"  [green]+[/green] 已保存到 .env")

    # --- Model ---
    model_env_key = f"{provider_name.upper()}_MODEL"
    if provider_name == "claude":
        model_env_key = "ANTHROPIC_MODEL"
    env_model = os.getenv(model_env_key, "").strip()
    if env_model:
        console.print(f"  [green]+[/green] {model_env_key} [dim]({env_model})[/dim]")
        model = env_model
    else:
        default_model = DEFAULT_MODELS.get(provider_name, "")
        model = _prompt("模型名称", default_model)
        if model and model != default_model:
            _save_to_dotenv(model_env_key, model)
            console.print(f"  [green]+[/green] 已保存到 .env")

    # --- Base URL (OpenAI only) ---
    base_url = ""
    if provider_name == "openai":
        env_base = os.getenv("OPENAI_BASE_URL", "").strip()
        if env_base:
            console.print(f"  [green]+[/green] OPENAI_BASE_URL [dim]({env_base})[/dim]")
            base_url = env_base
        else:
            console.print("  [dim]OpenAI 官方直接回车跳过，第三方兼容端点请输入 URL[/dim]")
            base_url = _prompt("Base URL")
            if base_url:
                _save_to_dotenv("OPENAI_BASE_URL", base_url)
                console.print(f"  [green]+[/green] 已保存到 .env")

    console.print()
    return {
        "provider_name": provider_name,
        "api_key": api_key,
        "model": model,
        "base_url": base_url,
    }


# ---------------------------------------------------------------------------
# 工具调用 / 响应渲染
# ---------------------------------------------------------------------------

# 正在运行的 Live spinner 实例
_live: Live | None = None


def _stop_live() -> None:
    """停止当前 spinner（如有）。"""
    global _live
    if _live is not None:
        _live.stop()
        _live = None


def print_tool_call(name: str, display_name: str, args: dict) -> None:
    """显示工具调用 — 带 spinner 动画。"""
    global _live
    _stop_live()

    args_brief = ""
    if args:
        args_str = ", ".join(f"{k}={v}" for k, v in args.items())
        args_brief = f" [dim]{args_str[:60]}{'...' if len(args_str) > 60 else ''}[/dim]"

    spinner = Spinner("dots", text=Text.from_markup(f"  [yellow]{display_name}[/yellow]{args_brief}"))
    _live = Live(spinner, console=console, refresh_per_second=10, transient=True)
    _live.start()


def print_tool_result(name: str, display_name: str, result) -> None:
    """显示工具执行完成 — 停止 spinner，打印结果。"""
    _stop_live()
    if isinstance(result, dict) and result.get("error"):
        console.print(f"  [red]✗ {display_name}[/red] [dim]{result['error']}[/dim]")
    else:
        console.print(f"  [green]✓ {display_name}[/green]")


def print_response(text: str) -> None:
    """渲染模型回复。"""
    _stop_live()
    console.print()
    console.print(Markdown(text), width=min(console.width, 100))
    console.print()


def print_error(message: str) -> None:
    console.print(f"  [red]{message}[/red]")


def print_info(message: str) -> None:
    console.print(f"  [dim]{message}[/dim]")


def get_input() -> str:
    try:
        return _get_session().prompt(HTML('<b><style fg="ansiblue">❯ </style></b>')).strip()
    except (KeyboardInterrupt, EOFError):
        # Ctrl+C 或 Ctrl+D 都退出
        console.print()
        return "/quit"
