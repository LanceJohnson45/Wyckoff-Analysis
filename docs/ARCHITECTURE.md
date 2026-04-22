# 系统架构

[← 返回 README](../README.md)

## 系统全景

```
                    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                    │  Streamlit   │  │  CLI (TUI)   │  │  GitHub      │
                    │  Web UI      │  │  Terminal    │  │  Actions     │
                    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                           │                 │                 │
                           ▼                 ▼                 ▼
                    ┌─────────────────────────────────────────────────┐
                    │              Agent Brain                        │
                    │  Web: Google ADK  ·  CLI: Agent Loop + BG Task  │
                    │                                                 │
                    │  13 FunctionTools — LLM 自主编排                 │
                    │  自动 Plan Mode — 复杂任务拆步骤执行              │
                    └────────────────────┬────────────────────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    ▼                    ▼                    ▼
             ┌─────────────┐    ┌──────────────┐    ┌──────────────┐
             │ Core Engine │    │ LLM          │    │ Cloud Store  │
             │             │    │              │    │              │
             │ Funnel      │    │ Gemini  ★    │    │ Supabase     │
             │ Diagnostic  │    │ OpenAI       │    │  Portfolio   │
             │ Strategy    │    │ DeepSeek     │    │  Settings    │
             │ Signal      │    │ Qwen/Kimi    │    │  Hist Cache  │
             │ Sector      │    │ 智谱/火山    │    │  Recommend   │
             └──────┬──────┘    │ Minimax      │    └──────────────┘
                    │           └──────────────┘
                    ▼
             ┌─────────────┐
             │ Data Sources│
             │             │
             │ tickflow ★  │
             │ tushare     │
             │ akshare     │
             │ baostock    │
             │ efinance    │
             └─────────────┘
```

## Agent 架构

### 双通道复用

Web 和 CLI 共享同一套工具函数（`agents/chat_tools.py`）+ 同一份 System Prompt（`core/prompts.py`），通过不同运行时驱动：

| | Web | CLI |
|---|---|---|
| 运行时 | Google ADK `LlmAgent` | 裸写 Agent Loop（`cli/tui.py` → `cli/agent.py`） |
| UI | Streamlit 页面 | Textual 全屏 TUI |
| 工具数 | 12（共享） | 13（+`check_background_tasks`） |
| 后台任务 | ✗ | ✓ 长任务非阻塞 |
| 消息排队 | ✗ | ✓ Agent 忙时自动排队 |
| Thinking | ✗ | ✓ 推理模型 reasoning 展示 |
| Plan Mode | ✓ prompt 驱动 | ✓ prompt 驱动 |

### ReAct 循环（Reasoning + Acting）

Agent 采用 ReAct 范式：每一轮 LLM 先推理（Reason），再决定是否行动（Act），观察工具结果（Observe）后进入下一轮推理，直到能直接回答用户。

```
                        ┌──────────┐
                        │  用户输入  │
                        └────┬─────┘
                             │
                   ┌─────────▼──────────┐
                   │  Reason            │
                   │  LLM 推理 + 规划   │◄───────────┐
                   │  (thinking/text)   │            │
                   └─────────┬──────────┘            │
                             │                       │
                    ┌────────┴────────┐              │
                    │  需要 Act?      │              │
                    └───┬─────────┬───┘              │
                     No │         │ Yes              │
                        ▼         ▼                  │
                  ┌──────────┐  ┌──────────────┐     │
                  │ 输出回答  │  │  Act         │     │
                  └──────────┘  │  执行工具     │     │
                                │              │     │
                                │ 后台工具?     │     │  Observe
                                │  ├─Y→ submit │     │  工具结果
                                │  └─N→ 同步   │     │  注入上下文
                                └──────┬───────┘     │
                                       └─────────────┘
                                    (最多 15 轮)
```

单轮 ReAct 示例：

```
用户: "帮我看看宁德时代"

Round 1 — Reason: 用户要看宁德时代，需要先查代码
         Act:    search_stock_by_name("宁德")
         Observe: {"code": "300750", "name": "宁德时代"}

Round 2 — Reason: 拿到代码 300750，执行诊断
         Act:    diagnose_stock("300750")
         Observe: {health: "CAUTION", l2_channel: "潜伏", ...}

Round 3 — Reason: 有诊断数据了，综合输出结论
         Act:    无（直接回答）
         Output: "300750 宁德时代当前处于潜伏通道..."
```

### 工具清单

| # | 工具 | 显示名 | 说明 | 执行 |
|---|------|--------|------|------|
| 1 | `search_stock_by_name` | 搜索股票 | 代码⇄名字双向模糊搜索（多源降级） | 同步 |
| 2 | `diagnose_stock` | 读盘诊断 | 单只股票 Wyckoff 结构化诊断 | 同步 |
| 3 | `get_portfolio` | 查看持仓 | 持仓列表 + 可用资金，纯数据 | 同步 |
| 4 | `diagnose_portfolio` | 持仓审判 | 逐只持仓量价体检 | 同步 |
| 5 | `update_portfolio` | 调仓操作 | 新增/修改/删除持仓、设可用资金 | 同步 |
| 6 | `get_stock_price` | 调取行情 | 近期 OHLCV + 涨跌幅 | 同步 |
| 7 | `get_market_overview` | 大盘水温 | 主要指数涨跌幅 | 同步 |
| 8 | `screen_stocks` | 全市场扫描 | 五层漏斗筛选 | ⚡后台 |
| 9 | `generate_ai_report` | 深度审讯 | 三阵营 AI 研报 | ⚡后台 |
| 10 | `generate_strategy_decision` | 攻防决策 | 扫描→研报→决策全流程 | ⚡后台 |
| 11 | `get_recommendation_tracking` | 战绩追踪 | 历史推荐记录 + 涨跌幅 | 同步 |
| 12 | `get_signal_pending` | 信号确认池 | L4 信号确认进度 | 同步 |
| 13 | `check_background_tasks` | 任务状态 | 后台任务进度查询（CLI 专属） | 同步 |

### 工具路由原则

System Prompt 内建路由规则，LLM 自主判断调哪个工具：

- "我有什么持仓" → `get_portfolio`（纯数据，秒回）
- "持仓健康吗" → `diagnose_portfolio`（逐只诊断，较慢）
- "帮我加/删持仓" → `search_stock_by_name` → `update_portfolio`（先查名再改）
- "有什么机会" → `screen_stocks`（后台执行）

**铁律：一个工具能回答的问题，绝不调两个。用户没要求分析，就不要分析。**

### 自动 Plan Mode

复杂任务（≥2 个工具）自动进入 Plan Mode：

```
用户: "帮我全面分析一下现在的市场"
  │
  ▼
Agent 输出计划:
  1. 查大盘水温 → get_market_overview
  2. 全市场扫描 → screen_stocks（后台）
  3. 诊断持仓 → diagnose_portfolio
  4. 综合建议
  │
  ├─→ 逐步执行，每步汇报进度
  ├─→ 步骤间可动态调整（如大盘极弱则跳过进攻）
  │
  ▼
最终综合结论
```

### 后台任务架构

`cli/background.py` — `BackgroundTaskManager`

```
Agent → tool_call: screen_stocks
  │
  ├─→ ToolRegistry 检测为 BACKGROUND_TOOLS
  │   {"screen_stocks", "generate_ai_report", "generate_strategy_decision"}
  │
  ├─→ BackgroundTaskManager.submit() → daemon Thread 执行
  ├─→ 立即返回 {"status": "background", "task_id": "bg_xxx"}
  │
  ▼
Agent → "已提交后台，可继续提问"
  │
  │   （用户继续聊天...）
  │
  ▼   （后台线程完成）
on_complete 回调 → TUI 显示通知 → 结果注入消息队列 → Agent 自动汇报
```

用户可随时通过 `check_background_tasks` 查询进度。

### 消息排队

```
用户输入 → Agent 忙? ─No→ 立即处理
                      │
                      Yes→ 入 deque 队列，显示 "⏳ 已排队 (N)"
                              │
                              ▼ （当前任务完成后）
                         自动取队首消息 → 继续处理
```

`/new` 清对话时同步清空队列。

### CLI Provider 层

```
LLMProvider (abstract)              cli/providers/base.py
  │
  ├── GeminiProvider                google-genai SDK
  ├── ClaudeProvider                anthropic SDK
  └── OpenAIProvider                openai SDK + base_url + reasoning_content
```

统一接口：`chat_stream(messages, tools, system_prompt) → Generator[chunk]`

chunk 类型：`thinking_delta` | `text_delta` | `tool_calls` | `usage`

OpenAI provider 兼容所有 OpenAI API 格式端点（DeepSeek / Qwen / Kimi / LongCat 等），支持推理模型的 `reasoning_content` thinking 流。

### TUI 视觉层次

```
❯ 用户问题                           ← cyan 粗体

  💭 推理摘要…  (1234 字)             ← thinking：一行，dim italic
  ⚙ 搜索股票  keyword=宁德           ← tool 执行：黄色
  ✓ 搜索股票  0.3s                   ← tool 完成：绿色
  ✗ 调取行情  1.2s 超时              ← tool 失败：红色
  ↗ 全市场扫描  已提交后台            ← 后台任务：cyan
  ───                                ← 分隔线
  最终 Markdown 输出...              ← Markdown 渲染

  ↑1,234 ↓567 · 2.3s               ← token 统计：dim
```

### 本地持久化（~/.wyckoff/）

| 文件 | 用途 |
|------|------|
| `session.json` | Supabase 登录态（access_token / refresh_token） |
| `wyckoff.json` | 模型配置（provider / api_key / model / base_url） |

启动自动恢复：登录态 → `restore_session()`；模型 → `load_model_config()`。登录过期提示 `/login`。

## 五层漏斗引擎

`core/wyckoff_engine.py`，~60 可调参数（`FunnelConfig`）。

| 层 | 名称 | 逻辑 |
|----|------|------|
| L1 | 剥离垃圾 | 剔除 ST / 北交 / 科创，市值 ≥ 35 亿，成交额 ≥ 5000 万 |
| L2 | 六通道甄选 | 主升 / 点火 / 潜伏 / 吸筹 / 地量 / 护盘 |
| L2.5 | Markup 识别 | MA50 上穿 MA200 + 角度验证 |
| L3 | 板块共振 | L2 通过股票行业分布，保留 Top-N 行业 |
| L4 | 微观狙击 | Spring / LPS / SOS / EVR 触发信号 |
| L5 | 退出信号 | 初始止损 -7%、利润激活线 +15%、跟踪止损（高点回撤 -10% 或跌破 MA50）、派发警告（高位缩量 3 天） |

## 信号确认状态机

`core/signal_confirmation.py`，L4 信号经 1-3 天价格确认：

```
pending ──(价格确认)──→ confirmed（可操作）
   └──(超时)──→ expired（失效）
```

TTL：SOS 2 天、Spring 3 天、LPS 3 天、EVR 2 天。

## Pipeline（定时任务）

```
cron (周日-周四 18:25 北京)
  ├─→ Step 2: 全市场 OHLCV → 五层漏斗 → ~30 候选
  ├─→ Step 3: LLM 三阵营研报 → 飞书推送
  └─→ Step 4: LLM 持仓决策 → OMS 风控 → Telegram 推送
```

## 数据源

```
tickflow(★) → tushare → akshare → baostock → efinance   （行情 OHLCV，五级降级）
tushare → akshare + 本地 24h 缓存              （股票列表，代码⇄名字映射）
```

## 云端存储（Supabase）

| 表 | 用途 |
|----|------|
| `portfolios` | 投资组合元数据 |
| `portfolio_positions` | 持仓明细 |
| `trade_orders` | AI 交易建议 |
| `user_settings` | 用户配置（API Key / Webhook） |
| `stock_hist_cache` | 行情缓存（qfq，滚动 400 天） |
| `recommendations` | 推荐跟踪 |
| `signal_pending` | 信号确认池 |

数据隔离：Web JWT → RLS，CLI access_token → RLS，脚本 service_role_key → 绕过 RLS。
