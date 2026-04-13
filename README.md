# 🏛️ Wyckoff 分析与交易系统

> **致用户：本项目从用户体验角度由两部分核心组成，无需全员 Fork 或本地运行即可使用。**

1.  **Streamlit Web 交互中心（即开即用）**  
    **👉 [点击直接访问线上的 Web 页面](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)**  
    这是本系统的可视化大本营，已在云端部署。你可以在此查看最新的大盘水温、AI 研报结果、单股量化复盘、持仓管理以及回测记录。**大部分日常看盘需求在 Web 页面即可完成，无需自己 Fork 运行。**

2.  **云端漏斗筛选任务 (Wyckoff Funnel)**  
    这是后台的“计算工厂”，通过 GitHub Actions 定时执行全市场的量化初筛、AI 深度审问、飞书推送及风控任务。

---

用 [tushare](https://tushare.pro/) + 自研 Wyckoff 漏斗做量化初筛，交由大模型开展多空博弈深度剖析，最后由 OMS 负责执行约束。适合拒绝无脑黑盒、需“白盒逻辑 + 量化防守 + AI参谋预判”的 A 股散户投资者。

> [!TIP]
> **💡 进阶功能：个人持仓换股工作**  
> 本项目除公开筛选功能外，还深度支持**个人持仓的动态管理与“AI 换股决策”**（基于实盘持仓的 Tape Reading 与外部猎物替换法则）。由于此功能涉及私人 API Key 配置与资产安全，如需使用或定制此项服务，**请联系开发者获取支持**。

---

## 你能做什么

| 功能 | 说明 |
|------|------|
| 📊 **每日选股** | 配置 GitHub Actions 后，北京时间周日到周四 18:25 自动跑 Wyckoff Funnel，从主板+创业板筛选候选并推送飞书 |
| 📘 **策略手册** | 见 `README_STRATEGY.md`（含核心量化金融术语、风控公式及各层筛选指标执行口径） |
| 🔬 **Wyckoff Funnel** | Web 前台提交参数，GitHub Actions 在后台执行多层漏斗筛选：剥离垃圾 → 六通道强弱甄别 → Markup 识别 → 威科夫狙击 → AI 双轨候选 |
| 🤖 **AI 研报** | 对筛选结果生成三阵营判断（逻辑破产/储备营地/处于起跳板），含结构战区、确认条件及防踏空策略 |
| 🎓 **AI 分析（大师模式）** | 单股维持本地轻量分析（默认近 320 个交易日）；批量代码与漏斗候选改为后台研报任务，避免 Streamlit 长时间持有大批量 OHLCV 与模型上下文 |
| 🕶️ **私人决断** | 结合个人持仓与外部候选，生成 Buy/Hold/Sell 私密指令，并通过 Telegram 单独发送；自动跳过停牌股、验证数据日期对齐，AI 乱出止损价时自动降级为持有 |
| 🛡️ **RAG 防雷** | 基于新闻检索自动过滤有负面舆情的股票（立案/调查/减持/业绩预亏等） |
| 🧪 **日线回测** | 轻量回放 Funnel 命中后的 N 日收益，输出胜率/分位数（无需分钟级数据） |
| 📁 **行情导出** | Web 或命令行拉取指定股票日线，导出原始/增强两份 CSV（OHLCV 开高低收量等） |
| 🧰 **自定义导出** | 支持 A股/指数/ETF/宏观 CPI 等多数据源，灵活配置参数 |
| 📈 **持仓管理** | 实时同步持仓至云端并生成状态签名；内置 AI 订单建议面板、自动作废旧单与过时预警 |
| 🎯 **推荐跟踪** | 记录历史推荐股票及加入价，自动同步每日收盘价并计算累计收益；默认按涨幅→推荐次数→AI→日期→分值→现价多级排序，支持搜索与单列排序 |
| 📣 **市场信号栏** | 登录后页面顶部统一展示最新交易日的大盘水温、A50（盘前风向标）与 VIX（恐慌指数），并给出简洁中文操作提示 |
| 🕘 **下载历史** | 查看历史下载记录（最近 20 条） |
| 🔐 **登录与配置** | 支持登录；飞书 / 企微 / 钉钉 Webhook 及多厂商 API Key（Gemini、OpenAI、智谱、Minimax、DeepSeek、Qwen）云端同步 |

---

## 🧠 Wyckoff 量价分析 Skills

如果你只想把这套 **Wyckoff 量价分析思路** 以轻量方式复用到 OpenClaw / AI Agent，可以直接使用这个简易 Skills 仓库：

- **仓库地址**：[`YoungCan-Wang/wyckoff_skill`](https://github.com/YoungCan-Wang/wyckoff_skill.git)
- **定位**：将威科夫量价分析的核心提示词、判断框架、输出口径拆成可复用的简易 Skill
- **适合场景**：单股复盘、持仓诊断、候选股结构判断、给 AI 助手快速挂载一套“威科夫视角”

---

## 🚀 快速开始

### 1. 环境

需要 **Python 3.10+**。

```bash
cd Wyckoff-Analysis
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
python -m pip install -U pip
python -m pip install -r requirements.txt
```

### 2. 配置

复制 `.env.example` 为 `.env`，填入：

- `SUPABASE_URL`、`SUPABASE_KEY`：登录用（项目依赖 Supabase）
- `FEISHU_WEBHOOK_URL`（可选）：飞书推送地址；可选 `WECOM_WEBHOOK_URL`、`DINGTALK_WEBHOOK_URL` 实现企微/钉钉推送
- 大模型：可配置 `GEMINI_API_KEY` 或 OpenAI/智谱/Minimax/DeepSeek/Qwen 等厂商的 API Key 与模型（见设置页）

> Tushare 注册推荐：可通过 [此链接注册](https://tushare.pro/weborder/#/login?reg=955650)，双方可一起提升获取更高级股票数据的权益。

### 3. 运行

**Web 界面（推荐）**：浏览器里查数据、下 CSV。
单股分析、导出、持仓等轻量交互仍在 Streamlit 进程内执行；Wyckoff Funnel 与批量 AI 研报则通过页面按钮触发 GitHub Actions 后台任务。

```bash
streamlit run streamlit_app.py
```

**命令行**：批量导出。

```bash
python -m integrations.fetch_a_share_csv --symbol 300364
python -u -m integrations.fetch_a_share_csv --symbols 000973 600798 601390
```

---

## 📅 每日选股（Wyckoff Funnel）

从全市场（主板 + 创业板）多轮过滤，最终输出高胜率的精要标的，经过量化压缩后交由 AI 研判并推送到飞书。  
水温判断同时参考指数趋势 + 市场广度（站上 MA20 占比），弱市会自动收紧筛选与买入容忍度。

从 `v2.0.0` 起，**Web 端不再本地重算漏斗**。
Streamlit 页面负责提交股票池和参数，真正的全量取数、漏斗计算、AI 候选整理由 GitHub Actions 后台执行，页面只展示运行状态和轻量结果摘要。

### 漏斗筛选逻辑（多层）

| 层级 | 名称 | 筛选逻辑 |
|------|------|----------|
| Layer 1 | **剥离垃圾** | 剔除 ST/北交所/科创板，保留市值 ≥ 35 亿、日均成交额 ≥ 5000 万的股票 |
| Layer 2 | **六大独立通道甄选** | ① **主升**：MA多头+RPS双高+乖离≤25%（过滤高位鱼尾）；② **点火**：低位大阳爆量突破（乖离≤20%）；③ **潜伏**：长强短弱回踩年线；④ **吸筹**：低位横盘紧凑极度缩量；⑤ **地量**：创下年内极小地量枯竭；⑥ **护盘**：大盘新低但个股拒创新低底背离 |
| Layer 2.5 | **Markup 识别** | MA50 从下穿上 MA200 并连续确认 N 日，角度验证趋势强度，标注已进入上升趋势的股票 |
| Layer 3 | **板块共振** | 行业分布 Top-N，筛选与热门板块共振的标的 |
| Layer 4 | **威科夫微观狙击** | Spring（终极震仓假突破）、LPS（极度缩量≤40%的最后回踩）、SOS（低位跳跃小溪放量点火，乖离≤20%）、EVR（高位放量不跌的变异） |
| Layer 5/AI | **三阵营 AI 评判** | 将候选交由 LLM 进行独立审讯，输出“逻辑破产/储备营地/处于起跳板”三阵营决策。采用大盘水温动态压缩总容量配额，默认更偏向潜伏/吸筹候选，降低单边追强倾向，并在结构战区内提供 T+1 的 Plan A/B 计划说明 |

### 启用方式

仓库内置工作流：`.github/workflows/wyckoff_funnel.yml`

- **定时**：北京时间周日到周四 18:25
- **手动**：Actions 页面选择 `Wyckoff Funnel` → `Run workflow`

### Web 端后台任务架构

仓库内置专门给 Streamlit 页面使用的后台工作流：`.github/workflows/web_quant_jobs.yml`

- `Wyckoff Funnel` 页面：提交后台漏斗筛选任务
- `AI 分析` 页面：提交后台批量研报或输入预演任务
- 页面职责：触发工作流、轮询运行状态、读取 artifact 结果
- 后台职责：拉全量 OHLCV、跑漏斗、调用模型、产出轻量 JSON 结果

这样做的目的，是把最耗内存、最容易超时的链路从 Streamlit Community Cloud 进程里移走。

### 配置 GitHub Secrets

`Settings` → `Secrets and variables` → `Actions`，添加：

| 名称 | 必填 | 说明 |
|------|------|------|
| `FEISHU_WEBHOOK_URL` | 是 | 接收选股结果与研报 ([配置教程](https://open.feishu.cn/community/articles/7271149634339422210)) |
| `WECOM_WEBHOOK_URL` | 否 | 企微群机器人 Webhook，与飞书同时推送 |
| `DINGTALK_WEBHOOK_URL` | 否 | 钉钉自定义机器人 Webhook，与飞书同时推送 |
| `GEMINI_API_KEY` | 是 | AI 研报（或使用 `DEFAULT_LLM_PROVIDER` + 对应厂商 Key） |
| `DEFAULT_LLM_PROVIDER` | 否 | 定时任务使用的厂商：`gemini` / `openai` / `zhipu` / `minimax` / `deepseek` / `qwen`，未配则 `gemini` |
| `TUSHARE_TOKEN` | 是 | 行情与市值数据 |
| `GEMINI_MODEL` | 否 | 未配则用默认模型；其他厂商可配 `OPENAI_MODEL`、`ZHIPU_MODEL` 等 |
| `SUPABASE_URL` | Step4 用 | 否（走 `USER_LIVE:<user_id>` 路径时需要） |
| `SUPABASE_KEY` | ❌ | Supabase 匿名 Key；脚本侧可作为读取兜底。 |
| `SUPABASE_SERVICE_ROLE_KEY` | ❌ | Supabase 管理员 Key；若 Step4 需要稳定读写，建议优先配置。 |
| `SUPABASE_USER_ID` | ❌ | **用户锁定**：指定 Step4 运行的目标用户 ID。 |
| `MY_PORTFOLIO_STATE` | ❌ | **本地账本兜底**：若 `USER_LIVE:<user_id>` 不可用，可用 JSON 字符串配置持仓 (格式见 `.env.example`)。 |
| `TG_BOT_TOKEN` | ❌ | **私密推送**：Telegram Bot Token，用于接收私密交易建议。 |
| `TG_CHAT_ID` | ❌ | Telegram Chat ID。 |
| `TAVILY_API_KEY` | ❌ | **防雷**：用于 RAG 新闻检索 (Tavily)，推荐配置。 |
| `SERPAPI_API_KEY` | ❌ | **防雷备用**：Tavily 挂了时自动切换到 Google News (SerpApi)。 |

> **提示**：以上配置只在你需要对应功能时才需填写。最基础运行仅需前 3 项。

### 美股 S&P500 定时链路

仓库现在内置了一条独立的 US 数据维护链路，不再要求你手工维护 `US_FUNNEL_SYMBOLS` 这种逗号分隔 secret。

相关工作流：

- `.github/workflows/us_sp500_bootstrap.yml`：初始化任务，一次性回填当前 S&P500 成分股近 360 个交易日核心日线到 `stock_hist_cache`
- `.github/workflows/us_sp500_constituent_sync.yml`：每月同步一次 S&P500 成分股，并为新增成分股补历史数据
- `.github/workflows/us_daily_bar_refresh.yml`：每日为当前成分股补短窗口日线，用于补齐最新完整日 bar
- `.github/workflows/wyckoff_funnel_us.yml`：先同步 snapshot、再预热缓存、最后跑 US funnel

当前 US 版 funnel 的口径是：

- 股票池：S&P500 成分股
- 基准：`SPY`（主基准）+ `IWM`（小盘对照）
- 需要的数据：完整日 bar（OHLCV），并派生 `amount / pct_chg / amplitude`
- 不使用：A 股版行业层 / 市值层

当前推荐的调优参数都放在 workflow `env` 里，而不是写死在代码里：

- `US_SP500_BATCH_SIZE`
- `US_SP500_SLEEP_SECONDS`
- `US_SP500_BOOTSTRAP_DAYS`
- `US_SP500_SYNC_BOOTSTRAP_DAYS`
- `US_REFRESH_TRADING_DAYS`
- `FUNNEL_PREWARM_DAYS`

如果你发现 `yfinance` 在 500 只规模下偶发限流或空返回，优先调小 `US_SP500_BATCH_SIZE`，其次适当增大 `US_SP500_SLEEP_SECONDS`。

### Web 端后台任务所需 Streamlit Secrets

如果你希望在 Streamlit 页面里直接点按钮触发后台漏斗或后台批量 AI，还需要在 Streamlit Secrets 中配置：

| 名称 | 必填 | 说明 |
|------|------|------|
| `GITHUB_ACTIONS_TOKEN` | 是 | GitHub API Token，用于触发 `workflow_dispatch` 并读取 Actions 运行结果 artifact |
| `GITHUB_ACTIONS_REPO_OWNER` | 否 | 默认 `YoungCan-Wang` |
| `GITHUB_ACTIONS_REPO_NAME` | 否 | 默认 `Wyckoff-Analysis` |
| `GITHUB_ACTIONS_REF` | 否 | 默认 `main` |
| `GITHUB_ACTIONS_WORKFLOW_FILE` | 否 | 默认 `web_quant_jobs.yml` |
| `GITHUB_ACTIONS_ALLOWED_USER_IDS` | 否 | 逗号分隔的用户 ID 白名单；配置后仅白名单账号可在页面里触发后台任务 |

推荐使用**细粒度单仓库 Token**，至少授予：

- `Actions: Read and write`
- `Contents: Read`

### 验证

跑一次手动触发后，检查：

- 日志中有 `阶段汇总` 且 `ok=True`
- Artifacts 中有 `daily-job-logs-*`
- 飞书收到漏斗结果 + 研报

常规跑完约 90～130 分钟。报错时看日志里缺哪个配置。

### 日线回测（轻量）

不依赖分钟级和高价数据源，直接回放 Funnel 命中后的未来 N 日表现：

```bash
python -m scripts.backtest_runner \
  --start 2025-01-01 \
  --end 2025-12-31 \
  --hold-days 15 \
  --top-n 3 \
  --trading-days 320 \
  --board all \
  --exit-mode sltp \
  --stop-loss -9 \
  --take-profit 0 \
  --sample-size 300 \
  --output-dir analysis/backtest
```

当前脚本默认值是 `--hold-days 15 --top-n 3 --exit-mode sltp --stop-loss -9 --take-profit 0`。  
交易日窗口默认是 `--trading-days 320`。若要做持有周期对比，建议分别回测 `15 / 30 / 45 / 60` 四档。

回测偏差口径说明（重要）：
- 默认**关闭**当前截面市值/行业映射过滤（降低 look-ahead bias）。
- 若你要复现旧口径，可显式加 `--use-current-meta`（会引入前视偏差，仅用于对比）。
- 无论是否开启，仍存在幸存者偏差（股票池来自当前在市名单）。
- 回测默认纳入双边摩擦成本：`--buy-friction-pct 0.2 --sell-friction-pct 0.2`（可按券商与滑点实况调整）。

输出文件：
- `summary_*.md`：收益统计 + 风险统计（最大回撤、VaR95、CVaR95、最长连亏）
- `trades_*.csv`：逐笔信号收益明细

### 常见报错

- `配置缺失: FEISHU_WEBHOOK_URL`
  - 原因：未配置飞书 Secret
  - 处理：在仓库 Secrets 添加 `FEISHU_WEBHOOK_URL`
- `配置缺失: GEMINI_API_KEY`
  - 原因：未配置模型 Key 或已失效
  - 处理：更新 `GEMINI_API_KEY` 后重跑
- `市值数据为空（TUSHARE_TOKEN 可能缺失/失效）`
  - 原因：`TUSHARE_TOKEN` 缺失/失效/额度问题
  - 处理：检查并更新 `TUSHARE_TOKEN`，确认账号权限
- `[step3] 模型 ... 失败` / `llm_failed`
  - 原因：模型不可用、限流、网络抖动
  - 处理：更换 `GEMINI_MODEL` 或稍后重试
- `[step3] 飞书推送失败` / `feishu_failed`
  - 原因：Webhook 无效、限流、网络问题
  - 处理：重新生成飞书机器人 Webhook 并替换 Secret
- `阶段 3 私人再平衡: 跳过（SUPABASE_USER_ID 未配置/用户持仓缺失）`
  - 原因：未配置 `SUPABASE_USER_ID`，或 `USER_LIVE:<user_id>`/`MY_PORTFOLIO_STATE` 都不可用
  - 处理：在 Secrets 配置 `SUPABASE_USER_ID`；优先保证 Supabase 有 `USER_LIVE:<user_id>`，必要时提供 `MY_PORTFOLIO_STATE` 兜底
- `阶段 3 私人再平衡: 跳过（TG_BOT_TOKEN/TG_CHAT_ID 未配置）`
  - 原因：Telegram Secret 未配置
  - 处理：配置 `TG_BOT_TOKEN` 和 `TG_CHAT_ID` 后重跑
- `User location is not supported for the API use`
  - 原因：模型地域限制
  - 处理：更换可用网络出口或供应商
- `Action 超时或明显慢于 2 小时`
  - 原因：数据源抖动、重试变多
  - 处理：查看批次日志定位卡点，必要时手动重跑
- `US workflow 中 S&P500 snapshot 丢失 / added-removed diff 看起来不稳定`
  - 原因：GitHub runner 是冷启动，未命中上一次 `data/us_sp500_constituents.json` cache
  - 处理：这不影响功能，系统会自动 live fetch 当前成分股；若想降低波动，优先确认相关 US workflow 的 cache restore/save 正常执行

### 私人决断（可选）

Step4 完全由 GitHub Actions Secrets 驱动：读取 `SUPABASE_USER_ID` 定位 `USER_LIVE:<user_id>`，读取 `TG_BOT_TOKEN/TG_CHAT_ID` 推送 Telegram，模型使用 `GEMINI_API_KEY/GEMINI_MODEL`。若 Supabase 持仓缺失可用 `MY_PORTFOLIO_STATE` 做兜底。

---

## 交流

### 飞书群二维码

![飞书群二维码](attach/飞书群二维码.png)

### 飞书个人二维码

![飞书个人二维码](attach/飞书个人二维码.png)

---

## ☕ 赞助与支持

各位股友，觉得这个脚本筛选的股票形态很得您心，辛苦给个吆喝，点个star。如果这个借助这个脚本赚到钱了，也欢迎赞助作者一顿汉堡，升级下大模型和股票数据，非常感谢

| 支付宝 (Alipay) | 微信支付 (WeChat) |
| :---: | :---: |
| <img src="attach/支付宝收款码.jpg" width="250" /> | <img src="attach/微信收款码.png" width="250" /> |

---

## 🤖 Agent 架构

系统采用多 Agent 协作架构，基于 **OpenAI Agents SDK + LiteLLM** 构建。6 个专职 Agent 分工协作，其中 4 个确定性 Agent 负责数据计算，2 个 LLM Agent 负责需要自然语言判断的深度分析。

```
                     ┌─────────────────────┐
                     │  OrchestratorAgent   │ ← pipeline 编排 + retry + checkpoint
                     │      (确定性)         │
                     └──────────┬──────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  ScreenerAgent   │ │MarketContextAgent│ │  NotifierAgent   │
│    (确定性)       │ │    (确定性)       │ │    (确定性)       │
│                  │ │                  │ │                  │
│ 4层 Wyckoff 漏斗 │ │ 大盘水温 + regime │ │ 飞书/企微/钉钉/TG │
│ 4500只 → ~30只   │ │ RISK_ON/OFF/CRASH│ │ pipeline 汇总通知 │
└────────┬─────────┘ └──────────────────┘ └──────────────────┘
         │
         ▼
┌──────────────────┐     ┌──────────────────┐
│AnalystAgent  🧠  │────▶│StrategyAgent 🧠  │
│    (LLM 驱动)     │     │   (LLM 驱动)      │
│                  │     │                  │
│ 三阵营研报生成    │     │ OMS 持仓决策      │
│ 逻辑破产/储备/起跳 │     │ EXIT/HOLD/PROBE   │
└──────────────────┘     └──────────────────┘
```

### Agent 分工

| Agent | 类型 | 职责 | 包装的核心模块 |
|-------|------|------|---------------|
| **OrchestratorAgent** | 确定性 | pipeline 编排、stage 重试、checkpoint | 新增 |
| **ScreenerAgent** | 确定性 | 4 层 Wyckoff 漏斗筛选 | `wyckoff_engine.py` 77KB |
| **MarketContextAgent** | 确定性 | 大盘 regime 计算（RISK_ON/NEUTRAL/RISK_OFF/CRASH） | `wyckoff_funnel.py` benchmark 部分 |
| **WyckoffAnalystAgent** | 🧠 LLM | 三阵营深度研报（逻辑破产 / 储备营地 / 起跳板） | `step3_batch_report.py` + `prompts.py` |
| **StrategyAgent** | 🧠 LLM | 持仓去留决策 JSON（EXIT/TRIM/HOLD/PROBE/ATTACK） | `step4_rebalancer.py` + OMS 引擎 |
| **NotifierAgent** | 确定性 | 多渠道通知分发 | 飞书/企微/钉钉/Telegram |

### LLM Provider 支持（via LiteLLM）

通过 LiteLLM 统一适配层，一套代码支持任意 LLM provider：

| Provider | 状态 | 说明 |
|----------|------|------|
| Gemini | ✅ 主力 | 默认使用 `gemini-3.1-flash-lite-preview` |
| OpenAI | ✅ | GPT-4o / GPT-4o-mini |
| DeepSeek | ✅ | DeepSeek-Chat |
| 通义千问 (Qwen) | ✅ | DashScope OpenAI-compatible |
| Kimi (Moonshot) | ✅ | moonshot-v1 |
| 智谱 (Zhipu) | ✅ | GLM-4 |
| 火山引擎 (Volcengine) | ✅ | Doubao/ARK |
| Minimax | ✅ | abab 系列 |

### Pipeline 执行流程

```
触发 (GH Actions cron / Web / CLI)
  │
  ├─ [1] ScreenerAgent ──── 拉取 4500 只 A 股 OHLCV → 4 层漏斗 → ~30 只候选
  │      同时产出 benchmark_context (大盘指数 + 量价)
  │
  ├─ [2] MarketContextAgent ── 标准化 regime (RISK_ON / NEUTRAL / RISK_OFF / CRASH)
  │
  ├─ [3] WyckoffAnalystAgent 🧠 ── LLM 三阵营审判 → Markdown 研报 + 起跳板代码
  │      │  └─ RAG 防雷 (Tavily 新闻检索) → 剔除负面舆情股
  │      │
  │      ▼
  ├─ [4] StrategyAgent 🧠 ── LLM 持仓 + 新标的决策 → JSON → OMS 风控引擎
  │      └─ 输出: EXIT / TRIM / HOLD / PROBE / ATTACK 指令
  │
  └─ [5] NotifierAgent ──── 飞书 + 企微 + 钉钉 + Telegram 多渠道推送
```

每个 stage 执行后自动 checkpoint，失败时按 stage 重试（默认 2 次），不必从头重跑。

---

## 附录

### 目录结构

```text
.
├── streamlit_app.py        # Web 入口
├── app/                    # UI 组件（layout/auth/navigation）
│   ├── background_jobs.py  # Streamlit 侧后台任务状态管理
│   └── ...
├── agents/                 # 🤖 Agent 层（OpenAI Agents SDK + LiteLLM）
│   ├── contracts.py        # Agent 间数据契约（dataclass）
│   ├── llm_adapter.py      # LiteLLM 万能 provider 适配
│   ├── orchestrator.py     # OrchestratorAgent — pipeline 编排
│   ├── screener.py         # ScreenerAgent — 4 层漏斗筛选
│   ├── market_context.py   # MarketContextAgent — 大盘 regime
│   ├── analyst.py          # WyckoffAnalystAgent — LLM 三阵营研报
│   ├── strategist.py       # StrategyAgent — LLM OMS 决策
│   └── notifier.py         # NotifierAgent — 多渠道通知
├── core/                   # 核心策略与领域逻辑
│   ├── wyckoff_engine.py   # Wyckoff 多层漏斗引擎（六通道L2 + Markup L2.5）
│   ├── prompts.py          # AI 提示词（投委会 / 漏斗 / 私人决断）
│   ├── sector_rotation.py  # 板块轮动分析
│   ├── holding_diagnostic.py # 持仓诊断
│   ├── stock_cache.py      # 股票数据缓存
│   └── constants.py        # 常量定义
├── integrations/           # 数据源/LLM/Supabase 适配层
│   ├── data_source.py      # 统一数据源（tushare → akshare → baostock → efinance）
│   ├── llm_client.py       # LLM 客户端（原生多 provider）
│   ├── stock_hist_repository.py # Supabase 缓存 + gap-fill
│   ├── supabase_client.py  # Supabase 云端同步
│   ├── supabase_portfolio.py # 策略持仓同步
│   ├── supabase_recommendation.py # 推荐跟踪
│   ├── rag_veto.py         # RAG 防雷模块
│   └── github_actions.py   # GitHub Actions 触发与结果查询
├── pages/                  # Streamlit 页面
│   ├── AIAnalysis.py       # AI 分析（单股本地，批量后台）
│   ├── WyckoffScreeners.py # Wyckoff Funnel 后台筛选页
│   ├── Portfolio.py        # 持仓管理
│   ├── RecommendationTracking.py # 推荐跟踪
│   ├── CustomExport.py     # 自定义导出
│   ├── Settings.py         # 设置
│   └── Changelog.py        # 版本更新日志
├── scripts/
│   ├── run_pipeline.py     # 🆕 Agent Pipeline CLI 入口
│   ├── daily_job.py        # 日终流水线（legacy）
│   ├── wyckoff_funnel.py   # 定时选股任务
│   ├── step3_batch_report.py  # AI 研报
│   ├── step4_rebalancer.py    # 私人决断
│   ├── web_background_job.py  # Web 后台任务执行入口
│   ├── premarket_risk_job.py  # 盘前风控预警
│   └── backtest_runner.py  # 日线轻量回测
├── tests/
│   ├── agents/             # Agent 层测试
│   └── ...                 # 现有单元测试
├── requirements.txt
└── .env.example
```

### 盘前风险与市场信号栏

- 盘前风控任务会综合 `A50 + VIX` 判断外部风险，并输出 `NORMAL / CAUTION / RISK_OFF / BLACK_SWAN` 四档结果。
- Web 端会将“盘后大盘水温”与“盘前外部风险”融合成一条顶部市场信号栏，统一按“最新交易日”口径展示，不要求用户理解内部工程标签。
- 页面展示采用 A 股常用颜色习惯：上涨偏红、下跌偏绿；VIX 恐慌抬升也会按防守方向显示。

### 交易日与时间窗口

按交易日计算，自动跳过周末与节假日。

- 结束日（北京时间口径）：
  - `17:00-23:59` → 取 `T`（当天）
  - `00:00-16:59` → 取 `T-1`（上一自然日）
- 最终会对齐到最近交易日（自动跳过周末与节假日）
- 开始日：从结束日向前回溯 N 个交易日（默认 320）
- 参数：`--trading-days`、`--end-offset-days`

### CSV 输出说明

- **hist_data.csv**：akshare 原始字段（日期、开高低收、成交量、成交额、振幅、换手率等）
- **ohlcv.csv**：增强版（OHLCV + 均价、行业），便于回测与可视化

### 复权

`--adjust`：`""` 不复权，`qfq` 前复权，`hfq` 后复权。

```bash
python -m integrations.fetch_a_share_csv --symbol 300364 --adjust qfq
```

### 常见问题

- **ImportError: create_client from supabase** → `pip install supabase>=2.0.0`
- **macOS pip externally-managed-environment** → 用虚拟环境安装依赖
- **文件名有空格** → 股票名本身带空格，脚本会做安全替换

### AI 分析：Alpha 虚拟投委会

Web 端提供「AI 分析」页面，采用「Alpha」虚拟投委会模式，由七位历史级交易大师人格共同决策。

当前执行口径：

- **单股分析**：继续在 Streamlit 内本地执行，适合轻量、即时交互
- **批量代码分析**：改为 GitHub Actions 后台执行
- **漏斗候选分析**：先在后台 Funnel 页得到候选，再在 AI 页提交后台批量研报

这样既保留了单股即时性，也把最重的批量 OHLCV 和模型上下文从 Community Cloud 内存里移走。

大师分工如下：

| 大师 | 职责 |
|------|------|
| 🌊 **道氏与艾略特** | 宏观定势，判断牛市/熊市主趋势 |
| 💰 **彼得·林奇** | 价值透视，PEG 估值，六大股票分类 |
| 🕵️ **理查德·威科夫** | 主力侦察，吸筹/派发阶段判断，供需法则 |
| 📐 **缠中说禅** | 结构精算，中枢、背驰、买卖点数学定位 |
| 🔥 **情绪流龙头战法** | 情绪博弈，周期定位，龙头识别 |
| 🐊 **杰西·利弗莫尔** | 关键时机，关键点突破确认 |
| 🕯️ **史蒂夫·尼森** | 微观信号，蜡烛图反转形态 |

### RAG 防雷（负面舆情过滤）

每日选股流程中集成 RAG 防雷模块，自动过滤有负面舆情的股票：

- **数据源**：Tavily 新闻搜索（如未配置则跳过）
- **默认负面关键词**：立案、调查、证监会、处罚、违规、造假、退市、减持、质押爆仓、债务违约、业绩预亏、业绩下滑、商誉减值、诉讼、仲裁、冻结等
- **配置**：`TAVILY_API_KEY` 环境变量

### 数据源降级机制

个股日线优先使用 tushare（固定前复权 qfq），失败时依次降级：

```
tushare → akshare → baostock → efinance
```

- `TUSHARE_TOKEN` 未配置时自动跳过，回退到 akshare 优先
- 指数/大盘数据始终 tushare 直连，确保稳定性
- 可通过环境变量按需禁用各源：`DATA_SOURCE_DISABLE_AKSHARE=1` / `DATA_SOURCE_DISABLE_BAOSTOCK=1`

### 自定义导出支持的数据源

| 数据源 | 说明 | 复权支持 |
|--------|------|----------|
| A股个股历史 | 日线 K 线，6 位代码 | ✅ |
| 指数历史 | 上证/深证/创业板/北证 | ❌ |
| ETF 历史 | 510300 / 159707 等 | ✅ |
| 宏观 CPI | 月度 CPI 指标 | ❌ |

美股单股导出
python -m integrations.fetch_a_share_csv --market us --symbol AAPL --trading-days 320

美股多股导出
python -m integrations.fetch_a_share_csv --market us --symbols AAPL MSFT NVDA

### Fork 与部署

1. Fork 本仓库，克隆到本地
2. 按上文配置 `.env` 后运行
3. 部署到 [Streamlit Cloud](https://share.streamlit.io/)：入口选 `streamlit_app.py`
4. 至少配置 `SUPABASE_URL`、`SUPABASE_KEY`、`COOKIE_SECRET`
5. 若要启用页面内的后台漏斗/后台批量 AI，再额外配置 `GITHUB_ACTIONS_TOKEN`

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/)

---

## 版本更新

详见 [`CHANGELOG.md`](CHANGELOG.md)。Web 端「版本更新日志」页面直接读取该文件。

---

**开源协议 | License**

本项目基于 [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE) 开源。

- ✅ 个人学习、研究、自用：自由使用
- ✅ 修改和再分发：需保持同样的 AGPL-3.0 协议并开源
- ✅ 通过网络提供服务（SaaS）：必须向用户公开完整源代码
- 📧 **联系方式**：可通过微信/飞书，或 GitHub Issue 联系

Copyright (c) 2024-2026 youngcan.

---

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=YoungCan-Wang/Wyckoff-Analysis&type=Date)](https://star-history.com/#YoungCan-Wang/Wyckoff-Analysis&Date)
