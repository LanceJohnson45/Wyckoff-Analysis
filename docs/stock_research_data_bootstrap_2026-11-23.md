# Mac mini 行情数据接入说明（2026-11-23）

交给 Mac mini 上继续开发的执行者：该机器目前没有 `data/local_market.sqlite3`，需要先用已有同步入口从授权的云端 PostgreSQL 建立本机行情库，再推进 plan.md 阶段一。本文补充[开发交接](stock_research_handoff_2026-11-23.md)，不表示同步或服务接入已经在 Mac mini 上执行。

续接状态：上述为原始接入前检查点。Mac mini现已建立来源工作树、配置并执行首次同步，实际结果与剩余缺口见[最新交接检查点](stock_research_handoff_2026-11-23.md#mac-mini-续接检查点)和[数据CLI](stock_research_data_cli.md)，不要再按下文原始“无SQLite”的状态判断本机。

## 三项接入信息

| 项目 | 已核对的信息 | Mac mini 需要处理的部分 |
|---|---|---|
| 已有行情 SQLite | 原开发机 `/Volumes/E/github/Wyckoff-Analysis/data/local_market.sqlite3` | 这是原开发机路径，不是 Mac mini 路径；数据库未入 Git。用下面的同步命令在 Mac mini 创建新文件 |
| 现有同步入口 | `origin/codex/stock-research-source` 的 `scripts/sync_local_market.py` | 入口尚未迁入新研究分支，应在单独来源工作树运行 |
| Personal services | [LanceJohnson45/dsh-superagent-services](https://github.com/LanceJohnson45/dsh-superagent-services)，计划参照 `personal-agent-handoff` 分支的 `680a67ac88a73a30d345cb89f6e526a6d51b75a6` | 本机限定范围搜索未找到 checkout；Mac mini 的实际仓库路径和当前部署版本尚未核验，不能假定 |

来源分支已推送至 `dc5fec0e`；新研究基础代码检查点为 `01050f95`。不要整分支合并来源分支，按职责迁移行情逻辑和教程资料。

## 1. 准备来源工作树

从 Mac mini 的新研究仓库根目录执行，先确认本地改动：

```bash
git status --short
git fetch origin
git worktree add --detach ../Wyckoff-Analysis-source origin/codex/stock-research-source
cd ../Wyckoff-Analysis-source
uv sync --python 3.12 --frozen
.venv/bin/python scripts/sync_local_market.py --help
```

如果来源工作树已经存在，进入它核对 `git log -1 --oneline`，无需重复创建。来源工作树使用自己的依赖和锁文件；新研究分支当前没有该同步脚本及其完整依赖链。不要只从来源分支复制单个脚本后直接运行。

## 2. 配置行情读取与本机文件

同步脚本显式加载**来源工作树根目录的 `.env`**，环境中已有同名变量优先。使用既有授权的行情 PostgreSQL 连接，填写以下配置；不将真实值提交到 Git：

```dotenv
PG_ENABLED=1
PG_HOST=
PG_PORT=5432
PG_DATABASE=
PG_USER=
PG_PASSWORD=
PG_SSLMODE=require
PG_DSN=
LOCAL_MARKET_DB_PATH=/ABSOLUTE/MAC/PATH/local_market.sqlite3
```

将 `PG_*` 空值换成实际授权连接，将文件路径换成 Mac mini 上选定的绝对路径。`PG_SSLMODE` 按现有服务的实际设置填写；使用离散配置时保持 `PG_DSN` 为空，因为已有实现会优先使用非空 `PG_DSN`。脚本使用只读、可重复读云端连接，仅写本机 SQLite；不会执行云端 INSERT/UPDATE。

新研究工作树的 `.env` 中指向同一文件：

```dotenv
STOCK_RESEARCH_LOCAL_MARKET_PATH=/ABSOLUTE/MAC/PATH/local_market.sqlite3
```

两个键的路径必须相同，分别服务于现有同步脚本和新研究配置。未设置 `LOCAL_MARKET_DB_PATH` 时，旧脚本默认写入**来源工作树**的 `data/local_market.sqlite3`；新研究配置默认读取**研究工作树**的同名相对路径。两个工作树默认目录不同，因此跨工作树使用时应显式设置绝对路径。

`STOCK_RESEARCH_POSTGRES_*` 是新研究状态库的配置，不替代上述行情同步使用的 `PG_*`。按 plan.md 限定角色和 schema，分别核实授权；不要直接复制其他服务或网关的整份 `.env`。

## 3. 首次同步及后续更新

在已配置好 `.env` 的来源工作树运行：

```bash
# 首次复制云端已有的美股历史；缺失的 SQLite 和父目录会自动创建
.venv/bin/python scripts/sync_local_market.py --market us --full

# 此后按云端更新时间增量同步，包括历史行情修订
.venv/bin/python scripts/sync_local_market.py --market us
```

`--full` 表示忽略同步水位并重读云端当前历史，不删除本机更早历史。不要加 `--symbols` 来替代完整股票池同步；指定自选列表时只读取该列表和现有基准。不同自选范围分别维护水位。

同步退出码：0 为传输成功且无已登记异常，1 为配置/连接/写入等失败，2 为传输完成但存在异常日线隔离。输出 `sync` 和 `local` 的统计，包括股票数、行数、最新日期、水位和隔离数。退出码 0 也不代表新研究系统的 95% 日期覆盖、320 根窗口或完整指数并集已经验收。

该命令只复制云端已有数据，**不会在线补拉行情**，也不会重建云端缺失年份。云端行情落后或缺少标的，应先通过已登记的云端行情维护入口补齐，再同步；不要临时引入另一复权口径混写本机库。本地文件绑定云端来源身份，更换源数据库时使用另一个本机文件。

## 4. 当前数据缺口与下一步

原开发机的只读统计为美股股票 520 只、最近交易日 2026-11-20，benchmark 只有 SPY（401 根）。这是原开发机检查结果，Mac mini 同步后的结果要重新读取，不预设相同。

现有同步器只自动包含美股 `INDEX:US:SPY` 基准键，不覆盖计划所需 QQQ、IWM。即使把 QQQ/IWM 放进 `--symbols`，也会走 `US:` 个股键，不能当作补齐 benchmark 的实现。下一步需同时核对云端独立基准键、同步映射及本机 `kind=benchmark` 的读取路径。

标普500与纳斯达克100并集、成员版本、行业分类、320 根窗口、交易日覆盖率及一致快照仍待新研究阶段一实现。不能仅凭本机股票总数或同步成功，宣称正式数据阶段通过。

## 5. Personal services 核验

目前已知的是远端仓库和设计基线，**没有已确认的 Mac mini 本地仓库路径**。在该机器先检查已有运行进程、launchd 定义和部署目录，再确认实际 checkout、分支与提交，不假定仓库位于原开发机的 `/Volumes/E/github/` 下。

实际定位后只读执行 `git remote -v`、`git branch --show-current` 和 `git rev-parse HEAD`，与上述远端及设计基线核对，并记录实际路径和版本。如机器尚无 checkout，再在选定开发目录克隆：

```bash
git clone --branch personal-agent-handoff git@github.com:LanceJohnson45/dsh-superagent-services.git
```

克隆命令是待执行步骤，不是已部署状态证明；若分支已变动，应根据实际部署核对接口，不重置或覆盖现有服务。先验证 fixed argv、direct workflow、投递去重与回执契约，再登记 `stock-research-premarket`。Mac mini 的 SSH/Tailscale 身份、services 实际路径、数据库角色及通知配置仍需现场核验。

## 本说明的验证范围

已对照来源分支的 `scripts/sync_local_market.py`、`integrations/market_data_sync.py`、`integrations/local_market_store.py`、`integrations/postgres_base.py`、`.env.example` 和既有同步说明核对参数、配置优先级及返回码。原有测试证据见开发交接。本文没有执行 Mac mini 同步、生产数据库探测、services 克隆或通知发送。
