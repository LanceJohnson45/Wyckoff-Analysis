# 股票研究开发交接（2026-11-23）

用户决定转到 Mac mini 继续开发，此处保存开发检查点，不表示 plan.md 六个阶段已交付。下一位执行者先读根目录 AGENTS.md、plan.md 和因子出处核对，再接续阶段一。

## 本机运行配置与同步续接

本节更新晚于下方“Mac mini续接检查点”，配置不再阻塞当前数据开发。

- 本机已新建独立 `stock_research` 数据库及同名登录角色，角色无superuser/createdb/createrole，研究库内public不可创建对象，仅研究schema可创建。随机密码仅写入根目录0600的 `.env`；没有复用云端行情密码或修改云端权限。实际 `init-schema` 成功，修正已有schema在受限账号下不应再次尝试CREATE SCHEMA的问题。
- 真实旧缓存诊断快照已提交本机研究库，164782根日线经独立新连接重读、摘要匹配，质量仍为false；没有把缺基准的结果改成合格。
- 新增 `stock-research sync-market --market us [--full]`，直接使用研究根目录的离散 `PG_*`。云端事务可重复读且只读，新范围 `research:us:all:benchmarks-v1` 包含三个独立基准；不复用旧SPY-only水位。来源身份保持旧代码JSON编码约定。新入口真实初次同步195132行/98批、1条隔离，第二次增量750行/1批、新拒收0但仍有1条未解决隔离，退出码均2。
- 新入口本机文件锁与快照协作，云端事务锁约束使用同一入口的跨机器同步。旧Actions和来源脚本未参与，不能声称全体系更新互斥。中断保留已提交批次但不推进水位，重跑可补齐。
- 上一提交保留的SQL文件在被Git跟踪后触发门禁，已删除，改为Python schema定义加 `schema-ddl` 导出；导出DDL在隔离PostgreSQL执行验证。根AGENTS已补充与实际质量脚本一致的无SQL文件约束。

仍缺正式当前成分及分类更新、QQQ/IWM源数据补齐、WBD源异常修复和旧更新入口的协调。这些是开发待办，不需要用户继续填写当前行情/研究库凭据。模型、通知和API鉴权到对应阶段再核对，不声称当前已可运行双策略研究。最新命令见[数据CLI](stock_research_data_cli.md)。

本批研究测试63项通过、无skip，包括受限schema账号初始化、只读源写入拒绝、三个基准与同名股票区分、历史修订增量、隔离修复、源身份不符、协作锁冲突、缺修订时间以及取消后重跑。Ruff、1125个Python文件格式、完整quality_gate、工作流和红线扫描通过；LOC旧基线警告和68条依赖非阻断警告保留。CI同款假值daily dry-run退出0，不代表真实模型或通知就绪。未运行全量pytest、Web检查和远端CI；独立wiki checkout仍不存在，未声称已同步。

## Mac mini 续接检查点

已快进接收 `cd52ed4f`，新增数据阶段CLI和真实PostgreSQL集成测试。命令、schema与限制见[数据CLI](stock_research_data_cli.md)。以下旧章节保留为 `01050f95` 时的交接记录，以本节为最新状态。

- 已接通版本构建/导入、只读一致行情、质量门槛、文件物化和PostgreSQL事务存取。新增 `stock-research` 包入口与doctor、snapshot、read-snapshot、build-versions、import-version、init-schema命令。行情与历史修订、完整版本、窗口及质量规则参与摘要；快照不是策略选股报告。
- 已建立 `/Users/wxjer/Documents/github/Wyckoff-Analysis-source`，分离来源分支和Python环境，未整分支合并。两工作树的 `.env` 均为0600和Git忽略；用户将另一机器 `.env` 拷入研究根目录后，只将 `PG_*` 行情键同步到来源工作树，并配置共享绝对SQLite路径。其他原有配置保留，未复制网关整份环境文件。
- 实际执行来源 `scripts/sync_local_market.py --market us --full`，写入195132行、98批。源快照水位为北京时间2026-11-23 20:32:07；本机股票520只、194757根，SPY基准375根，最近交易日2026-11-20。1根WBD 2026-10-05异常已隔离，同步退出码2；没有修补或伪造该行。
- 在只读、可重复读云端连接核对 `INDEX:US:SPY/QQQ/IWM` 及 `US:QQQ/IWM`，仅SPY有数据。QQQ/IWM不是单纯本机同步遗漏，云端补齐和独立kind映射仍待实施。
- 来源静态指数清单转换得到516股并集、512条缓存分类，已保存为带限制的候选文件，未安装为正式版本。另以明确标注的旧缓存回溯样本运行真实本机离线快照：日期对齐511/516、覆盖约99.03%，质量失败；缺两个基准、WBD隔离、少量缺数/分类及历史不足均进入报告。该运行只验证数据链路，不证明实际当前/历史成分，不作为策略证据。产物保留在忽略的 `.stock-research/`。
- Personal平台实际路径为 `/Users/wxjer/Documents/github/openclaw/dsh-superagent-platform`，旁边services checkout为 `dsh-superagent-services`，读取版本 `d405f961`、分支 `codex/personal-backlog-20261123`。未改动或重启服务；checkout不等于部署版本。

本轮验证：研究测试52项通过，无skip，包含临时原生PostgreSQL建表DDL、事务提交、独立重读、连接池重建、并发幂等、版本冲突回滚、历史修订和CLI子进程。Ruff、全仓格式检查、函数长度、工作流检查通过；quality_gate仍有旧LOC基线增长警告，依赖检查有68条非阻断警告（包括仅脚本入口修改导致的manifest/lock mtime提示），`uv lock --check`通过。生产daily dry-run初次发现旧虚拟环境MiniRacer命名空间残留，按现有锁重新安装mini-racer后恢复导入；再用CI规定的假值配置执行dry-run退出0，未调用行情、模型或发送通知。实际生产LLM配置仍有缺项，假值检查不代表真实模型就绪。

阶段一尚未总体验收：正式成分来源更新、行业资料刷新、QQQ/IWM云端补齐及旧更新入口参与协调仍缺。研究数据库已在后续接通，见顶部续接记录；当前doctor的行情/数据库检查通过，正式universe/classifications未安装。后续Martin/Wyckoff策略、AKQuant对拍、dsh、React、服务注册及调度均未交付。未跑全量pytest、前端检查、远端CI或独立整体审查，不能声称可合入或可发布。

**Mac mini 缺少行情 SQLite 时，先读[行情数据接入说明](stock_research_data_bootstrap_2026-11-23.md)**：包含原机文件位置、来源工作树准备、首次/增量同步、两个路径配置的对应关系，以及 Personal services 已知远端和待核实的本地路径。

## 两个分支

- `codex/stock-research`：新研究应用的开发分支，基线严格为上游 `22dc8c653f66486ae408e1ebfafdbfd2da43ba3c`。包含 plan.md、因子出处核对、研究依赖和基础模块。
- `codex/stock-research-source`：从原工作区 `563169c3` 建立的来源保留分支。`d7348d4c` 保存原工作区的美股行情与工作流改动，`dc5fec0e` 保存计划、因子出处和教材更新。既有 `core/martin_luk.py`、`integrations/local_market_store.py`、`integrations/market_data_sync.py` 也在该分支，可逐项迁移。

不要整分支合并来源分支：两套产品基线差异很大，新的工作台应沿用 React/CLI 方向，避免恢复旧 Streamlit。来源分支只是精确保留用户已有工作，未宣称这些工作流已经部署、完整回归或可直接合入新 main。

## 本次实际改动

- `stock_research/config.py`：Pydantic Settings、独立配置前缀、SQLAlchemy asyncpg URL，测试库强制派生 `_test` 后缀。这里只构造配置，尚未连接数据库。
- `stock_research/identity.py`：UUID4、Asia/Shanghai 操作时间、拒绝 NaN 的规范化 JSON 与 SHA256。
- `stock_research/calendar.py`：exchange-calendars XNYS 交易日、前一交易日、开盘和开盘前60分钟期限；处理夏冬令时和节假日。
- `stock_research/models.py`：指数成员有效期和多重归属、公司分类、OHLCV 与时区校验、质量报告、快照内容摘要和未来行情拒收契约。
- `tests/stock_research/test_foundations.py`：供应商代码归一化、成员有效期和去重、异常行情与时区、夏冬令时、节假日、测试库隔离、摘要边界。
- `pyproject.toml` 和 `uv.lock`：新增可选 `research` 依赖，AKQuant 固定 0.3.93。FastAPI 采用 0.165.0，兼容上游 MCP 已要求的 Starlette 1.x；exchange-calendars 固定 4.13.3。
- `.env.example` 列出当前研究配置；`.stock-research/` 运行目录已忽略。

新模块目前只有测试调用，生产路径**未接线**。未注册不存在的 `stock-research` CLI 入口。API token 和数据库角色权限需在服务实现时落实，不能将配置模型当作鉴权已完成。

## 尚未实施

阶段一仍缺股票池来源更新、分类版本构建、只读行情适配、一致快照、同步锁、质量门槛执行、PostgreSQL schema/迁移/存储和 doctor/snapshot 命令。后续 Martin 因子、Wyckoff 生产适配、dsh、双策略互评、React 工作台、services 接入及 Mac mini 部署均未实现。AKQuant 仅已安装，尚未做公式对拍、性能比较或历史实验。

尚未对新研究存储进行真实 PostgreSQL 集成验证，未连接生产库，未执行迁移，未启动研究服务，未注册定时任务，未发送飞书通知。

## 实测与限制

在研究分支、Python 3.12 环境执行：

```bash
uv sync --python 3.12 --extra dev --extra research
.venv/bin/python -m pytest tests/stock_research -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python scripts/quality_gate.py --ci
.venv/bin/python scripts/check_workflow_hygiene.py
.venv/bin/python scripts/check_dependency_hygiene.py
```

基础测试 15 项通过；Ruff 通过；1104 个 Python 文件格式通过；quality_gate 无新增函数长度违规；工作流与依赖检查通过。质量脚本同时给出旧 LOC 基线增长警告，依赖脚本给出 67 项现有范围版本等非阻断警告，不能把它们描述为零警告。没有执行全量 pytest、前端检查或远端 CI，未达到可合入/可发布验收。

来源保留分支使用原项目 Python 3.12 虚拟环境执行以下离线测试，18 项通过：

```bash
python3.12 -m pytest tests/test_us_yfinance_backfill.py tests/test_us_funnel_data_guardrails.py -q
```

教程的 Word/HTML 产物与核验文件按原工作区原样保留，本次未重新渲染或独立审查。

## 本机已知数据与部署缺口

原工作区 `data/local_market.sqlite3` 只读统计：美股股票 520 只，最近日期 2026-11-20；benchmark 只有 SPY，401 根。QQQ 和 IWM 缺失，完整指数并集未核验，不能据此生成正式合格报告。SQLite 文件未入 git，Mac mini 需要按既有授权数据源重建同步。分类缓存和现有指数 JSON 有历史时点限制，不得无说明地当作严格历史成分。

本机 dsh npm 安装目录存在，版本与接口仍需实际握手；本次没有执行模型调用。尚未得到 Personal services 本地仓库路径、目标 Mac mini SSH 身份或生产连接配置，应在 Mac mini 读取实际部署后继续接入，不猜测或复制网关凭证。

原工作区的未提交内容另外保存在本机 `~/.codex/backups/wyckoff-research-20261123-200244/`，有权限受限的压缩包、二进制 patch 和逐文件 SHA256 摘要。这个本机备份不会随 git 传输。原工作区未被改写；`.codegraph/daemon.pid`、`tmp/` 渲染临时文件与本地数据库不在来源分支的新提交中。

## Mac mini 续接

```bash
git fetch origin
git switch --track origin/codex/stock-research
uv sync --python 3.12 --extra dev --extra research
.venv/bin/python -m pytest tests/stock_research -q
git worktree add ../Wyckoff-Analysis-source origin/codex/stock-research-source
```

如果开发分支已存在，改用 `git switch codex/stock-research` 后检查本地改动再快进。从来源工作树逐项核对行情、教材和公式；不要复制 `.env` 或整个旧应用。下一步先完成 plan.md 阶段一，再推进 Martin 与 Wyckoff 适配。
