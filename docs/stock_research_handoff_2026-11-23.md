# 股票研究开发交接（2026-11-23）

用户决定转到 Mac mini 继续开发，此处保存开发检查点，不表示 plan.md 六个阶段已交付。下一位执行者先读根目录 AGENTS.md、plan.md 和因子出处核对，再接续阶段一。

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
