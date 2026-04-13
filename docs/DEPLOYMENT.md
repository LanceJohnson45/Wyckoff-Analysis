# Wyckoff Analysis 部署文档

本文档按三部分说明这个项目如何部署：

1. GitHub CI：用于校验和定时任务
2. Docker 后端：用于批处理与调度工作流
3. Web 站点：用于 Streamlit 前端

## 1. 项目部署模型

这个仓库不是传统的 API 优先后端。
它本质上是一个 Streamlit 应用，加上一组负责执行 Wyckoff 漏斗、AI 研报、持仓决策和数据维护的批处理脚本。

实际部署时，核心落点是：

- GitHub Actions：负责 CI 和定时自动化
- Docker Python 运行环境：负责后端批处理任务
- Streamlit Cloud 或容器化 Streamlit：负责 Web 站点

### 主要入口

- Web UI：`streamlit_app.py`
- 日常流水线：`scripts/daily_job.py`
- 新流水线入口：`scripts/run_pipeline.py`
- Web 触发后台任务：`scripts/web_background_job.py`
- 漏斗定时器：`scripts/wyckoff_funnel.py`
- 回测任务：`scripts/backtest_runner.py`

## 2. GitHub CI

### 当前 CI 做什么

现有工作流位于 `.github/workflows/ci.yml`，会在推送和 Pull Request 到 `main` 和 `feature/visible` 时运行。

它会执行：

- 安装 Python 3.11
- 安装 `requirements.txt` 里的依赖
- 做包导入健康检查
- 对所有源码模块做 Python 编译检查
- 执行 `tests/` 下的 pytest
- 对 `scripts/daily_job.py` 做 dry-run

### 这一步的意义

CI 能提前发现：

- 依赖破坏
- 语法错误
- 测试回归
- 主流水线所需环境变量缺失

### 推荐的 CI 策略

保持 CI 简洁且稳定：

- 固定 Python 3.11，和仓库配置保持一致
- 每个 PR 都跑单元测试
- 保留 dry-run 步骤，用来验证 Secret 和启动逻辑，不直接触发真实任务
- GitHub Actions artifacts 只用于长任务结果，不要混进 CI 主流程

### 相关工作流

仓库里还有一些面向生产的自动化工作流：

- `.github/workflows/wyckoff_funnel.yml`：定时漏斗筛选
- `.github/workflows/web_quant_jobs.yml`：Streamlit 触发的后台任务
- `.github/workflows/backtest_grid.yml`：回测任务
- `.github/workflows/premarket_risk.yml`：盘前风险检查
- `.github/workflows/recommendation_tracking_reprice.yml`：推荐跟踪刷新

## 3. Docker 后端部署

### 后端是什么

在这个项目里，后端指的是批处理运行时。
它会执行：

- Wyckoff 漏斗
- AI 研报生成
- 持仓决策逻辑
- 维护类任务
- 回测和数据修复任务

仓库里没有单独的 Web API 服务。

### 推荐的 Docker 策略

用一个统一的 Python 镜像承载所有批处理任务，然后按任务切换容器命令。
这样可以让 CI、定时任务和手工执行保持一致的运行环境。

### 推荐 Dockerfile

仓库目前没有内置 Dockerfile，下面是建议的基础版本。
它沿用 Python 3.11，并从 `requirements.txt` 安装项目依赖。

```dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml ./
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install -e .

COPY . .

CMD ["python", "-m", "scripts.run_pipeline", "--trigger", "manual"]
```

### 推荐容器命令

同一镜像可以通过不同命令执行不同任务：

```bash
# 运行主流水线
python -m scripts.run_pipeline --trigger manual

# 运行旧版日常任务
python scripts/daily_job.py

# 只运行漏斗定时逻辑
python scripts/wyckoff_funnel.py

# 执行 dry-run 校验
python scripts/daily_job.py --dry-run
```

### 推荐的 Docker 运行方式

生产环境建议拆成两个逻辑服务：

- Web 容器：运行 Streamlit UI
- Worker 容器：运行定时或按需的批处理任务

如果部署在单机上，worker 也可以由 cron、systemd timer、GitHub Actions 或外部调度器触发。

### 推荐的 docker-compose 结构

如果你要做本地或自建服务器部署，可以用下面的最小结构。

```yaml
services:
  web:
    build: .
    command: streamlit run streamlit_app.py --server.address=0.0.0.0 --server.port=8501 --server.enableCORS=false --server.enableXsrfProtection=false
    ports:
      - "8501:8501"
    env_file:
      - .env

  worker:
    build: .
    command: python -m scripts.run_pipeline --trigger manual
    env_file:
      - .env
```

### 后端环境变量

后端任务依赖密钥和可选的外部服务。
最关键的是这些：

- `TUSHARE_TOKEN`
- `FEISHU_WEBHOOK_URL`
- `GEMINI_API_KEY` 或其他厂商 Key
- `DEFAULT_LLM_PROVIDER`
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_USER_ID`
- `TG_BOT_TOKEN`
- `TG_CHAT_ID`
- `TAVILY_API_KEY`
- `SERPAPI_API_KEY`
- `MY_PORTFOLIO_STATE`

### 后端部署检查清单

- 确认容器里执行 `python -m scripts.run_pipeline --dry-run` 成功
- 如果还保留旧入口，确认 `python scripts/daily_job.py --dry-run` 也能通过
- 确认 webhook 和 LLM 相关 Secret 已注入到目标环境
- 如果需要查看执行结果，挂载或持久化 `logs/` 目录
- 定时任务保持时区为 `Asia/Shanghai`

### US S&P500 数据链路（GitHub Actions）

如果要启用美股 Wyckoff Funnel，当前推荐直接复用仓库内置的 4 条 workflow：

- `us_sp500_bootstrap.yml`：首次初始化，回填约 360 个交易日历史数据
- `us_sp500_constituent_sync.yml`：每月同步成分股并为新增股票补历史
- `us_daily_bar_refresh.yml`：每日补最新完整日 bar
- `wyckoff_funnel_us.yml`：同步/预热后执行 US funnel

US 这条链路当前不依赖 `US_FUNNEL_SYMBOLS` secret，而是自动维护 S&P500 成分股 snapshot。snapshot 文件为 `data/us_sp500_constituents.json`，并通过 GitHub Actions cache 在多次运行间恢复。

建议把下面这些参数保留为 workflow env，便于线上限流时直接调优：

- `US_SP500_BATCH_SIZE`
- `US_SP500_SLEEP_SECONDS`
- `US_SP500_BOOTSTRAP_DAYS`
- `US_SP500_SYNC_BOOTSTRAP_DAYS`
- `US_REFRESH_TRADING_DAYS`
- `FUNNEL_PREWARM_DAYS`

经验上，如果 `yfinance` 返回空数据或出现明显抖动：

1. 先把 `US_SP500_BATCH_SIZE` 从 `40` 下调到 `20`
2. 再把 `US_SP500_SLEEP_SECONDS` 从 `0.5/1.0` 提高到 `1.5~2.0`
3. 仍不稳定时，再考虑把 bootstrap / sync / refresh 拆得更稀疏

## 4. Web 站点部署

### Web 站点是什么

Web 站点就是由 `streamlit_app.py` 驱动的 Streamlit 前端。
它提供：

- 大盘概览
- AI 分析页面
- 漏斗筛选界面
- 导出工具
- 持仓和推荐跟踪
- 设置和维护页面

### 当前的 Web 部署方式

仓库本身已经默认采用 Streamlit 优先的部署模型。
README 推荐对外站点部署到 Streamlit Cloud。
`.devcontainer/devcontainer.json` 里也把 `streamlit run streamlit_app.py` 作为启动命令。

### Streamlit Cloud 部署步骤

1. Fork 仓库，或者把仓库连接到 Streamlit Cloud。
2. 将应用入口设置为 `streamlit_app.py`。
3. 在 Streamlit Secrets 中配置需要的密钥。
4. 部署并确认应用能正常启动，不缺少任何凭据。

### 前端所需的 Streamlit Secrets

最少需要配置：

- `SUPABASE_URL`
- `SUPABASE_KEY`
- `COOKIE_SECRET`

如果你希望 Web 界面还能触发后台任务，再补充配置：

- `GITHUB_ACTIONS_TOKEN`
- `GITHUB_ACTIONS_REPO_OWNER`
- `GITHUB_ACTIONS_REPO_NAME`
- `GITHUB_ACTIONS_REF`
- `GITHUB_ACTIONS_WORKFLOW_FILE`
- `GITHUB_ACTIONS_ALLOWED_USER_IDS`

### Web 运行命令

如果你选择自托管或容器化部署，推荐命令如下：

```bash
streamlit run streamlit_app.py --server.address=0.0.0.0 --server.port=8501 --server.enableCORS=false --server.enableXsrfProtection=false
```

### Web 部署注意事项

- 这个应用大量使用 Streamlit session state 和缓存
- 需要让实例保持足够热，缓存才有价值
- 不要把重型批处理任务放进 Streamlit 主进程里执行
- 漏斗和批量 AI 任务应通过 GitHub Actions 后台工作流跑

## 5. 推荐的部署拓扑

一个比较实用的生产拆分是：

- GitHub Actions 负责 CI 和定时批处理
- Docker 镜像负责后端任务和可选的自建批处理服务
- Streamlit Cloud 负责 Web 站点

This matches the repository design and avoids forcing heavy OHLCV and LLM workloads into the interactive web process.

## 6. Operational Checklist

Before going live:

- Run CI locally or in GitHub Actions and ensure tests pass
- Validate the backend dry-run path
- Confirm all required secrets exist in the target environment
- Verify the web UI opens and can read Supabase configuration
- Trigger one funnel job and one web background job end-to-end
- Check that artifacts and logs are produced as expected

## 7. Quick Reference

### GitHub CI

- File: `.github/workflows/ci.yml`
- Purpose: test, compile, import sanity, dry-run validation

### Scheduled backend jobs

- File: `.github/workflows/wyckoff_funnel.yml`
- File: `.github/workflows/wyckoff_funnel_us.yml`
- File: `.github/workflows/us_sp500_bootstrap.yml`
- File: `.github/workflows/us_sp500_constituent_sync.yml`
- File: `.github/workflows/us_daily_bar_refresh.yml`
- File: `.github/workflows/web_quant_jobs.yml`
- Main CLI: `scripts/daily_job.py`
- New CLI: `scripts/run_pipeline.py`

### Web site

- Entry: `streamlit_app.py`
- Recommended platform: Streamlit Cloud
- Alternative: containerized Streamlit service
