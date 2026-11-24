# Personal 的只读股票研究接入

本机 DSH「Personal Agent」预设已加载独立 `stocks` MCP。股票查询复用 `stock_research.query_mcp` 和股票 API 的共享只读身份；原 Personal 服务的 11 项工具、个人身份与 `personal_quotes` 的持仓范围保持。新建 Personal 会话使用新修订，已有会话保留其已绑定修订。OpenCode 的既有 Personal MCP 仍只有原只读八项，不会自动继承 DSH 预设配置。

## 工具与范围

| DSH 工具名 | 行为 |
|---|---|
| mcp__stocks__stock_research_latest_report | 查询最新固定报告，可指定目标交易日；读取实际行情日期、partial/late及量化状态 |
| mcp__stocks__stock_research_stock | 使用报告中的 run_id；追问时同时传 report_id 固定修订，读取完整冻结股票池中的观点与事实 |
| mcp__stocks__stock_research_run_status | 查询对应运行的当前进度、已提交结果与失败原因；不能用后来补齐改写旧报告 |

这是独立研究系统的共享信号，不是个人持仓或交易权限。查询前先核对目标日与实际行情日期；未知计算、null和成功空池分别表达。工具不提供新研究、重试、参数发布、私人问答、订单或通知。API 只读身份也不能读取私人问答。研究结论不是 BUY。

## 预览和安装

管理员使用已有研究安装及专用 `query.env`（0600）。新命令只读取配置元信息，不加载服务端 Settings，不读取或输出令牌内容：

```bash
.venv/bin/stock-research personal-mcp
# 核对目标 profile、plugin 与 before_hash 后，先检查 Personal 相关运行状态
.venv/bin/stock-research personal-mcp --apply --expected-profile-hash SHA256_FROM_PREVIEW
```

默认目标为 `~/.dsh/profiles/superagent/cordis.patch.yml`，可显式指定 `--profile`、`--workspace`、`--python`、`--backup-dir`。解释器路径保留虚拟环境入口。MCP 由 `/usr/bin/env -i` 使用固定 argv 启动，仅设置 PATH 与 Python UTF-8；不继承 PG、模型、网关或查询令牌环境变量，只在研究工作目录加载自己的查询配置。

命令只给唯一 `preset-personal-agent` 的插件块追加一项，不执行 YAML 的自定义标签；重复键、别名复用、身份或命名空间冲突均拒绝自动处理。移除新增节点后的结构必须与原文件一致，原文本也保持。显式写入核对预览摘要，并在合作式文件锁内再次检查原内容；0600备份和临时文件完成后才原子替换。已配置且一致时不重写文件。

输出的 `runtime_verified=false` 表示该命令只确认文件。维护者还须核对宿主实际预设诊断、工具目录和真实读取；不能把 `applied=true` 当作运行就绪。DSH 预设按修订管理已有 Agent，修改声明不等于给旧会话补工具。

## 精确撤回

```bash
.venv/bin/stock-research personal-mcp --remove
.venv/bin/stock-research personal-mcp --remove --apply --expected-profile-hash CURRENT_PREVIEW_HASH
```

撤回只移除本次命名的股票 MCP，保留后来的其他配置；不要求原查询工作区或配置文件仍存在。原备份用于核对，不直接整份恢复覆盖别人的新改动。撤回针对预设声明，已有会话仍由 DSH 修订生命周期管理。无需删除研究或 Personal 数据。

## 本机验证与部署边界

配置正本同步到 services 的 `operator-config/personal-agent.patch.yml`，运行 profile 由同一命令增量添加。本机运行宿主的已认证只读 Remote 已读回新插件，其他 Personal 插件的规范化摘要保持，预设没有 broken 诊断；未重启共享宿主或修改其他预设。

隔离 DSH 宿主实际挂载该 Personal 预设，保留原 11 项并新增三项。经工具执行链依次读取 Personal 能力、研究最新报告、GILD 固定观点和对应运行，模型轮数为零；子进程组已回收。这是运行接线与数据读取验证，真实界面控件另验。证据、备份及具体提交见[交接检查点](stock_research_handoff_2026-11-23.md)。

上述初次接线检查时尚未安装正式API或定点任务。后续API/独立股票tick已采用d42，唯一调度与通知状态见[MVP](stock_research_mvp.md)；查询客户端本身仍为同机checkout路径，不能沿用API采用记录声称它已安装。

## 当前只读链路复核

2026-11-25以当前实际DSH Personal预设的stocks声明启动独立stdio MCP，目录仅三项只读工具；真实读取11月24日固定修订14、IEX的11月23日事实和原运行，身份一致。该查询身份读取私人问答及评估均403；无模型轮次，配置全文摘要和原查询进程PID集合保持，所属新子进程已回收。本次不是共享DSH宿主完整目录重载验收。客户端仍从Documents下的研究checkout启动，固定安装目录迁移及宿主实际新修订读取仍待完成，不把新API部署等同于查询客户端部署。

## 受管只读客户端迁移

原注册已经存在时，默认安装仍拒绝覆盖。显式`--replace`只接受既有规范stocks声明，允许改变查询工作目录和Python解释器；清空继承环境、stdio、工具身份、调用超时和启动失败策略必须原值。旧目录或其query.env已不可用时也可迁移，校验不执行旧解释器。缺注册、重复、别名、命名空间冲突或额外配置拒绝操作，`--replace`与`--remove`互斥。

```bash
stock-research personal-mcp --replace --workspace /operator/fixed-query-workspace --python /operator/fixed-release/.venv/bin/python
stock-research personal-mcp --replace --workspace /operator/fixed-query-workspace --python /operator/fixed-release/.venv/bin/python --apply --expected-profile-hash SHA256_FROM_PREVIEW
```

目标代码、锁定解释器及0600专用查询配置需先由维护者准备；该命令不会复制身份或安装依赖。预览不写文件、不加载服务端Settings或启动进程。替换在内存中计算完整候选并验证移除stocks后其他文本逐字相同，只通过已有文件锁/CAS/0600备份/原子rename发布一次，没有先撤回再追加的磁盘空档。无变化重放不重写文件；后来配置变化须重新预览。

运行采用与回滚还须分别核对实际宿主预设修订和工具目录。可用同一`--replace`把受管节点迁回旧固定目录并绑定当前预览摘要，保留后来其他插件配置；已有会话继续使用原预设修订。`runtime_verified=false`不因迁移成功变成true。

此能力当前为源码候选，线上查询声明尚未切换。固定d42客户端候选已准备在秘密目录：1979文件及实际解释器/加载位置核验通过，原身份普通文件0600、目录0700；实际三工具及私有资源403通过，原profile/PID集合保持，无模型轮次。完整迁移CLI门禁、精确CI、共享宿主修订与operator镜像继续作为采用前置条件，API/股票tick不换包。

本批最终31项聚焦及完整Python6307项通过、无skip，Ruff/格式/函数与质量、工作流/依赖检查及CI同形操作dry-run通过；源码指纹在最终全量前后相同。第一次全量6304项之后另发现插件排序缺陷，保留原日志并在修复后重跑上述最终全量，不以中间通过替代交付。真实当前配置的CLI预览与影子CAS摘要一致，原配置未写；15次正式业务读取全成功、P95 0.534秒，五类响应摘要与四类研究表投影不变，四类活动0。当前提交CI、共享宿主采用与其他完整计划要求继续分别核验。
