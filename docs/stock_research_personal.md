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

上述初次接线检查使用同机checkout，后续API/独立股票tick已采用d42，唯一调度与通知状态见[MVP](stock_research_mvp.md)。查询客户端于11月25日另行迁移并完成共享宿主与真实页面验收，见下文，不沿用API采用记录作为客户端部署证据。

## 当前只读链路复核

2026-11-25以当前实际DSH Personal预设的stocks声明启动独立stdio MCP，目录仅三项只读工具；真实读取11月24日固定修订14、IEX的11月23日事实和原运行，身份一致。该查询身份读取私人问答及评估均403；无模型轮次，配置全文摘要和原查询进程PID集合保持，所属新子进程已回收。本次不是共享DSH宿主完整目录重载验收。该次复核时客户端仍从Documents开发目录启动；后续固定安装与共享宿主实跑见下文，独立stdio证据不冒充共享宿主证据。

## 受管只读客户端迁移

原注册已经存在时，默认安装仍拒绝覆盖。显式`--replace`只接受既有规范stocks声明，允许改变查询工作目录和Python解释器；清空继承环境、stdio、工具身份、调用超时和启动失败策略必须原值。旧目录或其query.env已不可用时也可迁移，校验不执行旧解释器。缺注册、重复、别名、命名空间冲突或额外配置拒绝操作，`--replace`与`--remove`互斥。

```bash
stock-research personal-mcp --replace --workspace /operator/fixed-query-workspace --python /operator/fixed-release/.venv/bin/python
stock-research personal-mcp --replace --workspace /operator/fixed-query-workspace --python /operator/fixed-release/.venv/bin/python --apply --expected-profile-hash SHA256_FROM_PREVIEW
```

目标代码、锁定解释器及0600专用查询配置需先由维护者准备；该命令不会复制身份或安装依赖。预览不写文件、不加载服务端Settings或启动进程。替换在内存中计算完整候选并验证移除stocks后其他文本逐字相同，只通过已有文件锁/CAS/0600备份/原子rename发布一次，没有先撤回再追加的磁盘空档。无变化重放不重写文件；后来配置变化须重新预览。

运行采用与回滚还须分别核对实际宿主预设修订和工具目录。可用同一`--replace`把受管节点迁回旧固定目录并绑定当前预览摘要，保留后来其他插件配置；已有会话继续使用原预设修订。`runtime_verified=false`不因迁移成功变成true。

采用前检查点：当时尚未切换线上查询声明。固定d42客户端候选已准备在秘密目录：1979文件及实际解释器/加载位置核验通过，原身份普通文件0600、目录0700；实际三工具及私有资源403通过，原profile/PID集合保持，无模型轮次。完整迁移CLI门禁、精确CI、共享宿主修订与operator镜像继续作为采用前置条件，API/股票tick不换包。

本批最终31项聚焦及完整Python6307项通过、无skip，Ruff/格式/函数与质量、工作流/依赖检查及CI同形操作dry-run通过；源码指纹在最终全量前后相同。第一次全量6304项之后另发现插件排序缺陷，保留原日志并在修复后重跑上述最终全量，不以中间通过替代交付。真实当前配置的CLI预览与影子CAS摘要一致，原配置未写；15次正式业务读取全成功、P95 0.534秒，五类响应摘要与四类研究表投影不变，四类活动0。此处记录本地门禁；随后精确CI与共享宿主采用结果见下一节，其他完整计划要求继续分别核验。

## 固定客户端采用与真实Personal问答

2026-11-25上海07:41，迁移器b79c2e60的[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45509833217)全部completed/success后，实际Personal stocks声明与services operator镜像已通过单次CAS切到`~/.dsh/secrets/stock-research-query-d42fa219`，代码和锁定解释器来自d42不可变包；未替换API/股票tick。共享宿主PID不变，新MCP子进程的真实cwd/解释器已核对，其他插件全文与原Personal会话投影保持。

真实Personal页面选择新预设并发送一次验收问题，GLM-5.3完成1轮/3步、实际只调用三项stocks工具，页面显示55秒。固定报告修订14、行情截止11月23日、IEX两策略入选与四阶段状态均与独立HTTP重读相符；固定报告和个股全文一致，运行状态仅动态observed_at变化。实际宿主目录共33项，其中原Personal 11项、stocks 3项，其余为既有宿主工具；另有一次系统标题模型请求。未调用个人记录/记忆或通知工具，旧会话保持。专用身份读取私人问答/评估仍403；迁移后15次业务读取全成功、P95 0.457秒，原五类HTTP摘要及四类研究表投影保持、全workspace四类活动0。

首次迁移观察把预设热重载期间的暂时未注册误当失败，已用当前CAS预览反向迁回并核对原状态；观察器补有界45秒等待后，第二次操作确认新声明（3.46秒）。没有因观察超时重复发布，`runtime_verified=false`仍是CLI自身的诚实范围；以上实机验收另存。

本次另发现DSH会把过长工具结果截成约50KB文本：latest_report与stock分别省略3414/4955字节，完整格式化结果保存在宿主临时spill文件。独立校验读取原完整结果通过；本次回答正确不能代替输出完整性修复，后续须在不丢事实/范围/版本的前提下消除不可访问的截断。浏览器保留两条验收前的扩展连接错误，本次问答期间无新增error；不声称全页从未报错。自然日、真实通知unknown及完整计划剩余项仍未完成。

## 长结果完整续页

原三项MCP现采用QueryPage v2，保持工具/权限及HTTP/CLI契约；scope回显原版本，next_cursor在同工具/原参数下读取同一结果。所有页按JSON path可重建全部原事实与摘要，单页实际SDK文本≤32KiB，暂存5分钟/8结果/8MiB，过期或重启明确拒绝。真实原结果2/4/2页和IEX320根历史8页均完整对照通过；该处为采用前SDK验证；08:32后的新Personal会话已完成真实模型全页读取，旧会话保留原预设。详见[分页契约](stock_research_queries.md#mcp-v2完整续页)。

本批最终完整Python6313通过/无skip、50条既有警告（308.85秒），34聚焦及Ruff/1360文件格式/函数质量、workflow/dependency hygiene与CI同形操作dry-run通过；最终全量前后源码指纹相同。原大小复现循环已由失败转为通过，另逐页重建证明全部原事实保留；这不是把首页面当作完整结果。15次正式业务读取全成功/P95 0.336秒，原五类HTTP摘要和四类研究表投影保持、四类活动0。此为采用前本地门禁；随后ece83八项CI及新会话实际采用均已通过，实际证据另见本页08:32记录。

## 08:32 完整分页实际采用

2026-11-25上海08:32，ece83f6f的[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45517132603)全部completed/success后，Personal查询客户端及operator镜像已采用完整分页v2。固定代码在`~/.local/share/stock-research/clients/query-pages-ece83f6f`，1977个Git文件与最终全量源码指纹核验通过；工作区为`~/.dsh/secrets/stock-research-query-pages-ece83f6f`，原专用身份0600保持，仍复用d42锁定解释器。实际只改变stocks节点cwd；API/股票tick/42计划、共享宿主PID及其他配置不变。清空环境查询入口的60个已加载项目模块均未落在Documents内。

新Personal页面的一次GLM-5.3问答完成1轮/7步、8次stocks只读调用，报告/个股/运行分别读完2/4/2页。独立原生记录逐页核对原参数/游标、result_id、摘要与终止条件，8份模型可见文本全为完整JSON，最大32736字节；全部chunks重建原模型和摘要，固定报告/个股与HTTP全文相同，当前run仅observed_at变化。修订14、11月23日截止、两策略正式初选及四阶段状态逐项一致；模型在报告后并行读取个股和运行的续页，未将不同结果拼接。实际目录33项、其他30项定义及原11项Personal保持，3个旧Personal会话投影完全相同；私人问答/评估仍403，未调用Personal/知识/文件/通知工具。另有一次系统标题生成。

本次新页面无error，截图及原生回执分别保存。页面用时74秒；完整读取增加模型轮次，不能据此宣称提速或降成本。采用后15次正式业务读全成功、P95 0.431秒，原五类HTTP摘要及四类研究表投影保持，全workspace四类活动0。

已有会话继续原预设修订，旧v1会话不会自动变成分页v2；使用本次新能力应新建Personal会话，并以实际schema_version=2核对。CLI预览的runtime_verified=false保持原义，实机采用/执行由以上独立证据确认。自然交易日、真实通知unknown及完整计划其余交付仍未完成。


09:55 API/tick另采用八项CI成功的633固定包；Personal声明、ece83代码及d42解释器保持。按现有声明启动的清空环境stdio在新API完整读回三项结果2/8/2页（IEX320根），原模型/摘要与HTTP一致、每页≤32736字节，owned进程已回收，原进程/live profile保持，模型调用0。此为SDK兼容检查，原08:32页面/真实模型证据单列；详见[新API采用](stock_research_mvp.md#2026-11-25并发准备采用)。
