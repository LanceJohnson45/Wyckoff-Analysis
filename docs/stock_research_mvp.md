# 单用户盘前 MVP

首期只向既有Personal个人飞书收件人发送。固定入口为 `python -m stock_research.mvp --config /absolute/private/path/mvp.env`，业务输入和输出沿用[盘前任务](stock_research_premarket.md)的runner文件协议。配置模板为 `config/research/mvp.env.example`，只含operator固定路径，0600；业务输入不能提供命令、路径或凭据。

## 采用含评估作业的新API版本

评估API源码加入 `evaluation_jobs/results/samples` 三表，需在研究库显式执行新版本 `init-schema`，保留原账本；启动只检查，不自动迁移。2026-11-25上海03:58，正式本机API/调度采用 `09bdd3cb`时，新增评估三表由公开CLI显式初始化；该批次后续运行版本为下文06:38采用的d42fa219。09提交的[完整八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45474002419)全部成功；原运行包保留供撤回。下一次采用须先取得该提交完整成功CI，再确认研究、问答、输入准备及评估作业均无pending/running任务；取消评估要等所属进程真实退出，不能只改数据库状态。截至2026-11-26 02:10，当前运行版本为文末`doctor-wiring-c0ae327b`。

运行包切换继续限于已有独立API/调度，不修改原42条Personal计划及共享服务。回退保留新三表和已保存结果，不删除实验数据；有未完成评估时先用其对应版本收尾，不让旧版本接管不认识的worker协议。新评估的进程隔离、容量与预算见[评估作业](stock_research_evaluations.md)，自然交易日验收仍分别记录。

同一API进程还可显式启用[专用静态工作台](stock_research_static_web.md)。本机API现已从固定release直接提供页面；采用遵守上述版本/迁移/空闲保护，不引入额外Node后台服务。

## 一次执行

1. 按原计划日期与固定股票池调用来源分支 `scripts/us_research_refresh.py`，补齐复权日线和SPY/QQQ/IWM。使用来源自己的解释器和PG配置，最长900秒，并为研究保留原期限余额。来源完成不等于质量通过，失败也不伪造行情。当来源回执明确为`failed/OperationalError`且剩余研究预算超过300秒时，最多按同一预算规则重试一次；超时、取消、校验失败和其他错误不重试，最终失败仍交给正式质量门禁裁决。
2. 用专用workflow身份调用原盘前API。API开启 `PREMARKET_REFRESH` 后只读复制行情，再走既有覆盖率、历史长度、固定快照、两策略与交叉评议；原期限及成功产物复用规则不变。
3. 自动事件等到纽约08:30冻结已知首报。未完成也发布准备/运行状态，不将缺名单解释为空机会。整个研究流程同时继续，原08:50前成功阶段增加时再发一条恢复更新。
4. 发送复用Personal的 `task_delivery.reserve/ready/deliver` 和既有 `feishu_send.py`。按目标交易日、触发类别及 `initial/recovery` 固定业务键，发送前保存正文；重放读取原正文。`sent`只有真实原生消息回执才成立，`retry/ready/unknown/unconfirmed`均不是成功。

手动事件使用真实执行时刻，完成后立即发布并明确“手动执行；不代表按时发布”。非交易日、过期和未到开始时间不启动行情或通知。通知正文包含报告编号、版本、截止日期、状态、候选原因、风险、待确认与交叉意见；完整事实保留在固定报告中。

## 身份与部署

父进程不读取来源PG、模型或Personal服务端身份。来源和通知各自用受控子进程读取既有配置，返回有限回执；子进程超时或取消须回收本次进程组。通知只允许既有 `ou_` 个人收件人，并核对来源workspace，不能从业务输入改发到群聊或他人。

API和来源代码须从固定版本安装到Documents之外，依各自锁文件建立venv；输入版本、SQLite和私有配置也放在可供后台读取的独立状态目录。不要直接把交互开发目录当24小时后台部署。来源候选必须包含行情协调锁和研究整池入口；API配置的空 `BENCHMARK_PATH` 表示消费来源云端已维护的三个基准，显式非空则另执行供应商基准补充。

单用户MVP采用独立launchd计划，每60秒运行 `scripts/stock_research_mvp_schedule.py --config /absolute/private/path/mvp.env --workspace <operator-workspace>`。tick固定纽约08:00原slot，只在原08:50前补跑，交易日由正式日历判断；同日已送达且研究终态保存done，不重复执行。单次进程50分钟内收尾；退出和重启不延长原slot期限。`--manual`使用真实当前事件时刻，单日手动完成标记独立保存。

### 通知范围与旧记录

手动和定点研究的通知正文、业务键、完成标记分别保存。同一范围内重放仍使用首次正文和原业务键，包括unknown；后来的模型结果不能覆盖已尝试发送的正文。定点沿用`us:YYYY-MM-DD:initial|recovery`和`<目标日>/<phase>/publication.json`；手动改用`us:YYYY-MM-DD:manual:initial|recovery`和`<目标日>/manual/<phase>/publication.json`。它仍是既有单日手动入口，不提供任意多次人工通知。Agent通知状态继续只统计定点键，不把手动送达当自然首报。

新冻结同时记录0600的`scope.json`，绑定schedule_id、workspace_id、trigger_kind、target_session、phase及完整Publication摘要；按日期/阶段的协作锁内先冻结正文再提交范围，外部投递在两者完成后开始。范围不符、正文缺失或旧共享路径已有正文却没有范围时，两个入口均以`mvp_publication_scope_unconfirmed`退出1，不生成done，也不修改正文或调用通知。中断只留下正文时同样停止，避免重新生成不同正文继续未知投递。

旧文件不会自动迁移或推断来源。部署前须按原执行input、固定报告、Personal业务行、正文摘要和原生引用核对；只有已证实属于该定点范围时，才能追加对应scope元数据并沿原业务键恢复。若旧手动通知占用了定点键，不能将其改标为scheduled、删除sent行或自动再发；保持原证据并单独处理修订。旧已完成记录及只读查询继续保留。本次修复的隔离测试不替代旧记录核验、真实unknown恢复或下一自然交易日验收。

这是用户优先最小MVP后的部署选择：原42条Personal计划、网关/平台配置及权限保持。通知仍直接复用其持久组件。将来迁入Personal计划面板时再完成workflows、capabilities、external、Policy、cron及同名标识六处登记，不能仅添加cron即声称已接入Personal调度。

## 验收状态

真实手动链已完成来源补拉、正式质量（覆盖100%）、量化/初选、3/4阶段的partial报告与个人飞书。首次通知配置依赖问题修正后沿原事件和冻结正文补发，CLI重放退出0；Personal仅一条业务记录，两次返回相同原生消息回执，飞书GET核对实际消息含报告编号。末项交叉评议因tool_budget_exceeded失败，未放宽预算或补造观点。

API与独立tick均已由launchd启动；原42条Personal计划保持。定点发布、恢复、冻结重放、纽约夏冬令时、非交易日/过期和真实子进程边界有专项测试。自然slot于2026-11-24上海21:00:06触发，同步超时后发生人工部署恢复，首报及恢复送达结果见下文11月24日验收，不视为全程无人干预。最新来源补拉若为incomplete，须继续读取正式质量结果，不能将来源symbols计数等同完整覆盖。

本批修复前截至2026-11-23的输入已单独核验：来源三基准各495行；SNDK缺目标日仍为incomplete。只读复制后正式质量为517/518、99.8069%，门禁通过，未降低95%覆盖或320交易日要求。完整手动报告和自然定点验收是不同检查点。
恢复发布存在时，首报与恢复消息都必须取得`sent`才写当天`done.json`；`retry/ready/unknown/unconfirmed`保留为未完成，后续自然tick在原08:50期限内沿原业务键和冻结正文重放。CLI同样返回非零，不把“首报成功、恢复未送达”整体报为成功。研究阶段已完成的产物继续复用。

## 2026-11-24稳定性验收

本批资料修复后518/518对齐，15只行业及SNDK最新日缺口已补；FDXF/HONA/Q/VYLR/SPCX共五只受上市时间限制的历史排除继续保留，不拼母公司行情，旧报告不改写。同步还有一条池外WBD旧无效行情隔离记录，不属于当前518只缺数。

自然slot在上海21:00:06触发，旧2,000行复制三次90秒超时；21:16:30人工部署10,000行候选后，保留原纽约08:00事件及08:30/08:50期限。随后自动重试复制186,479行、19批、66.77秒，冻结run 3441d925-bb23-4e3e-b00e-9762d9e81865。

首报于原21:30时刻约0.1秒内冻结，飞书原生消息21:30:01.834创建，Personal确认sent；报告ac0398c2-a098-4abd-8fbb-e465243570fb含3/4成功阶段。剩余Martin复核四次provider TIMEOUT后，原事件自动恢复于第五次完成；21:37:02.310的原生恢复消息对应b14ad808-a5ec-456e-add7-d53bef90365e，含4/4成功阶段。首报与恢复各一条业务行，正文分别匹配冻结文件，完成标记在两者sent后写入。完成后及部署后的tick未新增业务行或消息回执；不能用手动投递冒充本次原事件。

本次发生人工部署恢复，仍须下一正常交易日验证全程无人干预；传输unknown分支本次未自然出现，专项测试和真实sent读回不能替代该场景。

研究结束后，确认研究/问答/准备均无活动任务才采用cc35c3ac：相同修订且OHLCV相同不重写，保留同修订数值修复、新修订与隔离清除。Web API使用launchd ProcessType=Interactive，独立定点编排Standard；原42条Personal计划及共享服务不改。重启后20次真实HTTP全部200，含冷加载P95 1.97秒，后16次热读7～426ms；不同测试窗口与冷暖状态分开报告。

本地全量6,160通过；代码提交cc35c3ac的[完整CI八项](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45402900653)全部成功，包括完整Web和操作dry-run，远端6,159通过/一项既有安装DSH条件skip、coverage86%。最终文档提交CI另跟踪，草稿PR仍未合入，独立整体审查未完成。

模型流空闲诊断修复`ed465e40`在完整八项CI通过后，由无活动研究/问答/准备检查保护切换API及盘前调度。原请求/阶段/运行预算保持；切换后15次HTTP全部200、含冷加载P95 2.00秒，固定报告/个股/股票池/市场/参数内容摘要与切换前一致。当天发布文件和完成标记未变，首报/恢复仍各一条sent且原生消息与冻结正文匹配。此次受控切换不替代下一正常交易日的全程无人干预验收。

## 2026-11-25同进程入口采用

独立release为`workbench-09bdd3cb`，来源为精确Git归档、锁定依赖与专用研究构建。切换核对当前提交八项CI、所有workspace研究/问答/准备/评估活动0、worker真实退出；显式init-schema后再次核对。API与股票tick均切至该固定目录，原42条Personal计划及共享服务保持。旧配置除WEB_DIST_PATH外逐行相同；退回保留新增表和已存资料。

真实HTTP已核对HTML、会话签发、直接Bearer和同源评估读；没有创建正式评估或发送测试通知。切换后15次读全部成功，含冷加载P95 1.78秒，随后热读最高0.27秒，五类业务摘要与旧版一致。随后正式09入口已补518成员搜索、320根图表/锚点、历史修订与完整报告切换及评估空历史只读页面验收；写控件与Tailscale分别待验。自然11月25日窗口尚未来临，不能把本次部署当自然日闭环。

采用后launchd股票tick已使用新release且在原窗口之外正常退出0；这不算自然交易日运行。首报/恢复仍各一条sent账本和一个原生消息，发布内容/消息引用/时间逐项与切换前相同；四类活动均0。远端Python6231通过/一项既有安装DSH条件跳过、coverage86%，Web和操作dry-run成功。

新增可选MVP_STATUS_CONFIG_PATH供API只读核对既有Personal账本；沿既有PREMARKET_SOURCE_WORKSPACE绑定，独立进程只加载Personal数据库和workspace配置。此查询不调用投递组件、不改变tick或重试逻辑，也不将账本时间冒充原生创建时间，见[操作状态](stock_research_operations.md)。本批源码/门禁/采用分别记录。


## 2026-11-25操作状态采用

上海04:55，API与股票tick同时采用`operations-6fc65f46`，其[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45483827399)均completed/success；远端Python6249通过/一项既有安装DSH条件跳过、coverage86%，Web465/API221及两个既有外部Upstash条件skip、操作dry-run成功。本地完整Python6250/无skip；精确Git归档、同一锁文件与专用产物组成独立包，包内44项相关测试及真实只读通知核对通过。

本批无新表和迁移。切换前再次确认所有workspace四类活动0及工作进程退出，原自然窗口尚未开始。旧配置仅调整WEB_DIST_PATH并新增固定MVP_STATUS_CONFIG_PATH；其他逐行相同。API/调度及元数据均指向同一版本，旧09和0d包保留；42条Personal计划、共享服务和发布文件不改。

真实HTTP验证同源HTML/会话、直接Bearer、评估空历史及新操作状态：目标24日两个阶段sent，25日明确无记录，最近完整研究独立可读。15个并发分组状态请求全200，P95 0.206秒；未认证401、只读/盘前403、额外参数422；四类活动仍0。另15次固定业务读取全成功，含冷加载P95 1.679秒，后两批热读最高0.257秒，五类业务摘要保持。股票tick窗口外正常退出0，不算自然日。

切换后的Personal两条sent账本、原生消息、冻结正文/报告/时间与切换前逐项相同，没有测试发送或正式模型/评估请求。新增状态控件已在原生隔离PG/实际Python/浏览器验收，正式6fc新区域已补验刷新、跨日无记录/最近成功分离、partial到完整研究跳转及单区域保持；自然11月25日21:00/21:30/21:50、真实unknown、Tailscale及独立整体审查仍未完成。首次全套ENOSPC及其完整重跑日志保留；仅回收可重建依赖缓存和两个无锁已结束pytest临时目录，运行包/账本/验收证据均保留。

## 2026-11-25通知隔离采用

上海06:37:56，Mac mini M4上的独立API与股票tick采用`notifications-d42fa219`。该代码[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45499569161)全部completed/success，远端Python6295通过/1条件skip、coverage86%；依赖检查曾出现success和in_progress矛盾，原记录保留，定点重跑后才接受一致状态。精确归档1973个跟踪文件、相同锁文件、加载源码身份及包内128项通过；前端树与6fc完全相同，复用原5个已核验专用资产并逐文件比对。

无表迁移。切换时原自然窗口之外，所有workspace的研究/问答/准备/评估活动均0，实际worker退出；只替换两个股票launchd owner和固定资产路径。旧6fc/09/0d包、原42条Personal计划及共享服务保持。11/23手动首报、11/24定点首报/恢复的原事件、固定报告、账本正文及原生消息逐字匹配；只为两条已证实scheduled记录追加绑定原Publication摘要的scope，旧manual不改标，原正文/业务行/原生引用/时间/done均保持，没有新发送。

采用后15次业务读取全成功、冷加载包含在P95 1.767秒内，五类业务摘要及四类研究表小投影指纹一致。另15次状态读取全成功/P95 0.262秒；24日两阶段sent、25日无记录，权限401/403/403/422及四类活动0通过。新tick实际运行一次、窗口外退出0，不计自然日。

切换前从正式页面提交固定修订14的IEX策略分歧问题，真实模型一次完成、3次只读调用、20项声明与原始证据逐值复核；切换后独立API/PG/回执比对和浏览器重载仍同一问、同一答案/摘要/回执，引用控件可展开，浏览器无error。回答的数值核验不证明定性推断或策略收益正确。正式评估仍0；其他写控件、真实unknown、Tailscale及自然11/25窗口与完整计划继续分别验收。

### 2026-11-25正式历史评估补验

上述采用时点的“正式评估仍0”是历史检查点；06:57后已从同一d42静态入口创建一条真实行情评估并一次完成20截面，重载仍为原任务/结果，原研究数据保持。未换包或修改调度/通知，详见[正式页面验收](stock_research_evaluations.md#正式页面与真实行情验收)。


## 2026-11-25并发准备采用

上海09:55:30正式API与股票tick同时采用固定`preparation-633b4dd2`。[实现八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45527062177)全部completed/success，远端Python6317通过/1项既有安装DSH条件skip、coverage86%；本地原始全量6318通过/无skip。首份真实快照持锁的双API测试先红后绿，原无屏障场景另40次/80份回执直接completed；取消、原期限、不同范围及失败拥有者恢复均有原生PG证据。首次重试耗尽和另一轮partial的历史原因仍未证实，当前明确DiskFullError另保留日志，清理可重建下载缓存后完整重跑通过。

候选1978个Git文件、加载源码/锁、143个独立安装项目模块及包内139项通过；前端树和5个专用资产逐文件相同。原配置仅改变专用资产路径，两个股票owner仅改变解释器/工作目录及tick固定脚本路径。原slot之外、4类全workspace活动0、worker退出及共享会话空闲后停tick、复查、停API并等待退出；无DDL。旧d42及更早包、数据库和原Personal客户端保留，失败可恢复原plist/元数据。

新API直接/同源入口、24/25日状态、已有完整20截面评估及固定结果读取通过。15次业务读取全200，含冷加载P95 1.802秒，5类HTTP摘要及4类原研究投影相同；另15次状态读全200/P95 0.434秒，权限401/403/403/422保持。27张研究表的小投影、27份保护文件、4个Personal会话投影、共享宿主PID和三条旧通知账本/正文/原生引用/时刻均逐项相同；42计划的cron/workflow文件摘要保持。未新增研究、问题、评估或消息。新launchd tick窗口外实际退出0，四类活动仍0，API实际监听PID/cwd与新包一致。

Personal仍使用固定ece83代码和d42锁定解释器；按当前声明另启只读MCP，三项工具在新API读完2/8/2页（IEX320根），每页最多32736字节，重建原模型/摘要与HTTP一致，owned子进程已回收、原进程与live profile不变，模型调用0。这是SDK兼容核验；08:32真实模型页面验收另记，不把本次读取描述成新模型问答。

完整计划保持进行中：自然11月25日21:00/21:30/21:50及真实通知unknown仍未发生/验证，正式初选持久观测/人工审查、参数实验/AKQuant、Tailscale与独立整体审查等继续推进。收尾文档head的CI单独核对，不把实现633的绿灯移用于后续提交。

## 2026-11-25瞬时源重试采用

上海22:58，正式API与股票tick同时采用`stability-51c1ee22`。实现提交`250fe70a`的[Stock Research CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45631935944)整体成功；文档head`51c1ee22`另有[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45635384613)成功。运行包来自精确Git归档，1995个跟踪文件逐blob匹配；schema、依赖清单和锁文件与前一包逐字节相同，无DDL。锁定解释器为同一3.11版本、同一uv锁；专用研究构建生成research.html和4个assets。包内MVP、定点、盘前API与操作相关85项测试通过。

采用前全workspace研究、阶段、量化、量化尝试、准备、问答和评估的pending/running均为0，无真实worker进程；候选`.env`继承旧包并仅把`WEB_DIST_PATH`改到新包，权限0600。两个launchd plist先备份再定向替换路径。首次bootstrap在旧进程关闭竞争下返回Input/output error，核对两个服务已卸载后分别重新bootstrap；API PID45907，调度加载后窗口外tick退出0。采用后16次真实HTTP全200、P95 50.202ms，四类活动复查仍0，`release.json`指向同一`51c1ee22`和固定目录；原`selection-14d414cd`保留回退。真实瞬时源失败重试、自然交易日首报/恢复及通知去重仍须自然窗口分别验收。

## 2026-11-26 Tailscale候选固定包采用

上海00:07，正式API与股票tick同时采用`tailscale-1802719f`。该实现提交的[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45648628187)全部completed/success；远端Python6342通过、1项既有安装DSH条件skip，smoke dry-run成功。候选来自精确Git归档，1996个跟踪文件逐blob匹配；`uv.lock`和`pyproject.toml`与51包逐字节相同，无schema差异，专用前端为research.html加4个assets。包内Tailscale/静态入口聚焦24项通过，候选health/ready启动通过。

切换前研究、准备、评估、正式观测和问答的pending/running均为0，无真实worker。两个launchd plist备份在`state/plist-backups/tailscale-1802719f/`后定向替换；新API实际PID69764且cwd在新包。调度加载后窗口外tick退出0。候选`.env`权限0600，仅把`WEB_DIST_PATH`改到新包，未新增Serve键。采用后15次真实HTTP全200，P95 1532.828ms、中位数99.624ms；首读latest report冷加载1532.828ms，读回修订14、目标2026-11-24。五类资源active复查仍0。

`release.json`的API与调度指针均指向`tailscale-1802719f`；原`stability-51c1ee22`保留回退。当前`tailscale serve status`仍是`No serve config`，候选代码没有启用远端入口。真实Serve远端浏览器验收、自然交易日3/4→4/4恢复、瞬时故障重放和真实通知unknown继续未完成。

## 2026-11-26 doctor Python下限候选采用

正式运行时candidate doctor暴露一个真实缺陷：Python 3.11锁定环境本身满足`requires-python = ">=3.11"`，但`doctor`硬编码3.12，导致除该项外全部通过时仍返回`ready=false`。实现提交`beade4f0`把判定改为`MINIMUM_PYTHON=(3,11)`并补边界测试；该head的[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45662796166)全部completed/success，远端Python测试和smoke dry-run均通过。

上海01:31，正式API与股票tick采用`doctor-beade4f0`。候选来自该head精确Git归档的1996个跟踪文件；`pyproject.toml`与`uv.lock`和`tailscale-1802719f`逐字节相同，锁定解释器仍为Python 3.11.16，无schema变更。候选`.env`继承原配置并只把`WEB_DIST_PATH`改到新包，权限0600，未新增Serve键。两个plist备份在`state/plist-backups/doctor-beade4f0/`。API首次bootstrap再次出现旧进程关闭竞争的Input/output error，核对API已卸载且旧调度tick退出后分别重新bootstrap；重启前五类资源active为0。

采用后API实际PID93548、cwd在新包；15次`readyz`全200，P95 12.868ms、中位数10.682ms、最大55.495ms。candidate doctor返回`ready=true`，Python检查通过，其余依赖、日历、universe、分类、本地行情、基准补充和数据库检查全部通过。`reports/latest?target_session=2026-11-24`读回`b14ad808-a5ec-456e-add7-d53bef90365e`、revision 14、518/518且coverage 1.0；五类资源active复查仍0。新调度owner加载后窗口外手动tick退出0。`release.json`的API与调度指针指向`doctor-beade4f0`，`tailscale-1802719f`保留回退。Tailscale Serve仍是`No serve config`；远端浏览器、自然交易日3/4→4/4恢复、真实瞬时故障和真实通知unknown继续未完成。

### doctor下限接线补验

源级审查发现`supported_python`只被测试直接调用，`doctor`生产分支仍内联`(3,11)`；这不是行为错误，但会让后续下限调整出现双源漂移。提交`c0ae327b`让生产doctor改为消费该helper，并用monkeypatch监听真实doctor传入`(3,11,16)`、monkeypatch数据库失败返回非ready，先验证接线而非只测边界函数。聚焦7项、Ruff/format、函数门禁通过；该head的[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45669331940)全部成功。

上海02:10，正式API与股票tick采用`doctor-wiring-c0ae327b`。候选来自精确Git归档，依赖清单与`doctor-beade4f0`逐字节相同，Python仍3.11.16；`.env`保持Qwen和16384输出预算，仅改专用前端路径且权限0600。切换前五类active为0、无调度进程；plist备份在`state/plist-backups/doctor-wiring-c0ae327b/`，本次先等旧API退出再bootstrap，未复现I/O error。新API实际PID4161且cwd在新包；15次readyz全200、P95 12.727ms；doctor ready=true；revision14报告仍518/518且coverage 1.0；窗口外tick退出0、active复查0。当前包上50次并发混合业务读全部成功，P95 969.686ms、最大1013.704ms。真实浏览器重载报告和质量行正常、无新增page error。Tailscale Serve仍为`No serve config`。
