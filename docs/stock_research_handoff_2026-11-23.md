# 股票研究开发交接（2026-11-23）

用户决定转到 Mac mini 继续开发，此处保存开发检查点，不表示 plan.md 六个阶段已交付。下一位执行者先读根目录 AGENTS.md、plan.md 和因子出处核对，以下顶部为最新进度，下方旧检查点仅作历史记录。

## 当前检查点：固定报告市场/分组/比较/规则查询

输入准备回执已提交5952772d。新增固定report_id的GET market/groups/compare/rules与最小只读客户端；市场沿原Wyckoff事实及三基准，分组先验证原产物再绑定JSON由PG筛选/NULL最后排序/分页，比较2～8股共享一次冻结上下文，规则读原参数、出处和有效配置。股票查询复用同一上下文投影，旧修订意见边界不变。新增research_mcp入口复用Personal原三项并增加五项，共八项只读；query_mcp原入口仍仅三项，尚未注册进聊天模型循环或Personal部署。

最终冻结源码后全量Python5983通过（无skip、50旧警告），报告/查询/HTTP/源码12通过；完整Web gate为frozen-lockfile、TS strict、Web386、API221/默认两项外部Upstash跳过、Web/Pages构建。Ruff/格式、质量、红线、hygiene和daily dry-run通过，42份JS不含两种研究身份。Python LOC138490/+60.7%、TS35236/+89.5%旧基线警告保留。较早两次全量因测试期间编辑注释触发源码守卫而失败，不能计作通过；之后冻结源码重新全量通过，没有排除测试。全量开始后不要再编辑研究Python（注释也在指纹内）。

真实旧报告cf8da076修订3八工具逐项调用通过：126组SQL分页、Martin量化池30/Wyckoff0、两股同快照比较和三基准5根；另8股各60根读取通过，JSON371393字节。Node代理读200/跨站403，原报告ID/修订/摘要不变。主API确认0活动任务后重启为本轮源码，旧源码ready503且读报告200；新查询实际200。无新模型、正式通知、参数发布或Personal仓库/secrets修改。

接续完整股票池枚举（当前市场概况只列量化池；按代码的stock工具可查整池）、研究聊天只读模型循环/持久会话、主题/自选维护、锚点叠加、行情更新协调与局部量化降级、Personal唯一调度通知和正式部署。真实浏览器仍待已有账号正常登录，控件未验收；wiki checkout不存在，远端CI及整体独立审查未验收。

## 当前检查点：未建run的输入准备回执

参数界面已提交d0f1f6ce。新增preparation_attempts（显式init-schema后21表），HTTP新建和CLI默认/指定快照入口在参数/快照/量化/冻结前后保存独立尝试；ready关联真实run且只表示输入已固定，失败/取消保存固定错误码，硬中断保留unconfirmed。GET /v1/preparations按目标日过滤和SQL分页，既有Agent运行操作下方展示质量/未知状态与对应run，不清空旧报告或名单。CLI默认分支不再在记录外提前生成快照；仍消费原run_preview，由冻结回调在模型前确认。

最终完整Python5981通过（无skip、50旧警告），准备/流水线/参数专项17通过；完整Web gate为frozen-lockfile、TS strict、Web386、API221/默认两项外部Upstash跳过、Web/Pages-functions构建。Ruff/格式、全范围质量、红线、hygiene和daily dry-run通过，42份JS无两种研究身份。Python LOC138073/+60.2%、TS35231/+89.5%旧基线警告保留，增长对应本轮准备账本/API/CLI与界面。

真实隔离workspace临时用100%覆盖要求，原快照501/518、96.72%对齐按预期未通过；默认CLI、缺失快照CLI及两次API共4失败均独立读回，run总数0。stocks覆盖要求/参数未改，原cf8da076报告ID/修订3/摘要及名单保持。主API旧源码ready503且报告可读，确认0活动任务后重启ready200；原生PG21表、stocks准备数0，不补造历史。未启动新模型或发送通知。

实际浏览器仍待现有账号认证，控件未验收；wiki checkout不存在、远端CI和正式部署未验收。接续：完整分组/整池/比较/规则工具与研究聊天注册、主题/自选维护、锚点叠加、行情同步协调与单策略量化局部降级、Personal唯一调度及通知。无Personal仓库/secrets修改。

## 当前检查点：Agent参数维护界面

后台参数版本已提交d4327e40。现有Agent研究视图新增当前配置/报告冻结参数、完整JSON草稿、登记版本SQL分页及显式发布。载入不发布，刷新/折叠保留草稿；发布按当前revision，成功/失败后先显式读回，409不自动换修订重试。代理新增固定参数GET/登记POST/两策略发布POST，无新增页面或前端研究凭据。

完整Web gate通过：frozen-lockfile、TS strict、Web383、API221/外部Upstash默认两项跳过、Web及Pages-functions构建；Ruff/1211文件格式、函数门禁及daily dry-run通过。真实前端客户端经独立Node代理在隔离workspace登记/发布并由只读身份独立读回，旧修订409，跨站与只读写入403。主workspace参数仍operator基线/修订0，原cf8da076报告ID/修订3/摘要及冻结参数不变；未启动模型或发送通知。浏览器仍待既有账号认证，实际控件未验收；独立wiki不存在、远端CI及正式部署未验收，主题/自选、查询聊天与Personal继续开发。

## 当前检查点：参数注册、CAS发布与实际消费

MCP启动脱敏已提交c8057081。新增parameter_profiles/strategy_parameter_activations维护表，显式init-schema后共20表。参数名绑定不可变内容、发布追加且expected_revision冲突409；只读身份禁止写。API/CLI新研究统一解析发布配置或无发布时operator基线，私有物化并冻结完整来源；基线文件随后修改不影响发布值/旧运行。Stock观察同时返回完整原参数、Martin因子定义和Wyckoff原有效配置。

研究/质量183通过，全量Python5974通过（无skip、50旧警告）；最终一次CAS完整性/函数拆分后8项参数回归通过；Ruff/1211文件格式、全范围质量与红线扫描通过，daily dry-run成功。Python LOC137805/+59.9%、TS34945/+87.9%旧基线非阻断警告保留。真实隔离workspace按原基线创建201/发布200/过期409/独立读200，数值未改变；stocks主workspace发布数仍0，无模型/通知。主API可读原f77a7db4冻结参数且与Stock观察相同。参数维护界面/Node写代理尚未接线，未作UI或生产部署验收。

## 当前检查点：只读MCP启动错误脱敏

源码守卫已提交931fbe6c。用合成的尾随空格token复现MCP启动校验会把原输入写入stderr；现已只返回固定脱敏消息并退出1，原值和traceback不输出，正常stdio工具不变。真实子进程回归及研究/质量175项通过；上一完整Python5965项通过。本次Python变更也实测触发主API版本守卫（ready503、原报告200），零运行后受控重启恢复ready200。未泄露实际凭据、未启动模型或发送通知；浏览器/远端CI/wiki/正式部署仍待验收。

## 当前检查点：加载版本守卫与源码缓存失效

个股视图已提交c87e7bd1。新守卫固定进程加载时的递归研究源码及生产engine_hash，包含嵌套schemas；改Python后readyz/新建/续跑明确503，已有报告和取消/关闭仍可用。冻结前后校验，运行保存加载指纹。Martin/Wyckoff量化缓存加入相同源码键，旧Martin缓存不再在新代码下直接复用。详见[源码保护](stock_research_source_guard.md)。

聚焦研究及质量174项通过，全量Python5965通过、无skip、50条旧警告；Ruff/1204文件格式、全范围质量门禁、红线、workflow hygiene与daily dry-run通过。Python LOC137303/+59.3%、TS34945/+87.9%旧基线非阻断警告保留。隔离真实API进程修改嵌套schema后ready/create503、read200/运行0，重启ready200；主API受控重启并读取原完整报告通过。未改实际源码来注入漂移，未启动模型/通知。独立wiki、远端CI、浏览器登录后的实际操作与整体独立审查仍未验收；配置维护、研究聊天、Personal及数据阶段失败回执继续接续。

## 当前检查点：固定报告个股事实与纯行情图

名单Agent视图已提交57793adc。新增个股面板，整池代码查询、两策略因子/排除、原锚点标量、行业/主题、版本引用及证据讨论；复用K线priceOnly模式，仅显示快照OHLCV，不绘制通用图表的自动结构、均线或标记。HTTP/CLI/MCP新增可选report_id并校验同run/workspace，修订早期查询不再带入后来初选/互评。

真实前端请求经Node代理读回同一固定报告320根日线及两策略事实，CLI/MCP同report_id重读通过。全量Python5962、聚焦171，完整Web gate通过：Web379、API221/两项外部Upstash默认跳过、TS strict、frozen-lockfile、Web与Pages-functions构建；Ruff/1200文件格式、全范围质量、红线、daily dry-run通过，42份JS中无两种研究令牌。Python LOC137255/旧基线+59.2%，TS34945/+87.9%为非阻断审查信号。

浏览器仍在正常登录页，画布/切股/分页/讨论操作待用户认证后验收。待续：运行服务在开发中源码修改后的版本守卫、锚点叠加、研究聊天工具注册、参数/主题/自选维护、未建run的数据失败回执、Personal注册/通知与部署。未改Personal仓库或secrets，未运行新模型或发送通知。wiki checkout/远端CI/独立整体审查未验收。

## 当前检查点：持续名单接入Agent

观察名单后台已提交57267e9b。现有研究视图已显示当前名单/变化事件、策略筛选和SQL分页，明确与固定历史报告分开；最近检查与有效状态日期、数据不足/未复核分别展示。真实Node代理与前端契约解码读回5项，跨站403、名单POST404，两种研究令牌不在42份JS产物中。

完整Web gate再次通过：frozen-lockfile、TS strict、Web375、API221（两项外部Upstash按默认条件跳过）、Web及Pages-functions构建。没有新增路由或改锁文件。实际浏览器仍在现有登录页，用户交互验收未完成，未绕过认证。后续继续个股图表/事实入口、研究聊天工具注册、参数/主题/自选维护、未建run的数据失败回执与Personal接入。

## 当前检查点：持续观察名单终态事务

外部查询工具已提交662644d0。新增watchlist_items/events/days三表，显式init-schema后本机共18张表。终态流水线在同一事务保存观察状态/事件、运行终态及报告修订；缺数据/模型结论保留最后有效状态及来源，first-success日来源、去重、旧日期保护和force对照规则见[持续名单](stock_research_watchlists.md)。显式track-run已将真实f77a7db4接入，独立进程及只读HTTP读回5条new/5事件，重复事件仍5，原报告ID/revision/hash不变。

研究及质量聚焦171项通过，全量Python5962通过、无skip、50条旧警告；Ruff/1198文件格式、全范围质量门禁、红线扫描和daily dry-run通过，Python LOC137240、旧基线增长59.2%为非阻断审查信号；名单面板、全局数据失败尚未创建run时的回执、参数/主题/自选维护及Personal部署继续开发。未发送通知或重跑模型；浏览器登录待用户完成，wiki/远端CI/正式部署均未验收。

## 当前检查点：固定股票事实与独立只读工具

新增按run限定的股票观察HTTP查询，直接读取冻结快照及两策略产物并核对绑定摘要，保留原因子ref、排除、行业/主题、截止日日线、初选和互评。新增独立只读身份，全部非GET方法403；外部查询进程只持有专用0600查询配置，不读根.env/PG/模型身份。实际MCP仅注册最新报告、股票观察、运行状态三项只读工具，已逐项真实HTTP调用完整运行f77a7db4，独立CLI重读一致。详见[只读查询](stock_research_queries.md)。

现有Agent面板检查点已提交5e488411；本批工具尚未注册进现有Agent模型循环或Personal部署，未改Personal仓库、secrets、计划或通知。研究/API及质量聚焦167项通过，全量Python5958通过、无skip、50条旧警告；最终Ruff/1190文件格式、质量、hygiene、红线扫描及daily dry-run通过。Python LOC136776、相对旧基线增长58.7%保留为非阻断审查信号。浏览器仍待现有账号认证；持续名单、参数/主题/自选维护及部署继续开发。

## 当前检查点：完整真实互评与Agent研究面板

以8f626c72冻结的真实运行 `f77a7db4-b830-4310-9463-2e97565b9c17` 已completed：Martin五只初选、Wyckoff空初选，Martin对空目标评议及Wyckoff对五只评议四阶段均成功，保留late=true。实际证据工具分别调用9次和18次。独立CLI与HTTP重读的初选/评议ID相同，逐份原始输出经冻结上下文重新验证，回执及源码摘要与该提交一致。父运行67c23a49的失败历史原样保留。此为执行与引用验证，不是收益、BUY或准时交付证据。

现有/chat读盘室增加策略研究子视图，前后端接线已实现：公司名称取同一快照、历史运行与固定报告修订、初选/互评分栏、数据日期/质量/迟到、阶段错误、预览/force重分析/失败续跑/取消，以及将固定报告带入既有对话。同源Vite代理仅显式启用且loopback绑定，独立研究身份只在Node侧使用，真实HTTP读回完整报告、跨站403及产物不含研究令牌已验证。专用聊天工具、持续名单、参数/主题/自选维护和正式部署仍未完成。使用说明见[Agent研究面板](stock_research_agent_panel.md)。

本批全量Python5955通过，无skip、50条旧警告；Web372通过，API221通过且两项外部Upstash集成按默认条件跳过；TS strict、frozen-lockfile安装、Web及Pages-functions构建通过。函数门禁新增server目录70行范围，无新豁免；CI研究测试补装research extra及原生PostgreSQL测试工具。本机pnpm包装器曾误选x64可选依赖，改由原生Node运行锁定pnpm10.33.2完成非交互安装，锁文件未变化。浏览器仍在现有登录页等待用户完成认证，未绕过认证，不能声称真实页面操作验收。无远端CI/正式通知/自然交易日部署；wiki独立checkout缺失，未同步。

## 模型引用契约与质量门禁修正

真实互评失败复核发现ACGL/MRSH价格数值完全正确，拒收原因为claim未在纯文字字段重复列同一ref。v2移除重复要求，保留各观点有效引用、每股自身claim、范围及逐值一致；错误数值返回具体ref和固定值。互评限制列表在prompt schema按本次注册内容枚举，缺本策略计算须insufficient_evidence；修正显示剩余预算，SDK监督采用本阶段实际预算，耗尽独立记tool_budget_exceeded。旧失败记录不追认，需force新运行。API错误报文同时进入OpenAPI。当时真实新版完整互评尚待验证；最新结果见顶部检查点。

另发现quality_gate的PY_DIRS原先未纳入stock_research，已补扫描全包及CLI层100行规则，其他函数70行；拆分DSH插件配置和协议错误分类后新范围检查通过，无新豁免。新增临时研究包长函数的CI入口回归，证明会被拦截。先前门禁通过的声明仅对应旧扫描范围。研究测试158项及质量门禁5项通过；最终全量Python5954通过、无skip、50条旧警告。Ruff、1177文件格式、新范围quality_gate、hygiene、红线扫描和假值daily dry-run通过；研究包纳入后Python LOC为136309，旧基线增长58.1%属非阻断审查提示，67条依赖警告保留。当时新版真实完整互评仍待验证，后续结果见顶部；无远端CI或部署声明。

## 研究API与运行管理续接

已接通仅loopback的 `serve`、独立Bearer身份、Host/Origin检查、显式响应、SQL分页、固定报告修订、SSE游标、HTTP启动/重试/取消及有限后台任务。新增api_requests持久化幂等键摘要，冻结与绑定中断可恢复；启动只恢复API创建的未结束任务，CLI运行独立。执行版本变化要求force新建，报告明确late。独立API身份已写入本机0600的.env，未暴露凭据。详见[API预览](stock_research_api.md)。

真实HTTP已验证健康200、缺身份401、认证读取200、创建202及迟到标记。实际运行 `67c23a49-8c02-4e62-91bc-cfb2cb74f4be` 已完成Martin五只初选（实际五次证据工具查询，含一次结构修正）和Wyckoff空初选。对这五只的Wyckoff互评第一次因自写数据限制触发结构修正后耗尽查询预算；通过HTTP retry续跑仅重做该互评，保留两份初选ID及已成功阶段，第二次仍因输出契约失败。当前partial，不能声称真实非空互评已验收。原文、事件及失败回执仍在忽略目录。已核验冻结source_hash与本批暂存Python内容完全一致。

全量Python5949通过、无skip、50条旧警告；随后新增来源不可用/非交易日测试，研究套件155通过。真实隔离PG与ASGI覆盖401/403/404/409/422/503、范围隔离、固定历史修订、幂等重放/冲突、强制父版本、取消收尾和SSE断点。Ruff、1177文件格式、函数质量、工作流/依赖检查、锁文件、红线扫描和假值daily dry-run通过；既有LOC与67条依赖警告保留。未改前端，未运行Web门禁、远端CI、wiki同步或部署。

接续：改善模型引用错误的精确反馈与限制字段schema，然后补个股/分组工具、持续名单、参数/主题/自选维护及现有Agent界面。services、Tailscale与自然交易日、效果记录仍在后续阶段。PDP技能已读取，现有Agent单页扩展符合单页豁免，无新增页面路由。

## 执行器与研究账本续接

已接通 `run --preview` 与独立 `read-run`，固定DSH 0.2.1-alpha.2 SDK接口。每次尝试建立独立home、Agent目录、会话和工作目录；实际注册目录精确限制为两个只读MCP证据工具，数值/范围/上下文摘要机械校验。新增九张执行账本表，本机显式补建成功；初选事务提交后互评，成功结果不可覆盖，失败阶段可续跑，运行锁和取消回收已测试。配置已指向本机已有SDK静态模型连接，未复制Personal网关凭据或生产会话。详见[执行预览](stock_research_executor.md)。

真实CLI预览运行 `c642eaad-f9cd-4b99-84c0-2f6e9de2f8cc` 为partial：Wyckoff量化池为空并提交空初选，Martin两次模型请求均遇到供应商流式空闲TIMEOUT，未生成正式初选；Martin对空目标互评完成。独立CLI重连PostgreSQL读回完全一致。此前模型探测曾真实调用证据工具但输出截断，GLM路线另遇配额上限；均未作为正式初选。本批不能声称真实非空初选和交叉评议验收已通过。

研究测试149项通过；全量Python5944通过、无skip、50条既有警告。真实MCP stdio和本机DSH离线握手纳入测试，外部模型调用独立于自动测试。修复session级临时库fixture继承其他套件加载的.env问题，以及旧存储测试对全表计数缺少workspace范围的问题。Ruff、1162文件格式、quality_gate、hygiene、uv lock检查、红线扫描及假值daily dry-run通过；旧LOC和67条依赖警告保留。没有Web改动或远端CI、独立整体审查、wiki同步、服务部署和通知发送。

当前按plan.md继续阶段四：带认证的HTTP查询、固定报告修订与运行管理，然后接持续名单和现有Agent界面；阶段五services与Mac mini自然交易日验收、阶段六后续表现仍未完成。

## Wyckoff生产路径适配续接

已接通 `analyze --market us --as-of 2026-11-20 --strategy wyckoff --no-llm`，支持原快照重放。直接复用美股生产_run_layers、_candidate_rows及水温限制，不重新实现信号。新增可选层级诊断对原生产返回指标无影响；冻结实际FunnelConfig、覆盖、引擎源码及NumPy/pandas版本。独立参数为 `config/research/wyckoff-us-v1.json`，默认路径已配置，无需新凭证。操作见[Wyckoff CLI](stock_research_wyckoff_cli.md)。

真实固定快照490只参与计算，L1=490、L2=305、L3=47、L4命中=0，SPY水温CRASH，最终空池。上游预点火和趋势观察独立保存，不补名额。快照缺可核验金额，上游辅助评分仍使用close×volume代理；研究禁止非零金额硬门槛，actionable等仅为上游原始诊断，没有执行许可或模型结论。

下一步为dsh执行器、受控只读证据工具、独立初选事务和交叉评议。本机可执行文件已在 `~/tools/e2e/bin/dsh` 找到并实测版本0.2.1-alpha.2；PATH中没有dsh不代表未安装，不能沿用另一机器0.2.2-rc.1接口假设。已读本机headless文档；未运行模型、未复用生产profile会话或修改Personal服务。候选/因子数据库表、API及运行恢复仍待接入，wiki checkout不存在。

本批最终全量Python为5897通过、23跳过（未安装可选mcp extra）、50条既有警告；研究测试125项无skip，相关回归332项通过。显式污染环境下研究/回测130项、工作流套件1015项通过。补修既有ArmStat在缺t值时应先否决低于成本差值的问题；研究及工作流测试隔离各自环境键，保留生产配置读取和基准冲突检查。Ruff、1146文件格式、quality_gate、工作流检查和假值daily dry-run通过；67条依赖与旧LOC基线非阻断警告保留。真实报告已独立重读，与生产入口逐字段一致且缓存复用。未跑Web、远端CI、独立整体审查或部署，不声称可合入/发布。

## Martin量化观察续接（上一批）

已接通 `analyze --market us --as-of 2026-11-20 --strategy martin --no-llm` 和固定快照重放。共享EMA/ATR/ADR、收益、相对强弱、量幅收缩、确认锚点、成熟/早期回调、首/重复回测、行业覆盖、分项得分和分支合并均保存到不可变报告。配置正本为 `config/research/baseline-v1.json`，本机参数路径已指向它。操作和算法边界见[Martin CLI](stock_research_martin_cli.md)。

真实快照扫描491只合格股票，生成30只量化复核候选；没有模型初选或BUY。来源旧模块的实际输出已生成黄金夹具；相同快照157070行ATR/ADR与AKQuant对拍，位置、预热缺失一致且最大绝对误差为0。EMA未经AKQuant对齐，生产继续使用pandas。耗时和峰值内存仅是一次局部工程测量，不证明完整策略性能或收益。

下一步按阶段三接Wyckoff生产路径、dsh模型执行、独立初选与互评。当前无模型调用、研究API、任务注册、通知发送或运行恢复状态机；量化产物尚未写入候选/因子数据库表。阶段一官方时点数据和上游更新协调缺口仍保留。本机没有独立wiki checkout，未声称同步或独立整体审查完成。

本批109项研究测试通过、无skip；Ruff、1141文件格式、quality_gate、工作流检查、红线扫描及假值daily dry-run通过。LOC旧基线及67条依赖非阻断警告保留；未跑全量pytest、Web检查或远端CI，不能声称可合入或发布。

## 公开版本和基准补充检查点

已新增 `refresh-versions --allow-current-universe` 和 `fetch-benchmarks`，明确保存聚合源提交与时间，不再把来源分支的静态JSON当作当前更新。默认两个源文件的修订时间为2026-11-05/2026-10-25，不是指数提供方认证，不能证明之后没有调整。实际518股并集、503条GICS分类，主题不自动推断，人工自选和已有主题会保留。原文和全部摘要保存在忽略的运行目录。

已将yfinance 1.7.0加入research extra并同步uv.lock，取得三基准各320根，共960根；SPY同日期OHLC最大相对偏差约1.93e-7，成交量检查通过。补充文件独立保留SDK表、元数据及来源，不写入云端或冒充同步来源；显式配置 `STOCK_RESEARCH_BENCHMARK_PATH` 后使用。云端QQQ/IWM仍缺失，研究通过带出处的单独基准补充解决此缺口。

真实2026-11-20快照已通过配置的数据门槛并提交研究库：501/518对齐，覆盖96.72%，历史可用Martin491只、Wyckoff490只，28项质量问题仍保留。生效 `.env` 指向本批不可变成分、分类和补充文件，doctor的配置检查ready。仅表示数据链路就绪，不代表严格历史时点、收益验证或双策略已运行。来源说明见[公开数据与实测](stock_research_public_data.md)。

本批研究测试87项通过、无skip；Ruff、1130个Python文件格式、函数长度、工作流检查和CI同款假值dry-run通过。quality_gate仍有旧LOC基线警告，依赖检查67条非阻断警告。未跑全量pytest、前端、远端CI及独立整体审查；本机没有独立wiki checkout。

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
