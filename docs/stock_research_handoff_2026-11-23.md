# 股票研究开发交接（2026-11-23）

## 续接检查点：来源维护与研究复制协调候选（2026-11-24）

研究复制新增数据库范围US任务/数据事务锁，仍只读/可重复读，忙碌失败不推进本机水位。独立来源工作树`../Wyckoff-Analysis-market-source`从实际wxjer@b5cb2d79建立，候选提交08c3845097177cd477cc9a2454c6389dae392eec，分支codex/stock-research-market-source。US旧CLI持整项任务锁，US键upsert/裁剪及可能删除US数据的清理持共享数据锁；CN/HK单市场写入/复制不取US锁。旧dc5fec0e来源工作树及.env不动，新工作树未复制根.env。详见[协调协议/采用边界](stock_research_market_coordination.md)。

源候选复用既有正式批量前复权下载/落库、固定目标日AAPL/SPY就绪检查，为显式研究池及SPY/QQQ/IWM独立键补齐；多票缺ticker标签拒收、按原窗口裁剪，缺目标日/部分失败返回不完整。既有手动refresh成对接受as_of/symbols，US写Actions候选共用非取消并发组；bootstrap/成分sync统一PG与冻结uv安装并去除旧凭据片段打印，无新增cron。研究worker没有写身份、没有自动派发源补拉。源候选未生产采用，原wxjer/SQL/PostgREST不会自动取得锁；不能把本机接线称云端旧写入已经受控。

冻结研究源码655af6d14232037aed173f18500113feb745bb19c85c07a1a1dd5582ec223f76：全量6129passed/无skip/50旧警告，协调与原同步12passed。源候选完整tests/950passed/94条件skip/37旧警告（4真实旧行情、7真实模型、83文档型指标），修复旧绝对路径以实际执行规则用例；旧OpenD手工联网脚本逐字移到scripts，不把它当单测通过。两端daily dry-run、研究完整静态/质量/hygiene通过；源端6个workflow hygiene、新模块Ruff/format通过。源端窄输入红线的3项目录误报已核实真实目录分别31/60/47个py文件；裸dict实际问题改为TypedDict，无新豁免。

实际跨工作树源CLI→原生PG→研究复制→正式快照1passed/2.96秒：源任务持锁时复制被拒绝、本机水位仍null，源端成功后1280行/1合成股票及3基准复制、质量通过；源端最后回执quality_checked=false与正式质量区分。只替换供应商I/O边界，完整源写入/PG提交/研究读取均实际执行；退出0且源进程组消失，0云写入/主研究写入/模型/通知。工程材料只在忽略目录market-coordination-*。

仅自有研究8788保护重启：旧86866/session77198终态143、PID消失/端口释放，新13914/session9507，ready/旧固定报告及问答、跨workspace404/只读私有问答403通过；26表摘要保持、活动数0。根.env及生产更新/workflow身份关闭不变，未重启共享Personal/DSH或改变42计划。

源候选与研究评审head分别运行远端门禁，当前本地证据不代替真实head/run终态。独立Wiki同批本地说明协调及采用边界，未推送上游。后续继续源候选采用/必要时自动补拉与unknown回查、原报告期限发布、通知业务键/unknown回查、唯一workflow六处清单/cron所有权、正式Mac/TCC/网络/多用户与自然交易日；正常浏览器、PIT/效果证据和另session整体审查保留。整份plan继续，不宣称完成或已正式上线。

## 续接检查点：研究分支CI与评审入口（2026-11-24）

有界行情准备已提交77eb2a4b。当前fork的main与本地开发历史相差810提交，续接沿本地codex/stock-research，发布评审head使用codex/stock-research-runtime、base保留原codex/stock-research，分别核对本次续接与上游合入范围。新增Stock Research CI在开发分支push及以它为base的PR中复用同提交ci.yml，full_web=true，不传生产secrets，不部署；PR-policy仅PR事件适用。具体配置与真实head/run核对方法见[研究CI](stock_research_ci.md)，源码检查仍以ci.yml为准。

CI入口变更本地workflow/dependency检查4项、CI政策16项与完整hygiene/静态门禁通过，数据功能仍为6125全量/35专项及Web412/API221+2原条件跳过的冻结源码。分支检查、PR策略、正式部署及整体审查分别验收，运行结果以实际Actions终态为准；不因发布评审分支或本地绿灯宣称可合入。独立Wiki本地分支codex/stock-research单独提交，不推送上游。

## 续接检查点：有界行情更新与Wiki定位恢复（2026-11-24）

在6476e9bc上接通operator显式启用的盘前行情准备：PREMARKET_REFRESH默认false，独立worker只读PG增量同步，再刷新原已明确启用的三基准补充，正式快照质量通过后才冻结run。新增refresh-market CLI，不调用模型或写研究库；更新完成与quality_checked分别表达。更新预算受原事件/120秒准备总预算限制，子预算默认90、允许1～110秒。匿名控制管道授予只读源连接，argv/文件/回执不含凭据，清空继承身份并使用临时HOME；新基准路径只供本次准备，不改根.env或旧研究。

PREPARATION_ATTEMPTS的JSON新增market_refresh，旧记录为null；更新未确认先落库，同步已确认立即保存，基准失败/超时/取消保留已知同步，不补零或伪成功。父端确认worker退出并校验产物后才发布成功，真实SIGKILL父进程验证控制管道关闭使worker和进程组消失。直接父端是API/显式更新CLI，外层workflow的未知HTTP受理仍按admission_unconfirmed处理。Agent已有输入准备区域消费同一契约，显示更新状态/已确认同步/拒收隔离和错误，仍分别显示快照质量与研究结果，无新页面。量化共用processes.py隔离/回收边界，原协议/缓存/预算保持。没有新表、迁移或依赖。

最终源码794459d5de1d3319880b9361b15e2c02117468b68be4083fa4bde501afdf4f11，全量Python6125通过、无skip、50旧警告；专项35通过，含坏配置/进程绑定/超限报文、真实CLI两种退出、原生PG/API落库、供应商失败、超时、取消及父端SIGKILL。Web412、API221及2项原Upstash条件跳过，锁定安装/TS strict/Web构建/Pages函数构建通过；旧chunk提示保留。Ruff/1292文件格式、114文件红线零命中、完整质量与workflow/dependency hygiene、合成daily dry-run通过。Python143498/+66.5%、TS36179/+94.6%旧LOC基线警告保留，无豁免、排除或锁文件变化。

最终原生隔离PG/HMAC网关→fixed argv→真实HTTP→更新worker→快照/实际量化→固定报告联调1通过/8.48秒，两个manual事件八份回执；首次同步1280行、别名增量0行，共用run26d8958e-1f05-4f16-bcbb-4baa564c8748/report90324696-e1ef-4d3b-b2e5-48269a7ccdc6及origin。独立SQL确认两请求/一run、量化2job/2attempt、初选2/评议2。明确合成空池、manual/late、目标23日/行情20日、0模型和通知；临时API85615/59245已回收/释放，临时services测试按字节核对后移除。

授权云端仅SELECT→隔离本机库及真实公共供应商实测58.71秒完成：195132行、520只股票、保留1项隔离，三基准摘要38383fc6ab006f5ebbb1d36f87b3eedc39165de3d9072bb2d454595bec7b5c30。2026-11-20离线快照501/518对齐、96.72%、quality通过，persisted=false，无新研究/模型/通知。独立读隔离库确认股票最新日20日、当前要求23日为0只；不是当前行情就绪。实际worker84904终态0且进程组不存在，父端故意污染的PG/API/Personal环境未被继承。私有证据仅在忽略目录.stock-research/bootstrap/premarket-refresh-*。

零活动/26表摘要守卫后仅重启本线程8788 API：旧78111终态143、PID消失及端口释放；当前86866/session77198，日志premarket-refresh-main-api.log。ready、旧cf8da076报告/a45bf9c4问答、跨workspace404/只读私人问答403独立读回通过；26表仍按stocks过滤有workspace表、其余全表，前后保持。新更新开关和workflow身份均关闭，根.env未改；共享8802/网关/原42计划不改不重启，旧数据文件和原默认基准不覆盖。

此前只检查当前fork的Wiki不完整。README指向YoungCan-Wang/WyckoffTradingAgent Wiki，该地址可读，已克隆为忽略的独立wiki_repo_new/，基线61b9fed；新增开发分支设计页并同步Home/Sidebar及架构/运维导航，分支配置/表契约仍由docs维护。Wiki独立提交、不向上游推送；旧检查点的“本机Wiki缺失”按本条更新。当前fork Wiki的HTTPS/既有SSH探测均Repository not found，不据此断言上游Wiki不存在。

下一步必须处理源端更新与旧维护/Actions写入的统一互斥，不能仅把本批只读复制当作补拉。随后接报告期限发布、通知稳定业务键/unknown回查、唯一stock-research-premarket六处清单及cron所有权。正式Mac/TCC、网络/多用户、自然交易日、正常浏览器、PIT/效果评估、远端CI及另session整体审查仍待完成；整个plan.md目标保持。对应研究、Wiki与Personal契约分别交付，运行面未正式启用。

## 续接检查点：跨事件研究去重与首次绑定恢复（2026-11-24）

上一批研究3cf0a69f/services文档0db649c已本机提交并快进，本批继续对齐plan第9节。同日、研究workspace、来源能力/trigger_kind和配置版本的新盘前事件保留各自job/plan/准备回执，复用首次run、报告与期限。事件ID/job/时刻不再使研究重复创建；回执新增必填可空origin，明确首次job、事件及绝对期限，未冻结为null。别名恢复/取消统一领取首次意图锁、共享取消标记；新manual不能刷新预算或复活取消研究。不同配置或trigger_kind仍分开。强制重分析仍仅operator显式force+parent_run_id，已实测新run父链及请求重放、原初选/报告保持；workflow身份未扩权。

首次冻结清单及请求映射均保持；别名已提交ready但绑定前退出时，请求账本从最早已校验ready准备回执找回run，后来配置变化也不重新准备。没有新表、迁移、配置或依赖。专项首次将新事件接收时间设在事件之前，被日历正确标before_start；修正合成夹具时刻，未改生产日历或排除用例。双API真实同日快照锁竞争可先留下input_unavailable准备回执，重放后仍只有一个run与两次量化尝试，失败历史保留。

最终源码8983b80dbc27413d0224d586e952c54aa37fa5b4c78abb5a8e2b3edc944b7f0d，全量Python6112通过/无skip/50旧警告，专项83通过。真实隔离原生PG/HMAC网关连续两个manual事件→固定argv→HTTP联调1通过/6.68秒，八份产物回执；两job e1a6e0de-2f50-4a7b-b68f-3e77093ec77b与5914905e-4036-4254-9e81-7038f01edb72，共用run ef291309-3940-4f1b-8dfc-d5d53ef48499、report0234e056-27ef-4c97-adb6-f26c3799baab与origin。独立SQL确认API请求2、run1、量化job2/attempt2、初选2/评议2；合成空池、manual/late、目标23日/行情20日、真实模型和通知0。临时API76616/50132已回收/释放，services证明用例按字节核对后移除；私有材料保留忽略目录premarket-identity-*。

Ruff/1285文件格式、完整质量、109文件红线零命中、workflow/dependency hygiene及合成daily dry-run通过。Python LOC143076/+66.0%、TS36116/+94.2%旧基线警告保留。额外全研究测试目录扫描的test_chat.py工具名拼接被SQL-02误报，已用HEAD字节相等确认是既有测试，未改无关文件或加豁免；不称全目录零命中。本批无Web/services生产源码变更，沿用既有完整Web409/API221及两项默认Upstash跳过、services500通过及3原Docker条件skip；无新测试排除。远端CI未执行。

零活动和26表摘要守卫后，仅重启本线程8788 API：旧67720终态143、PID消失/端口释放；当前78111/session2410，日志premarket-identity-main-api.log。ready、旧cf8da076报告/a45bf9c4问答、跨workspace404/只读私人问答403独立读回通过；26表审计仍按stocks过滤有workspace表、其余全表，前后保持。生产workflow身份关闭；没有主工作区新研究/问答或通知，Personal共享8802/网关/原42计划未改未重启。

下一步接必要行情更新及旧写入入口协调，再接原期限报告发布、通知稳定业务键/unknown回查和唯一stock-research-premarket六处清单/cron所有权。正式Mac运行包/TCC、Tailscale/多用户、自然交易日、正常浏览器、PIT/效果评估、独立wiki缺失、远端CI及另session整体审查继续保留；整个plan.md仍在推进，不宣称正式部署或全计划完成。

## 续接检查点：盘前有限API与实际执行（2026-11-24）

在f3a422dc计划入口上接通--execute：专用workflow身份只允许盘前任务受理、读回、局部重试与取消，固定来源工作区；与operator/read身份不同，默认关闭。固定argv只读取独立workflow.env或显式--identity-file，不继承PG、模型、Personal或operator身份。API_REQUESTS持久化原事件意图、冻结run与取消标记，准备失败保留；并发及进程重建重放复用原事件，冻结后绑定前中断可找回原run。没有新表或迁移。

事件原截止时间贯穿准备、量化及模型阶段，恢复不延期；客户端使用60/180秒退避，读回失败只查原任务，未知受理不猜测新任务。取消在拥有者确认前不冒称成功。真实worker超时测试确认PID与进程组回收。UTC模型上下文序列化统一为Pydantic规范，修复原期限进入上下文后+00:00/Z差异导致的摘要校验失败；最早日历边界和不一致事件均在输入层拒绝。

最终源码摘要69374372a05c48602f2bb20716c6ef66921e371fa3df9cd768cc176cdba8cafa。原生隔离PG/HMAC网关→direct固定argv→真实HTTP→实际量化worker→固定报告联调1通过，四份产物回执；独立SQL确认原意图1、run1、量化job2/attempt2、初选2及评议2。job66d25be2-67c8-4fbd-8375-68f0642939c0、run4f55fe00-1019-48f9-bc37-9a329e1a0e24、reportf647946c-23b6-497e-8e24-e4f02841e051均属隔离合成数据，显式manual/late、目标23日/行情20日，空池模型调用0，不是当日真实研究。临时API65139/53642终止及端口释放已确认，证明用例已从services工作树移除；私有材料仅在忽略目录.stock-research/bootstrap/premarket-execution-*。

最终全量Python6105通过、无skip、50旧警告，专项76通过；Ruff/1283文件格式、完整质量、红线108文件零命中、workflow/dependency hygiene及合成daily dry-run通过。Python LOC143004/+65.9%、TS36116/+94.2%旧基线警告保留；无新豁免、测试排除或依赖变化。services全量500通过/3原Docker条件skip。本批无Web源码变化，沿用a4f1408a完整Web409/API221及两项默认Upstash跳过，不将本地通过称为远端CI通过。

零活动守卫后仅更新本线程8788 API；旧48204终态143、PID消失/端口释放，当前67720/session85786，日志premarket-execution-main-api.log。ready及原完整报告/问答、跨workspace404/只读私人问答403独立读回通过。额外独立repeatable-read只读审计26表：有workspace字段按stocks过滤，其余全表逐行摘要，重启前后保持；没有新增主工作区研究或通知。生产workflow身份仍关闭，原42计划/Personal共享8802及网关未改未重启。

下一步优先对齐plan第9节：当前只保证同一事件重放，新manual事件的同配置去重与强制补跑parent链仍待实现。随后接行情更新协调、通知/未知回执回查、唯一stock-research-premarket六处清单和cron所有权。正式Mac运行包/TCC、Tailscale/多用户、自然交易日、正常浏览器、PIT/效果评估、独立wiki缺失、远端CI与另session整体审查继续保留；整个plan.md未完成。

## 续接检查点：盘前原计划时刻与固定入口计划预览（2026-11-24）

Personal三项只读接入已提交研究6d8c1e3f、services3b52256/文档6f60ac2并本机快进；运行宿主8802已加载、原11项保持。此后继续plan第10/11节，先修正direct cron固定脚本收不到原slot的问题。services本批代码f238c70：仅显式pass_schedule_context=true的direct计划追加五项原事件元数据至既有input.json，严格布尔/身份时刻一致；缺省保持原action/输入/幂等，原42计划未启用。生产网关部署副本未更新，不冒称新调度已生效。

研究新增python -m stock_research.premarket --plan，按原slot纽约日期确定目标日、前一完整日、正式开盘减60分钟、纽约08:50恢复截止；节假日跳过不滚日。自动过期预算0，显式手动迟到仍标记且只用原手动事件起3000秒余额，重放不重置。输入16KiB/额外字段拒绝、ID/原时刻一致，runner文件限同一run目录并拒绝软链/越界，0600原子输出。仅生成mode=plan/research_started=false，不读取研究.env/Settings、不接HTTP/PG/模型/通知，不能将services completed当作研究成功。见stock_research_premarket.md；实际执行入口仍待后续接通。

services原生隔离PG/HMAC事件→direct→固定股票argv→input/output真实联调1通过，四份产物回执；23日13:00Z事件延迟到24日观察，目标仍23日、行情20日、expired/预算0，无新研究。临时证明用例已移除，工程结果保留忽略目录.stock-research/bootstrap/premarket-context-bridge.{json,log}，不提交数据库、私有输入或日志。

最终源码冻结后全量Python6060通过（无skip、50旧警告），新增计划专项20通过；services专项56、最终全量500通过及3原Docker条件skip。Ruff/1273文件格式、完整质量、root红线99文件、workflow/dependency hygiene和合成daily dry-run通过。Python LOC142401/+65.2%、TS36116/+94.2%旧基线警告保留。services四旧红线命中与6f60ac2独立基线文本相同（默认URL、load_state、两个旧测试helper），新helper已改TypedDict，新增0；不称其全仓零命中。未改Web，沿用a4f1408a完整Web409/API221及两项默认Upstash跳过。无依赖变化、函数豁免或pytest排除。

主workspace零活动守卫后，仅更新本线程8788 API。旧25392及中间47423均终态143、PID消失/端口释放确认；补充手动预算固定后最终PID48204/session52210，日志premarket-context-main-api-final.log。ready200，独立连接/HTTP确认原行/摘要及完整cf8da076报告、a45bf9c4问答保持，跨workspace404、共享只读身份私人问答403。没有新研究、问答、个人记录或通知；共享Personal8802/网关/调度不重启。计划来源的两条历史资料断链已改为本地git核实存在的dc5fec0e固定提交链接，91处本轮本地文档链接检查通过。

下一步接专用有限API身份、原期限内启动/等待/局部恢复、行情更新/60及180秒退避，再完成唯一stock-research-premarket六处清单/cron所有权与通知去重回查。已只读确认services存在/internal/deliver受工作区bus身份限制，另有内部task_delivery；接入需核对稳定业务键/未知投递状态，不能复制SA_INTERNAL或网关PG身份给股票workflow。正式Mac运行包/TCC、Tailscale/多用户、正常浏览器控件、旧行情更新协调/PIT及效果评估、wiki缺失、远端CI和另一个session整体审查继续保留，整个plan.md目标未完成。

## 续接检查点：Personal三项只读查询已加载（2026-11-24）

私有问答有界重试已提交a4f1408a。本批personal-mcp命令先预览，只在提供原摘要后追加唯一Personal预设的stocks MCP；保留自定义JS标签和其余原文，不重新序列化整份运行配置。合作式文件锁、两次写前字节核对、0600备份和原子替换；精确撤回只删除本次插件，保留后来其他修改，撤回不依赖原源码或身份文件。默认预览不写入，命令不构造服务器Settings。查询子进程env -i只载既有query.env，不继承PG、模型或Personal身份；安装结果runtime_verified=false，不将写文件当成宿主已加载。详见stock_research_personal.md。

原生DSH 0.2.1-alpha.2隔离宿主实际挂载Personal预设，保留原11项并增加三项，合计14项；经agent工具执行链实读Personal能力、固定报告、GILD观点与对应run，四项一致，模型轮数0。报告仍为cf8da076-2f58-42de-93fa-ae2fa6de65cd/run f77a7db4-b830-4310-9463-2e97565b9c17，实际行情日2026-11-20，不冒称当日新研究。拥有者退出后独立ps确认PID26776及其进程组均不存在。

运行profile仅新增本次节点，写前零活动守卫及0600备份完成。正常认证的运行DSH Remote独立读回Personal预设无broken、新插件与仓库片段一致；剔除该节点后其他Personal插件摘要前后一致。共享宿主PID8802未重启；原会话持旧预设修订，新Personal会话取得新工具。OpenCode仍只读8项，不自动继承。撤回预览得到原profile摘要、原备份摘要一致；没有实际撤回已启用接入、没有模型或通知。备份位于~/.dsh/backups/stock-research-personal-20261124，私有验证材料位于忽略的.stock-research/bootstrap/personal-mcp-*。

源码冻结后全量Python6040通过、无skip、50旧警告；新专项11通过。Ruff/最终1269文件格式、完整质量门禁、红线98文件、workflow/dependency hygiene及合成daily dry-run通过。Python LOC142285/+65.1%、TS36116/+94.2%旧基线警告保留，无新函数豁免、测试排除或依赖变化；本批无Web源码变更，沿用a4f1408a的Web409/API221及两项默认外部Upstash跳过完整Web检查。services独立工作树全量491通过、3项原Docker条件跳过；首次从错误cwd启动测试曾有3个网关导入错误，纠正为实际services工作目录后全量通过，未排除用例。bin/check-sync.sh通过，五份Personal文档及NOW三份镜像字节一致。

仅重启本线程8788 API，旧PID17003终态143、端口释放已确认；当前PID25392/session78765，日志personal-mcp-main-api.log。ready200，独立连接/HTTP确认主workspace零活动研究/问答、全部原行和摘要保持，原完整报告及a45bf9c4问答可读，跨workspace404、只读身份读取私有问答403。26表未变；未创建研究/问答或修改个人事实、持仓、提醒与计划。

本批代码已提交研究6d8c1e3f、services3b52256（文档6f60ac2），并本机快进合入原services分支，未推送；原checkout的hourly-report-review.md删除及platform/social/mail既有dirty保留。正式launchd运行包/TCC路径、Tailscale身份入口、浏览器正常登录控件、唯一stock-research-premarket工作流/六处注册/定时/通知回执、行情旧更新协调和数据缺口、PIT及效果评估仍待接续。独立wiki checkout缺失，远端CI与另一个session整体审查尚未通过，不声称可合入或全plan完成。

## 续接检查点：私有问答有界重试与逐次回执

量化局部恢复已提交dbceb196。本批同一问答只对明确transient_provider_error/transport_error自动重试一次，共用原创建时间计算的deadline、固定报告/父链、模型连接及20次工具请求总预算。每次先在chat_turns JSON中追加running，失败回执和已确认计数先保存，下一次只用余额；每次独立产物、DSH home/agents home、注册表及0600身份文件，收尾删除身份。没有新表、迁移或配置键，仍26表。

成功尝试与已核验答案同事务提交，随后取消拥有者仍保持completed；幂等POST只读原终态。限流、结构/证据、预算、期限、边界/清理错误和取消不重试；本地清单/证据文件不可读属于边界失败，执行器缺失/无执行权限记executor_unavailable，不能冒充provider网络错误。重启不重放未知请求。公开attempts最多两项，不含私有回执路径；未知计数为null，顶层为各次已确认请求数之和，超过预算且被拒绝的请求仍计入请求计数。历史attempts为空表示未记录，不能补造旧次数。Agent原问答条目展开状态、失败与计数，无新页面。

隔离原生PG/ASGI及真实stdio子进程的合成Harness验证失败后成功、两次失败、分类拒绝重试、剩余预算、原期限、模型连接只解析一次、取消/重启、并发摘要CAS、跨workspace及独立读回。成功提交后立即注入拥有者取消，已完成回答仍可读。模型与事实为明确合成数据，没有调用真实provider。独立实际客户端→Node代理→HTTP→PG及组件SSR读回两次状态/四次请求（余额20→17），跨站与共享只读身份403，私有路径不出响应；这不替代正常登录后的浏览器控件验收。合成联调问答仅在quant-proof工作区，主workspace没有新问答或研究，原存储行及内容摘要保持。

最终源码冻结后全量Python6029通过（无skip、50旧警告），新增专项19通过；Web409、API221通过及两项默认外部Upstash集成跳过，frozen-lockfile、TS strict、Web及Pages-functions构建通过。Ruff/1265文件格式、全范围函数门禁、红线136文件、workflow/dependency hygiene和合成daily dry-run通过。Python LOC142045/+64.8%、TS36116/+94.2%旧基线提示保留；没有新函数豁免、pytest排除或锁文件变化。42份生产JS检查七份实际私有值均未发现。

确认主workspace零活动研究/问答后，仅重启本线程8788 API，旧PID14911终态143/端口释放确认；当前PID17003/session47455，ready200，日志chat-retry-main-api.log。原cf8da076报告、a45bf9c4完成问答及所有主工作区存储行/摘要独立读回保持，新增历史attempts空数组只读元数据。临时65102 API（PID16457）已终态143并确认端口释放；原Vite客户端读取旧报告/问答通过。最终合成联调问答55adec2b-9805-4d5d-9518-3f6d3ee7fa3a仅在隔离工作区，真实provider调用为零。凭据/回执/日志/客户端验证均保存在忽略目录.stock-research/bootstrap，不提交。

行情协调只读调查确认：来源dc5fec0e的scripts/us_sp500_maintenance.py依赖旧core.stock_cache、yfinance及旧交易日历，旧market_data_sync.py未持新研究协作锁；us_daily_bar_refresh/us_sp500_bootstrap在来源工作流，主研究基线没有这些运行时。不能把新同步锁说成已覆盖旧更新/Actions，也不能直接复制一个脚本就执行云端写入。后续须一并迁移已登记入口、固定目标日和指数并集/独立基准范围，校验全部写入方参与锁，先在隔离PG验证，再交付部署；当前源工作树未修改。

继续plan.md全局目标：旧行情更新协调、正式/PIT来源及云端缺口、Personal三工具注册/唯一workflow/通知、Mac部署和研究评估。Personal仓库只读核对，原有未提交改动保留，尚未认领或改动其文件。浏览器仍待正常账号认证，wiki checkout缺失，远端CI、另一个session整体审查及正式部署未验收。

## 续接检查点：按策略量化任务与局部补算

worker隔离已提交d8876d7f。本批API/CLI在共同快照、参数及模型偏好冻结后才执行量化；新run、两份pending任务及初始报告同事务创建。新增quant_jobs/quant_attempts，显式init-schema后独立SQL确认26表。每次worker前保存running尝试，成功因子与终态同事务提交；CAS拒绝并发推进及覆盖成功，超时/进程错误最多自动重试一次，失败继续另一策略。显式resume只补未成功量化和缺失模型阶段，使用原参数，成功初选及原run输入payload保持。

缺计算在股票、整池、市场、规则、分组及锚点查询中表达为不可用/null，和成功空池分开；名单保留前一有效状态并标当日未复核。报告修订固定当时量化绑定，后续成功不回填旧报告。HTTP/CLI可分页读取本run实际尝试；历史已完成运行的任务ID和次数未知，recorded=false，不制造历史。Agent既有报告内显示固定状态及可展开的当前尝试历史，无新页面。缓存v2绑定归并后的快照ID及内容摘要，跨workspace复用相同内容不会串快照身份。说明见stock_research_quant_execution.md。

最终源码冻结后全量Python6010通过，无skip、50旧警告；Web406、API221通过及两项默认外部Upstash集成跳过，frozen-lockfile、TS strict、Web/Pages-functions构建通过。Ruff/1263文件格式、函数门禁、红线134文件、workflow/dependency hygiene及合成配置daily dry-run通过。Python LOC141888/+64.6%、TS36066/+94.0%旧基线提示保留；增长对应量化持久状态、恢复、查询与Agent契约，无新函数豁免或测试排除。

隔离真实行情验证run94dd217d-77a2-4cb8-9693-cc6fe97d1d09：Martin实际worker两次超时，Wyckoff实际计算空池及初选成功；仅补Martin后两策略量化成功，Wyckoff产物/初选ID与原输入payload、旧报告5195d5e1均保持。独立数据库和真实前端客户端→Node代理→HTTP读回四条尝试、旧失败和新成功状态，跨站403。刻意缺模型配置且没有调用provider，新报告f07d741d仍为partial/executor_configuration_failed，不称新完整研究通过。主workspace未创建量化任务/尝试或新模型run；原cf8da076报告保存内容和a45bf9c4问答保持，当前API新增legacy只读量化元数据。

仅重启本线程8788 API：旧PID90285终态143/端口释放确认，当前PID3258/session63898，ready200，日志quant-recovery-main-api.log。临时65102 proof API已停止，终态143且端口释放；原Vite客户端实际通路可用。42份生产JS未含已检查的七份研究/模型/PG私有值。证明材料、凭据及日志保留在忽略的.stock-research/bootstrap，不提交。

浏览器仍待正常账号认证，未作控件验收；wiki checkout缺失，远端CI、另一个session的整体独立审查及正式部署仍待完成。无Personal/secrets改动或通知。总体plan.md goal继续，接续行情更新协调、Personal三项只读注册/唯一调度通知、Mac部署及研究评估。

## 续接检查点：可停止的量化worker，局部恢复继续开发

锚点图表已提交b0a331eb。本批先替换API prepare_from_market与CLI run的线程量化为analyze_isolated，正式compute_martin/compute_wyckoff在无身份环境的独立Python worker运行。父端保留控制管道，正常超时/取消回收自建进程组并等候终态；父端异常退出时worker原生读取线程在EOF后以75退出。临时目录0700/输出0600，报告与两帧协议、快照/参数/引擎及结果摘要在父端校验后才原子发布缓存；同输入并发接受校验后的首份缓存，损坏不覆盖。同步analyze命令仍前台执行，共用缓存身份/绑定校验。

这是局部恢复的执行前置，尚不是按策略失败隔离：当前仍先后准备两策略，单策略失败仍使准备失败，不能宣称另一策略初选继续。quant_jobs/quant_attempts尚未创建，数据库仍24表。接续在共同输入冻结后保存独立尝试/成功绑定，resume只补失败，历史报告保留当时绑定；缺计算/失败/空池分开表达，所有查询与名单及Agent须共同更新。具体约束与接续架构见design/stock-research-quant-recovery-2026-11-24.md及stock_research_quant_execution.md。

专项26通过，新补协议/文件限额后worker4项单独通过。最终源码冻结后全量Python6004通过（无skip、50旧警告）；Ruff/1258文件格式、质量、红线93文件、workflow/dependency hygiene及合成配置daily dry-run通过。Python LOC141190/+63.8%、TS35956/+93.4%旧基线提示保留，增长对应进程协议/监督/分派及测试。本批没有Web或API报文字段变更，未重复Web gate；上一锚点提交完整Web gate仍为402/221及两项默认Upstash跳过。第一次全量6003通过后增加限额用例，因此重新全量6004，不把未收集的新用例冒充已全量。

真实原快照32150191在隔离缓存目录完成Martin491/30、Wyckoff490/0的实际worker计算和缓存复用。比较旧f77产物时发现原始JSON有生成时间/摘要、Martin分组成员顺序及Wyckoff引擎标识差异；明确规范化这些元数据后，全部数值、条件、评分和池逐项一致，不称原JSON相同。真实控制EOF退出75且无报告文件/残留PID，未创建新数据库run或模型。测试还实际审计了worker导入文件打开，未见.env/.dsh身份读取；环境白名单不继承PG/API/模型/HOME，临时目录清理验证通过。

已有某次全tests目录红线扫描把test_chat.py的MCP工具名集合误判为SQL动态标识符，实际无SQL；未改无关用例或添豁免，最终扫描生产包及本批新测试/关联source_guard测试通过。没有排除pytest。确认0活动研究/问答后，仅重启本线程8788 API，旧PID84188终态143与端口释放确认；当前PID90285/session74859，日志quant-worker-main-api.log。readyz200，原cf8da076报告、锚点及a45bf9c4已完成问答独立HTTP读回不变。

浏览器仍待正常账号认证，未作控件验收；wiki checkout不存在，远端CI/独立整体审查/正式部署各自未验收。没有Personal/secrets改动或通知。总体goal保持，继续真正的按策略局部恢复，再处理行情更新协调、Personal唯一调度通知及Mac部署。


## 续接检查点：已确认锚点的固定图表投影

主题/自选维护已提交b123dfc0。本批为原Martin锚点增加独立固定报告GET /v1/reports/{report_id}/anchors/{symbol}、ResearchReader.anchor_chart和query_cli anchor-chart。完整目录保留原索引/标量引用，默认最近确认项，每次只返回一条曲线，不放大原股票/问答/MCP工具响应。锚点及确认日必须在冻结行情中且匹配原参数右侧确认间隔，不重新检测结构。完整日线从锚点累计典型价成交量均价、末值与原daily_estimate对拍；确认前不显示，请求末N根不重置累计，零量保持null。

Agent个股图表已接目录选择、显示开关、单条估算曲线与确认点，原锚点/确认日期分别显示；priceOnly仍关闭自动均线、结构与支撑推断，显式evidenceLines独立接入，切换/销毁清理。合成测试驱动生产compute_martin对拍保存结果，原生PG→ASGI→最小客户端覆盖固定报告、范围和读身份；渲染器实际setData/确认标记/清理验证。实际画布和选择按钮仍待正常账号认证，此处没有宣称浏览器验收。

真实原cf8da076修订3的GILD/ACGL/MRSH各有16/15/15项原锚点，选择索引0分别读回56/53/48个确认后点，截止值与原估算一致；两根窗口与完整曲线尾部一致。独立PG、只读HTTP、CLI及前端客户端→实际Vite代理逐项通过，原标量引用仍可查，跨站403/外报告404；原报告及a45bf9c4完成问答不变。默认最近确认项为两根曲线，和手选早期锚点区分；未创建新模型/量化运行、通知或维护发布，stocks建议/发布仍0。

冻结源码后全量Python6000通过（无skip、50旧警告），专项9通过；完整Web gate为frozen-lockfile、TS strict、Web402、API221/两项默认外部Upstash跳过、Web/Pages-functions构建。Ruff/1251文件格式、全范围质量、红线125文件、workflow/dependency hygiene与合成配置daily dry-run通过；42份生产JS无研究operator/只读及本机模型凭据。Python LOC140877/+63.4%、TS35956/+93.4%旧基线警告保留，本批增长对应显示投影/契约/客户端与图表。

确认0活动研究/问答后，仅重启本线程8788 API，旧PID78095终态143及端口释放确认，当前PID84188/session3233、readyz200；服务保存于忽略的anchor-main-api.log。本机Vite沿用原服务，实际客户端通路可用。浏览器仍按上一检查点browser2/tab2在正常login待handoff结束；wiki checkout不存在，远端CI、整体独立审查及正式部署未验收。说明见stock_research_stock_view.md；继续行情更新协调/局部量化降级、Personal唯一调度通知及Mac部署，保持plan.md总体goal，不修改其他Personal工作树或secrets。


## 续接检查点：主题确认与人工自选冻结输入

上一批整池浏览已提交f31895b6。本批增加theme_proposals与input_publications，显式init-schema后24表；主题提议pending不生效，人工确认按主题基准/当前修订CAS，完整成员定义可清空。manual按代码add/remove追加、先读当前最高修订，不移除指数关系，行业只补缺。来源、理由、确认时间、生效日、请求及摘要保留；未知提交复用原请求/幂等键核对，旧修订不自动覆盖。私有历史及冻结来源仅operator可读，共享只读身份403。

生效日不能早于确认时的纽约日期。API创建与CLI默认run按snapshot.as_of在一条MVCC查询中选择已生效最高修订，按内容摘要物化股票池/分类后进入原生产快照与量化路径，来源摘要绑定实际快照。显式snapshot-file保持原文件；单独snapshot --offline继续只读版本文件。旧截止日不消费后来发布，旧报告不补造历史；人工输入仍标非严格PIT。首次全量暴露来源标签导致同输入API/CLI重复run，已排除该标签参与run_key，原回归测试保留并重新完整通过，实际版本摘要仍参与身份。

原生隔离PG测试实际覆盖并发CAS、异参幂等冲突、跨workspace、缺分类/行情、manual移除保留指数、未来定义排除、来源损坏拒收、SQL分页、API/CLI同UUID及生产FACTOR_RESULTS中已确认主题；只有确认时钟为固定合成交易日受控。真实隔离workspace经前端客户端→Node代理→HTTP→PG完成EngineeringProof/GILD提议及确认、自选发布，3000中文字理由、旧修订409、只读/跨站403、超64KiB 413均验证。独立进程SELECT及临时API重启后读回相同发布；真实旧as_of未消费新发布，生效日只验证派生版本解析，没有运行新真实量化或模型。

最终源码冻结后全量Python5997通过（无skip、50旧警告），完整Web gate为frozen-lockfile、TS strict、Web397、API221/两项默认外部Upstash跳过、Web/Pages-functions构建。Ruff/1247文件格式、全范围质量、红线118文件、workflow/dependency hygiene与合成配置daily dry-run通过。Python LOC140691/+63.2%、TS35796/+92.5%旧基线警告保留，新增功能对应人工输入存储、解析、API/CLI及现有Agent接线。42份生产JS不含operator/只读/模型身份，凭据和实测日志只在忽略目录。

主API仅重启本线程拥有的8788服务并确认旧进程退出/端口释放，新版ready200；原cf8da076修订3报告及a45bf9c4完成问答保持，stocks提议/维护发布均0。临时隔离proof API已停止并确认释放端口。现有Vite客户端实际读取原报告冻结来源及空维护历史通过。IAB原tab1已成为错误dataURL，另建browser2/tab2正常/login并handoff，真实按钮仍待正常账号认证，不能把客户端测试视为控件验收。

本批说明见stock_research_input_maintenance.md。wiki checkout不存在，远端CI、独立整体审查及正式部署未验收；无Personal仓库/secrets修改、通知或主workspace参数发布。继续已确认锚点叠加、行情更新协调/局部量化降级、Personal唯一调度与通知及Mac部署，保持plan.md总体goal。



## 续接检查点：完整冻结股票池搜索与分页

固定报告问答已提交6f8f6c0c。本批增加GET /v1/reports/{report_id}/universe及最小ResearchReader.universe，基于原快照全部有效成员去重投影，由PG执行字面搜索/代码稳定排序/分页。search最多100字符，LIKE通配符不扩展；保留无行情/分类成员和未来分类排除。两策略data_eligible、calculation_available、in_pool、selected各自表达，未提交初选为null，已提交未入选为false，不重算策略。规范绑定问答market_overview增加view=universe，仍八工具且不能换报告；独立research_mcp原市场契约与Personal三工具保持。Agent原个股区加搜索/分页表格，点击公司接原固定事实与问答；豁免为单页修补，不增加页面或角色。

真实cf8da076修订3通过只读HTTP完整翻完518只，稳定无重复；17只无行情、15只无行业仍在。Martin数据/实际计算491、量化池30、初选5，Wyckoff490/0/0，与原报告及原个股事实一致。独立stdio调用绑定market_overview(view=universe, search=gild)，取同报告GILD事实；前端客户端经Node代理读两个20项页面、搜索、打开固定个股及SSR对拍通过，跨站403。原报告和已完成问答的私有证据/回执逐字节保持；没有启动新模型、通知或参数发布。

冻结源码后的全量Python5992通过（无skip、50旧警告），整池/固定查询/问答专项11通过；完整Web gate为frozen-lockfile、TS strict、Web393、API221/两项默认外部Upstash跳过、Web/Pages-functions构建。Ruff/格式、质量、红线、workflow/dependency hygiene与合成配置daily dry-run通过，42份JS无operator/只读/模型身份。Python LOC139707/+62.1%、TS35543/+91.2%旧基线警告保留，增长对应整池查询/工具分支/界面。新增设计与操作说明见stock_research_universe.md。

主API旧源码ready503而报告可读，确认无活动研究/问答后只重启本线程拥有的8788服务，终态退出和端口释放确认后再启动。本机readiness/新端点及旧完成问答可读；数据库仍22表，无新DDL。实际浏览器控件仍待正常账号认证，IAB在/login且Mac锁屏；wiki checkout不存在，远端CI、独立整体审查与正式部署未验收。接续主题/自选的人工确认及实际输入消费、已确认锚点叠加、行情协调与局部量化降级、Personal唯一调度通知/部署；总体goal继续。


## 续接检查点：固定报告研究问答

上一批固定报告查询已提交06eab5d1。本批新增chat_turns（显式init-schema后22表），POST创建/GET历史及单问/显式取消与Agent内问答接线。每问固定report_id，幂等键复用原记录，父问答CAS只允许一个后继；历史从PG重建最多六问/24000字符。私有问答仅operator可读写，API_READ_TOKEN连GET也拒绝。研究运行与问答共享两席容量，防止持有执行锁后耗尽证据查询连接；满员重放原请求仍可读。

独立DSH复用可信主模型与原监督边界，固定八项规范工具，没有报告/URL/SQL/文件参数。工具实际成功返回的facts进入本轮注册表，结构化观点ref及数值claims逐值检查；一次JSON修正包含具体错误且共享deadline/调用预算。180秒、20次调用；当前问答不自动重试provider失败，不能据此声明plan的一次失败重试已覆盖问答。最小GET身份写入本轮home的0600临时文件，配置只含路径，收尾删除；DSH会清理凭据形状的环境变量，不能靠隐式继承TOKEN。模型凭据不交给MCP，回执同时脱敏两种身份。另以独立临时上下文目录实际stdio调用八工具，6658项事实且原执行注册表未改；API正常重启后，前端仍读回同一completed记录及两条失败历史。

真实主模型auto/deepseek-v4.1-flash的问答a45bf9c4-88ef-4c64-a258-89b84139e031已completed，固定原cf8da076报告修订3。report_read/run_status/market_overview实际三次成功，注册4335项事实，20条claims独立重新验证，并从新PG连接和实际前端客户端读回；相同键返回原UUID，READ身份403，原报告不变，Harness及MCP进程回收、临时身份删除。另有两条failed保留：第一条工具启动被TOKEN清理阻断，第二条输出/修正未过契约；修正后新建记录成功，失败不算成功或被删掉。关闭后的tool-manifest会随工具卸载变空/invalid，执行期间目录由初始化及回答前校验，不应把关闭后文件当作仍运行目录。

最终冻结源码后完整Python5991通过（无skip、50旧警告），问答/运行管理/执行器22通过；完整Web gate为frozen-lockfile、TS strict、Web390、API221/默认两项外部Upstash跳过、Web/Pages-functions构建。Ruff/格式、质量、红线、workflow/dependency hygiene及daily dry-run通过；dry-run明确注入合成模型/API配置，不调用生产或通知。42份生产JS不含研究operator/只读/模型凭据。Python LOC139531/+61.9%、TS35459/+90.7%旧基线警告保留，本批增长对应问答存储/执行/工具/前端。正常登录后的真实按钮和图表仍未验收；Mac锁屏、IAB仍在/login。wiki checkout不存在，远端CI、整体独立审查及正式部署未验收。

接续完整股票池枚举/搜索、主题及自选维护实际消费、已确认锚点叠加、行情更新协调/局部量化降级、Personal唯一调度通知和Mac部署。保持总体goal继续，不把本批问答交付视为plan完成。不修改已有Personal工作树、不发送通知、不发布主workspace参数。

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

源端独立草稿[PR #11](https://github.com/LanceJohnson45/Wyckoff-Analysis/pull/11)以wxjer为base；研究续接继续[PR #10](https://github.com/LanceJohnson45/Wyckoff-Analysis/pull/10)，base仍为原研究分支。两者不是同一部署范围，实际CI按各自head核对。

源候选08c38450的[Market Source CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45310016730)已completed/success，完整源端测试与daily dry-run均在同一job成功。另以现有授权云端连接实际取得两把协议锁，transaction_read_only=on，退出释放、未写数据；股票最新仍20日，云端QQQ/IWM仍0行。该结果验证云端只读协议兼容，不代表源候选已部署或缺口已修复。
