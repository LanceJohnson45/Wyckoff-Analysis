# 研究API与运行管理预览

`stock-research serve` 启动独立研究API，仅监听 `127.0.0.1:8788`。本机研究库需先显式 `init-schema`，启动会检查运行账本及请求映射表可读，不自动迁移。`.env` 中配置独立且至少32字符的 `STOCK_RESEARCH_API_TOKEN`，不能复用Personal网关身份；本机已生成身份，保存在0600的忽略文件中。部署主机和端口分别使用API_HOST/API_PORT，Origin白名单使用API_ALLOWED_ORIGINS的JSON数组；不提供任意主机绑定或关闭认证的配置。

健康检查 `/healthz`、依赖就绪 `/readyz` 无需身份，其余端点包括OpenAPI均需 `Authorization: Bearer <研究API身份>`。Host只接受localhost/127.0.0.1；带Origin的浏览器请求须在白名单内。当前未配置跨源CORS代理，工作台接入须使用受信同源后端。令牌不能进入URL、原始报告、浏览器持久化或日志。

所有JSON响应为code/message/data/correlation_id四键，与plan.md约定一致。成功code为0000；未认证401、未授权来源403、对象不存在404、状态/幂等冲突409、无效参数422、数据或存储未就绪503。未处理错误使用500及脱敏消息，不输出驱动错误、凭据或本地文件路径。

| 入口 | 已接通行为 |
|---|---|
| GET /v1/research-runs | SQL分页，page>=1、page_size=1～100，创建时间及UUID稳定排序 |
| POST /v1/research-runs | 固定共同输入、两份量化任务与首份pending报告后返回202，后台按策略量化并执行模型阶段 |
| GET /v1/research-runs/{UUID} | 当前状态、质量、数据截止日、报告期限、初选、互评及错误 |
| GET /v1/research-runs/{UUID}/events | SSE按持久化报告revision续读，max(Last-Event-ID, after)>=0，每连接最多三十秒 |
| POST /v1/research-runs/{UUID}/retry | 仅补未成功阶段；完整运行须显式重分析 |
| POST /v1/research-runs/{UUID}/cancel | 取消API拥有的任务，等候收尾再保存cancelled，不接管其他执行者 |
| GET /v1/reports/latest | 可按target_session或run_id过滤，返回最近创建运行的最新固定修订 |
| GET /v1/research-runs/{UUID}/reports | SQL分页的固定修订列表，page_size最多100 |
| GET /v1/reports/{UUID} | 原修订初选与互评；不会用当前运行状态重写历史 |
| GET /v1/stocks/{symbol}/observations | 必填run_id，可选同run的report_id固定当时意见；原因子、排除、行业和日线绑定快照，limit=1～320 |
| GET /v1/watchlists | 当前分策略观察状态及数据不足/未复核标记，SQL分页 |
| GET /v1/watchlists/events | 按strategy/symbol查询当时变化，SQL分页 |
| GET /v1/strategies | 两策略当前实际参数及operator基线/已发布来源 |
| GET、POST /v1/parameter-sets | 注册版本SQL分页/不可变注册，创建201 |
| GET /v1/parameter-sets/{UUID} | 参数产物详情 |
| POST /v1/strategies/{strategy}/activate-parameters | expected_revision发布，过期409 |
| GET /v1/research-runs/{UUID}/parameters | 固定运行参数及来源，不用当前发布覆盖 |
| GET /v1/reports/{UUID}/universe | 全部有效成员/字面搜索/SQL分页，缺数保留；见[整池浏览](stock_research_universe.md) |
| GET /v1/reports/{UUID}/market、groups、compare、rules | 固定修订市场/分组/比较/规则，只读且不重算历史 |
| GET /v1/preparations | 独立输入准备回执，可按target_session过滤和SQL分页；ready不等于量化/模型完成 |
| POST、GET /v1/chat-turns | operator私有问答创建202、按报告/会话SQL分页，必须固定report_id |
| GET /v1/chat-turns/{UUID}、POST /v1/chat-turns/{UUID}/cancel | 持久状态/回答及显式取消，不覆盖终态 |
| GET、POST /v1/theme-proposals、POST /v1/theme-proposals/{id}/confirm | 私有提议与人工CAS确认，完整契约见[输入维护](stock_research_input_maintenance.md) |
| GET /v1/input-publications、GET/POST /v1/universe-overrides、GET /v1/universe-overrides/{symbol} | 私有发布历史与manual维护、当前最高修订 |
| GET /v1/research-runs/{id}/inputs | 原运行冻结版本与发布关联，operator私有读取 |
| GET /openapi.json | 当前显式Pydantic输入/输出契约 |

创建请求仅接受market=us、target_session、preview=true及可选force/parent_run_id。必须带16～128字符的Idempotency-Key。相同键只返回原运行，改变请求载荷返回409；force与parent_run_id须同时提供，生成关联父运行的新版本。请求不能设置文件路径、SQL、模型或连接配置。冻结失败返回503，质量报告仍由数据入口物化；它不是已接受的研究运行。此入口不执行行情同步、通知或订单，更新和services调度仍在后续阶段实现。

API请求映射保存在新增api_requests表：只保存请求键摘要、业务请求及绑定运行。数据库请求锁覆盖冻结和绑定，崩溃后锁自动释放；冻结已创建但尚未绑定的运行按请求UUID找回。正常运行仍按日期和配置幂等，多次HTTP请求不重新选股。一个API进程的研究运行与问答合计最多管理两个任务，任务有强引用，关闭先停止接单、取消及等候任务，再关闭数据库。启动只恢复API创建的未结束任务；CLI独立运行继续使用自身锁。

成功初选不可覆盖。续跑重读首次冻结快照、参数和策略产物；执行代码或提示词版本改变时记录execution_version_changed并要求force新建，不能把新验证器/指令伪装成旧版本。阶段及总超时沿冻结清单执行。已超过报告期限的修订明确 `late=true`，研究状态与是否按时分别表达。

[源码版本保护](stock_research_source_guard.md)还校验当前进程加载版本与磁盘：改Python后readyz及新建/续跑明确503并提示重启，已有报告读取和取消/关闭继续可用。计算缓存随源码变化失效；重启不代表旧运行可以用新版覆盖初选。

SSE游标是run内单调revision，断线后用Last-Event-ID继续。每条report事件包含与固定报告GET相同的类型；无新修订时心跳，运行结束或连接时限到达后关闭。需要继续观察时使用同一游标重连。当前没有通知事件或投递成功声明。

RunView现在带companies，名称来自同一冻结快照的当时分类或成分名称，不向当前市场查询改写历史身份。浏览器EventSource以after查询参数初始化游标，重连的Last-Event-ID继续有效。

本机真实完整四阶段运行已能通过HTTP和独立CLI读回；[Agent研究面板](stock_research_agent_panel.md)已接线，登录后页面操作仍待验证。[个股固定查询与三项外部只读工具](stock_research_queries.md)已实现并实际调用，API_READ_TOKEN仅能GET共享研究数据，不能读取私有问答，独立查询进程不持有PG/模型身份。[持续观察名单](stock_research_watchlists.md)已接终态事务、HTTP/CLI及Agent面板（页面操作待登录）；[参数版本与CAS发布](stock_research_parameters.md)已接后台、API/CLI实际消费和Agent维护界面。[未建run的输入准备回执](stock_research_preparations.md)已接HTTP/CLI与Agent。[固定报告研究查询](stock_research_queries.md)已接HTTP及独立研究MCP；[固定报告研究问答](stock_research_chat.md)已接后台模型循环与Agent界面；[完整股票池浏览](stock_research_universe.md)已接HTTP/绑定工具和Agent；[主题/自选维护](stock_research_input_maintenance.md)已接输入消费与Agent；Personal工具注册/services、Tailscale入口和自然交易日部署仍待完成。

固定报告新增GET /v1/reports/{report_id}/anchors/{symbol}，limit=1～320、index可选；返回原锚点目录和一条确认后曲线。只读身份可读，未知索引422、冻结锚点/行情不一致503，原股票/问答工具不增加曲线负担。详见[个股图表](stock_research_stock_view.md)。

API/CLI run量化已使用可取消/限时回收的独立worker，完整校验后才发布缓存；同步analyze保持前台执行。共同输入先冻结，单策略量化失败已独立记录，另一策略继续并可只补失败部分，详见[量化执行边界](stock_research_quant_execution.md)。

按策略量化状态见RunView.quant及固定报告；GET /v1/research-runs/{run_id}/quant-attempts提供当前run独立尝试SQL分页（page_size≤20），不改变旧报告当时状态。量化失败不生成空产物，成功计算/初选不重做；缺计算的池、分组和计数可为null，不能解释为零。详见[局部量化恢复](stock_research_quant_execution.md)。

私有问答响应新增attempts（历史默认为空）：最多两次，包含状态、错误码、工具请求数（null表示未确认）、余额、上下文摘要、固定截止、起止时间及回执标记。顶层tool_calls累计已确认请求数。仅明确临时provider/传输错误且预算剩余时自动重试一次，同键POST不另起尝试；不新增重试/覆盖端点。私有路径不出HTTP，详见[问答重试边界](stock_research_chat.md)。
