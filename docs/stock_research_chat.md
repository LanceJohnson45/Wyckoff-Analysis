# 固定报告研究问答

现有Agent「策略研究」内提供独立问答。「带此报告继续讨论」和个股讨论会载入问题并固定报告；提交后后台使用可信模型连接及八项只读工具核对证据，不再将大段冻结JSON交给通用交易聊天。上方切换历史报告不会更改已经开始的问答，需显式改用另一报告。

## 输入、历史与状态

每问绑定report_id，问题最多6000字符。POST /v1/chat-turns必须带16～128字符Idempotency-Key，返回202只表示已受理。相同键和规范化输入复用原记录，换问题/报告/父问答返回409。parent_turn_id限定同报告且已结束的父问答；一个父问答仅允许一个后继，冲突时刷新，不自动换父重发。

GET /v1/chat-turns必填report_id，可选conversation_id，page>=1、page_size=1～100，以创建时间和UUID稳定排序并在PG分页。GET /v1/chat-turns/{id}读取单问。pending/running继续轮询，completed才显示已核验回答；failed/cancelled保留独立错误，不展示成成功空回答。POST /v1/chat-turns/{id}/cancel等待所拥有执行器收尾，不接管其他执行者。终态不可覆盖。

历史来自数据库父链，最多最近六个完整问答、合计24000字符，浏览器不能提交模型历史或伪造证据。失败问题保留为失败语境；旧回答的引用须本轮重新读取才能使用。断线导致提交未知时保留原问题和键，通过「确认原提交」查回，不能自动新建问题。关闭/重启明确记录取消或execution_interrupted，不自动重复未知模型消费。

## 证据与模型边界

后台沿用operator已有MODEL_PROFILE_PATH或MODEL_ENV_PATH的可信主模型，不接受问题指定模型、连接或路径。每轮独立DSH home及进程组；实际注册目录必须精确为market_overview、group_strength、stock_factors、stock_history、compare_stocks、strategy_rules、report_read、run_status。market_overview默认读取市场，可用view=universe分页/搜索全部冻结成员，详见[整池浏览](stock_research_universe.md)。工具不接收报告、URL、SQL或文件参数，范围由本轮冻结上下文固定。knowledge_search尚未授权接入。

默认180秒、20次工具请求预算；一次JSON修正共享剩余预算。明确返回transient_provider_error或transport_error、仍有时间和工具余额时，自动重试一次；两次使用原截止时间、模型连接、报告和父链，不重新计时。第一次失败先持久保存，第二次只得到20减已确认请求数的余额。provider_rate_limit、invalid_output、max_tokens、deadline_exceeded、tool_budget_exceeded、边界/清理错误及取消均不重试。本地清单/证据文件缺失属于边界失败，执行器文件缺失或无执行权限记executor_unavailable，不能当作provider网络抖动重试。重启或未知投递结果仍不重复模型请求。选股任务原有重试边界保持。问答与研究运行在同一API实例共享两席执行容量，为证据查询预留数据库连接；满员409不创建新问答。

公开attempts按顺序最多两项，保存状态、固定错误、工具余额与已确认请求数、上下文摘要、原截止、起止时间及回执是否存在。运行中或硬中断未能返回计数时tool_calls为null，明确表示未知；问答顶层tool_calls为各次已确认请求数之和。超过余额的请求被监督器拒绝，仍计入已尝试请求计数。历史问答attempts为空表示未记录逐次明细，不代表实际未执行。Agent可展开每次状态；重试耗尽保留两次失败，终态不被重复POST覆盖。

工具实际成功返回的标量和原ref保存到本轮私有证据注册表。观点必须引用本轮已读ref，数字、日期与百分比单独放claims，逐值核对；布尔与数值不混用。数组/count只表示条目数，不是策略分数。校验确认结构与引用数值，不证明模型定性推论正确，也不替代收益评估。report_read是固定修订，run_status是当前对应运行，量化池与正式初选分别表达。

## 身份、存储与接入

显式stock-research init-schema补建chat_turns，研究schema现有26张表；API启动只检查表，不自动迁移。每问保存报告/父链、幂等摘要、模型、源码/提示词/上下文摘要、状态及回执；尝试明细追加在同一chat_turns JSON中，摘要CAS避免同running状态推进互相覆盖，成功尝试与已核验答案同事务提交，提交后拥有者取消仍保留完成回答；不增加表或启动迁移。每次执行独立产物、DSH home/agents home和证据注册表，私有回执路径保留于原记录。公开响应不含输入内部字段、回执路径、模型连接或令牌。

问答仅API_TOKEN的operator身份可读写；共享API_READ_TOKEN连问答GET也返回403。当前是一个operator workspace，正式多用户身份映射属于部署工作。行情工具进程只持独立GET身份，不持PG或operator权限；0600临时身份文件位于本轮私有home，仅路径进入Harness配置，进程收尾删除。模型凭据不传给MCP进程，回执同时脱敏模型和只读身份。

Node开发代理仍仅loopback/同源/固定路由。问答创建正文上限48KiB，其他写入仍8KiB；6000字符限制仍由后端验证。两种研究身份都只在服务端，不进入VITE变量或生产bundle。生产静态构建没有开发代理，不能据此声明Tailscale或多用户部署完成。

真实模型、持久读回及本批验证结果记录在[交接检查点](stock_research_handoff_2026-11-23.md)。正常账号登录后的真实控件、独立wiki、远端CI及正式部署分别验收。

量化恢复接入后，工具沿固定报告返回当时quant状态；未成功计算、null池/分组/计数不能解释为空池、零或否定股票。run_status的后来补齐仅是当前进展，不可写回report_read旧修订；提示词按此更新，原已完成问答保持。当前尝试分页不增加聊天工具或重算权限。
## 问答证据体积与分页

八项问答工具均支持`fact_offset`（默认0），返回`total_facts`及可选`next_offset`。保留同一工具的其他入参，用`next_offset`续读必要页。每页工具JSON不超过48KiB，每个工具进程累计返回不超过256KiB；超限明确报错，不截断标量、不把未返回页登记为已读。数组`count`仍是原数组完整条目数，`total_facts`仅表示标量证据总数。

`stock_factors`返回股票事实、排除与观点，完整参数/定义改由`strategy_rules`读取；`stock_history`只返回身份和日线。`run_status`返回当前状态，固定意见由`report_read`读取。报告/市场不重复返回整个数据合格股票清单，量化池沿`market_overview`，完整成员沿`view=universe`查询。报告观点中的原始引用清单不当作本轮已读原事实，需通过相应股票/分组工具读取才能用于数值声明。

这些投影和分页只影响问答工具载荷，HTTP报告、浏览器图表与冻结产物保持原结构。上线后应重新实跑固定报告问题；旧失败问答保留原终态，不因修复而改写。模型上下文溢出与工具预算耗尽均不能当作成功回答。
