# 固定报告研究问答

现有Agent「策略研究」内提供独立问答。「带此报告继续讨论」和个股讨论会载入问题并固定报告；提交后后台使用可信模型连接及八项只读工具核对证据，不再将大段冻结JSON交给通用交易聊天。上方切换历史报告不会更改已经开始的问答，需显式改用另一报告。

## 输入、历史与状态

每问绑定report_id，问题最多6000字符。POST /v1/chat-turns必须带16～128字符Idempotency-Key，返回202只表示已受理。相同键和规范化输入复用原记录，换问题/报告/父问答返回409。parent_turn_id限定同报告且已结束的父问答；一个父问答仅允许一个后继，冲突时刷新，不自动换父重发。

GET /v1/chat-turns必填report_id，可选conversation_id，page>=1、page_size=1～100，以创建时间和UUID稳定排序并在PG分页。GET /v1/chat-turns/{id}读取单问。pending/running继续轮询，completed才显示已核验回答；failed/cancelled保留独立错误，不展示成成功空回答。POST /v1/chat-turns/{id}/cancel等待所拥有执行器收尾，不接管其他执行者。终态不可覆盖。

历史来自数据库父链，最多最近六个完整问答、合计24000字符，浏览器不能提交模型历史或伪造证据。失败问题保留为失败语境；旧回答的引用须本轮重新读取才能使用。断线导致提交未知时保留原问题和键，通过「确认原提交」查回，不能自动新建问题。关闭/重启明确记录取消或execution_interrupted，不自动重复未知模型消费。

## 证据与模型边界

后台沿用operator已有MODEL_PROFILE_PATH或MODEL_ENV_PATH的可信主模型，不接受问题指定模型、连接或路径。每轮独立DSH home及进程组；实际注册目录必须精确为market_overview、group_strength、stock_factors、stock_history、compare_stocks、strategy_rules、report_read、run_status。工具不接收报告、URL、SQL或文件参数，范围由本轮冻结上下文固定。knowledge_search尚未授权接入。

默认180秒、20次工具调用；一次JSON修正共享剩余预算。当前问答不会自动重试provider失败，错误保存为限流、暂不可用或执行失败，避免未知费用重复。选股任务原有重试边界保持。问答与研究运行在同一API实例共享两席执行容量，为证据查询预留数据库连接；满员409不创建新问答。

工具实际成功返回的标量和原ref保存到本轮私有证据注册表。观点必须引用本轮已读ref，数字、日期与百分比单独放claims，逐值核对；布尔与数值不混用。数组/count只表示条目数，不是策略分数。校验确认结构与引用数值，不证明模型定性推论正确，也不替代收益评估。report_read是固定修订，run_status是当前对应运行，量化池与正式初选分别表达。

## 身份、存储与接入

显式stock-research init-schema补建chat_turns，研究schema现有22张表；API启动只检查表，不自动迁移。每问保存报告/父链、幂等摘要、模型、源码/提示词/上下文摘要、状态及回执。公开响应不含输入内部字段、回执路径、模型连接或令牌。

问答仅API_TOKEN的operator身份可读写；共享API_READ_TOKEN连问答GET也返回403。当前是一个operator workspace，正式多用户身份映射属于部署工作。行情工具进程只持独立GET身份，不持PG或operator权限；0600临时身份文件位于本轮私有home，仅路径进入Harness配置，进程收尾删除。模型凭据不传给MCP进程，回执同时脱敏模型和只读身份。

Node开发代理仍仅loopback/同源/固定路由。问答创建正文上限48KiB，其他写入仍8KiB；6000字符限制仍由后端验证。两种研究身份都只在服务端，不进入VITE变量或生产bundle。生产静态构建没有开发代理，不能据此声明Tailscale或多用户部署完成。

真实模型、持久读回及本批验证结果记录在[交接检查点](stock_research_handoff_2026-11-23.md)。正常账号登录后的真实控件、独立wiki、远端CI及正式部署分别验收。
