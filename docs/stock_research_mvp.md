# 单用户盘前 MVP

首期只向既有Personal个人飞书收件人发送。固定入口为 `python -m stock_research.mvp --config /absolute/private/path/mvp.env`，业务输入和输出沿用[盘前任务](stock_research_premarket.md)的runner文件协议。配置模板为 `config/research/mvp.env.example`，只含operator固定路径，0600；业务输入不能提供命令、路径或凭据。

## 一次执行

1. 按原计划日期与固定股票池调用来源分支 `scripts/us_research_refresh.py`，补齐复权日线和SPY/QQQ/IWM。使用来源自己的解释器和PG配置，最长900秒，并为研究保留原期限余额。来源完成不等于质量通过，失败也不伪造行情。
2. 用专用workflow身份调用原盘前API。API开启 `PREMARKET_REFRESH` 后只读复制行情，再走既有覆盖率、历史长度、固定快照、两策略与交叉评议；原期限及成功产物复用规则不变。
3. 自动事件等到纽约08:30冻结已知首报。未完成也发布准备/运行状态，不将缺名单解释为空机会。整个研究流程同时继续，原08:50前成功阶段增加时再发一条恢复更新。
4. 发送复用Personal的 `task_delivery.reserve/ready/deliver` 和既有 `feishu_send.py`。按目标交易日及 `initial/recovery` 固定业务键，发送前保存正文；重放读取原正文。`sent`只有真实原生消息回执才成立，`retry/ready/unknown/unconfirmed`均不是成功。

手动事件使用真实执行时刻，完成后立即发布并明确“手动执行；不代表按时发布”。非交易日、过期和未到开始时间不启动行情或通知。通知正文包含报告编号、版本、截止日期、状态、候选原因、风险、待确认与交叉意见；完整事实保留在固定报告中。

## 身份与部署

父进程不读取来源PG、模型或Personal服务端身份。来源和通知各自用受控子进程读取既有配置，返回有限回执；子进程超时或取消须回收本次进程组。通知只允许既有 `ou_` 个人收件人，并核对来源workspace，不能从业务输入改发到群聊或他人。

API和来源代码须从固定版本安装到Documents之外，依各自锁文件建立venv；输入版本、SQLite和私有配置也放在可供后台读取的独立状态目录。不要直接把交互开发目录当24小时后台部署。来源候选必须包含行情协调锁和研究整池入口；API配置的空 `BENCHMARK_PATH` 表示消费来源云端已维护的三个基准，显式非空则另执行供应商基准补充。

单用户MVP采用独立launchd计划，每60秒运行 `scripts/stock_research_mvp_schedule.py --config /absolute/private/path/mvp.env --workspace <operator-workspace>`。tick固定纽约08:00原slot，只在原08:50前补跑，交易日由正式日历判断；同日已送达且研究终态保存done，不重复执行。单次进程50分钟内收尾；退出和重启不延长原slot期限。`--manual`使用真实当前事件时刻，单日手动完成标记独立保存。

这是用户优先最小MVP后的部署选择：原42条Personal计划、网关/平台配置及权限保持。通知仍直接复用其持久组件。将来迁入Personal计划面板时再完成workflows、capabilities、external、Policy、cron及同名标识六处登记，不能仅添加cron即声称已接入Personal调度。

## 验收状态

真实手动链已完成来源补拉、正式质量（覆盖100%）、量化/初选、3/4阶段的partial报告与个人飞书。首次通知配置依赖问题修正后沿原事件和冻结正文补发，CLI重放退出0；Personal仅一条业务记录，两次返回相同原生消息回执，飞书GET核对实际消息含报告编号。末项交叉评议因tool_budget_exceeded失败，未放宽预算或补造观点。

API与独立tick均已由launchd启动；原42条Personal计划保持。定点发布、恢复、冻结重放、纽约夏冬令时、非交易日/过期和真实子进程边界有专项测试。首个自然slot尚未发生，自动按时送达待自然日观察。最新来源补拉若为incomplete，须继续读取正式质量结果，不能将来源symbols计数等同完整覆盖。

截至2026-11-23的最新输入已单独核验：来源三基准各495行；SNDK缺目标日仍为incomplete。只读复制后正式质量为517/518、99.8069%，门禁通过，未降低95%覆盖或320交易日要求。完整手动报告和自然定点验收是不同检查点。
恢复发布存在时，首报与恢复消息都必须取得`sent`才写当天`done.json`；`retry/ready/unknown/unconfirmed`保留为未完成，后续自然tick在原08:50期限内沿原业务键和冻结正文重放。CLI同样返回非零，不把“首报成功、恢复未送达”整体报为成功。研究阶段已完成的产物继续复用。
