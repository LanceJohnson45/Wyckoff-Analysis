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

Personal自动计划需六处一致：workflows固定argv、服务端capabilities、external能力、Policy路由、cron、同名标识；纽约 `0 8 * * 1-5`，`pass_schedule_context=true`，单次3300秒。更改共享配置前确认没有活动run，保存原资产、保留其他计划，再按既有操作规程加载。原有42条计划不替换。

## 验收状态

本批代码已接主流程，定点发布/恢复/冻结重放与真实子进程边界有专项测试。真实行情补拉、报告、飞书送达及后台计划启用须分别记运行证据；未观察到自动slot执行前，不宣称“自动按时送达已验收”。
