# 盘前固定任务与原计划时刻

固定入口为 `python -m stock_research.premarket --plan` 或 `--execute`。plan只生成日历/预算；execute使用独立有限API身份实际受理、等待、局部恢复及读回固定报告。两者读取runner的`SA_RUN_INPUT_JSON`、在同一run目录原子保存`SA_RUN_OUTPUT_JSON`；services completed表示程序结束，研究状态另见output.receipt.status，通知当前固定为delivery=not_requested。

当前代码已通过隔离原生PG/合成行情及真实固定argv→HTTP调用链。生产42计划未启用新开关，专用身份默认关闭；行情刷新、通知及正式股票调度仍待接续。

## 调度上下文

Personal direct计划可显式配置`pass_schedule_context: true`，严格布尔且只允许direct；默认不传，旧action与input保持。网关只将经认证和持久化的以下元数据写入input.json：

```json
{"schedule":{"schedule_id":"stock-research-premarket","event_id":"stock-research-premarket:2026-11-23T13:00:00Z","scheduled_for":"2026-11-23T13:00:00Z","workspace_id":"ws-beta","trigger_kind":"scheduled"}}
```

scheduled_for来自原事件occurred_at，规范化为UTC，不取领取/启动时间；ID必须匹配同一时刻。手动事件使用manual-身份及trigger_kind=manual。事件正文、凭据、路径或argv不进入业务输入；额外字段拒收。来源workspace_id必须匹配API固定的PREMARKET_SOURCE_WORKSPACE，不能选择研究数据库或租户。目标须晚于当前安装日历的首个交易日（1990年初），并不超过2040年末，以便确定前一完整交易日。

## 日期与期限

自动slot固定纽约08:00，目标日取原slot的纽约日期，节假日skipped_non_session，不滚日；as_of为前一正式交易日。report_due取正式开盘减60分钟，含夏冬令时和半日市。自动执行截止原目标日纽约08:50；显式手动截止原手动事件加3000秒，重放只减少余额。

| 计划status | 行为 |
|---|---|
| before_start | 观察早于slot/纽约08:00，预算0 |
| within_window | 原事件窗口内的剩余预算 |
| expired | 自动原08:50或手动原3000秒已耗尽，预算0 |
| late_manual | 显式手动且已过08:50，只使用原手动预算余额 |
| skipped_non_session | 非交易日，无as_of/期限，预算0 |

late表示观察时刻已过原报告期限。计划预览的`--at`必须带时区，只允许plan；execute使用实际时钟。冻结run保存原job、事件与绝对期限；量化worker、模型阶段及恢复共同取该期限与原run创建时预算的较小者，不因API重启/重试延长。成功量化、初选及旧报告修订保持。

## 专用身份和API

服务端可选`STOCK_RESEARCH_API_WORKFLOW_TOKEN`至少32字符，须不同于API_TOKEN和API_READ_TOKEN，并配置`STOCK_RESEARCH_PREMARKET_SOURCE_WORKSPACE`。默认空值关闭该身份。独立客户端默认读取`.stock-research/workflow.env`（0600，模板见config/research/workflow.env.example），operator固定argv可用`--identity-file`指定另一专用文件，业务输入不能指定路径；键为WORKFLOW_HOST/PORT/TOKEN；地址仅127.0.0.1，不继承研究Settings、PG、Personal或模型配置。不使用操作员或共享查询身份。

| 方法与路径 | 回执 |
|---|---|
| POST /v1/premarket-jobs | 202；保存原意图，受理、跳过或准备失败均返回job_id及明确状态 |
| GET /v1/premarket-jobs/{job_id} | 200；持久准备回执、当前run及其最新固定报告 |
| POST /v1/premarket-jobs/{job_id}/retry | 202；仅原意图未成功部分，超期/取消/已完成冲突409 |
| POST /v1/premarket-jobs/{job_id}/cancel | 200；未冻结也保留取消标记，已完成研究保持终态 |

专用身份仅可访问这四个动作，不能列举全部任务、管理普通研究、查询共享报告或私人问答、修改参数/持仓/配置。对象、来源或研究workspace不符404，其他功能403，未认证401，跨站/Host校验保持。普通共享只读身份也不能读取盘前job。API_REQUESTS复用原账本记录独立前缀的原事件与run绑定，PREPARATION_ATTEMPTS记录失败；没有新表/迁移，普通CreateRun请求载荷保持。

请求级PG锁序列化受理、恢复与取消，SQL摘要CAS保留取消标记；重放先找原run，冻结后绑定前崩溃也可恢复映射。已取消意图不能由另一个API拥有者复活。取消已结束的partial任务只封存任务，保留原run/报告/初选，不把有效结果改写成空。恢复先保存pending状态，再排队执行。

## 客户端恢复与收尾

输入限16KiB，文件限runner授予的同一run目录，拒绝软链/越界；输出0600且不覆盖不同内容。HTTP禁代理/重定向，单次响应上限2MB，响应核对原job、计划、run与报告绑定；错误不回显正文/凭据。

准备失败或未确认按60秒、180秒两次退避，始终重放同一原意图。首次POST传输未知也使用该身份重放，不创建新事件。GET观察失败只重读原job，不据此认定终态或重启执行。仅明确临时的provider/transport/quant超时或进程错误可局部恢复，先60秒再180秒间隔，并服从原总期限；结构/证据/配置错误和已耗尽运行预算不重试。截止或SIGTERM时，对已确认任务有界取消/读回；准备重放后仍未确认记录preparation_unconfirmed；无法确认受理或取消时保留admission_unconfirmed/cancel_unconfirmed，不能声称已停止。未知受理不通过新POST猜测取消。

当前execute使用已有合格本地行情快照；未接必要行情更新时，数据不足返回准备失败，不把旧数据当当日新研究。没有报告通知请求或自动投递。下一步接行情更新及退避、原报告期限发布、通知稳定业务键与unknown回查，再完成唯一workflow六处登记、cron所有权和正式Mac运行包/网络/自然交易日验收。

当前去重边界是同一原事件及其首次冻结run；跨新手动事件的同配置合并、显式强制补跑的parent链仍须对齐plan.md第9节，正式登记前完成。
