# 盘前固定入口与原计划时刻

目前提供无副作用的计划入口：`python -m stock_research.premarket --plan`。它从Personal固定argv runner提供的`SA_RUN_INPUT_JSON`读取计划上下文，把结果原子写入同一run目录的`SA_RUN_OUTPUT_JSON`，并输出相同JSON。`mode=plan`、`research_started=false`始终明确；services的completed只表示计划程序结束，不代表股票研究或通知完成。

## 调度上下文

Personal direct计划可显式配置`pass_schedule_context: true`，仅布尔值且只允许direct；默认不传，旧计划的action和input行为保持。启用后，网关只将以下元数据写入固定命令的input.json：

```json
{"schedule":{"schedule_id":"stock-research-premarket","event_id":"stock-research-premarket:2026-11-23T13:00:00Z","scheduled_for":"2026-11-23T13:00:00Z","workspace_id":"ws-beta","trigger_kind":"scheduled"}}
```

scheduled_for来自认证并持久化的事件occurred_at，不取领取/启动时间；ID必须匹配同一原时刻。面板显式手动事件保留manual-身份及trigger_kind=manual。事件subject/summary、路径、凭据和argv不进入该输入。重放同一事件仍复用原services run，配置冲突不会猜测新动作。

研究入口只接受唯一股票计划名及这五个字段。workspace_id表示Personal事件来源工作区，不用来选择研究数据库/租户。未来API调用仍须使用独立授权及服务端固定研究workspace；计划元数据不授予写权限。

## 日期与预算

自动slot须为纽约08:00，时区由固定版本XNYS日历决定；目标日取原slot的纽约日期，节假日明确skipped_non_session，不滚到下一交易日。交易日的as_of取前一完整交易日，report_due为正式开盘减60分钟，含夏冬令时与半日市。

| status | 行为 |
|---|---|
| before_start | 观察时间早于slot/纽约08:00，预算0 |
| within_window | 不超过内部3000秒且不越过原目标日纽约08:50 |
| expired | 自动事件已到原目标日08:50，或手动事件原时刻后3000秒已耗尽；预算0，跨日补跑不改目标 |
| late_manual | 显式手动且已过08:50，计划标迟到；仅使用该手动原时刻起3000秒内的余额，不因重放重置；仍未启动研究 |
| skipped_non_session | 非交易日，as_of/期限为空、预算0 |

手动计划的执行期限始终固定为原手动事件时刻加3000秒；重复预览/延迟领取只能减少余额。late按观察时刻是否已过原报告期限表达。`--at`只用于无副作用计划验证，要求带时区；不接受无时区本地时间。输入限16KiB，拒绝额外字段、身份/时刻不符、软链和越过runner产物根的路径；输出0600且不覆盖不同内容。入口不加载研究.env或Settings，不发HTTP、读取研究库、调用模型或发送通知。

## 实施边界

原生PG隔离网关→direct事件→固定argv→本入口→output.json已经实际联调；延迟到24日处理23日slot，仍归属23日/行情20日并标expired。自动调度尚未启用，既有42计划保持原配置。计划入口不能替代完整stock-research-premarket：下一步接独立有限API身份、行情更新及退避、原期限内研究/局部恢复、报告发布、通知去重与回查，再完成六处清单及正式Mac运行包/自然交易日验收。
