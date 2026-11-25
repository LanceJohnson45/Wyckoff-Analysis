# 盘前固定任务与原计划时刻

固定入口为 `python -m stock_research.premarket --plan` 或 `--execute`。plan只生成日历/预算；execute使用独立有限API身份实际受理、等待、局部恢复及读回固定报告。两者读取runner的`SA_RUN_INPUT_JSON`、在同一run目录原子保存`SA_RUN_OUTPUT_JSON`；services completed表示程序结束，研究状态另见output.receipt.status，此基础入口固定delivery=not_requested；来源补拉与实际飞书发送使用新增[单用户MVP编排](stock_research_mvp.md)。

当前代码已通过隔离原生PG/合成行情及真实固定argv→HTTP调用链。此处记录的是基础入口检查点：原42计划未启用新开关，专用身份默认关闭。单用户MVP的联调及部署状态独立记录，不能由本页基础入口测试推断已送达。

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

late表示观察时刻已过原报告期限。计划预览的`--at`必须带时区，只允许plan；execute使用实际时钟。冻结run保存首次job、事件与绝对期限；量化worker、模型阶段及恢复共同取该期限与原run创建时预算的较小者，不因API重启/重试延长。成功量化、初选及旧报告修订保持。

## 专用身份和API

服务端可选`STOCK_RESEARCH_API_WORKFLOW_TOKEN`至少32字符，须不同于API_TOKEN和API_READ_TOKEN，并配置`STOCK_RESEARCH_PREMARKET_SOURCE_WORKSPACE`。默认空值关闭该身份。独立客户端默认读取`.stock-research/workflow.env`（0600，模板见config/research/workflow.env.example），operator固定argv可用`--identity-file`指定另一专用文件，业务输入不能指定路径；键为WORKFLOW_HOST/PORT/TOKEN；地址仅127.0.0.1，不继承研究Settings、PG、Personal或模型配置。不使用操作员或共享查询身份。

| 方法与路径 | 回执 |
|---|---|
| POST /v1/premarket-jobs | 202；保存原意图，受理、跳过或准备失败均返回job_id及明确状态 |
| GET /v1/premarket-jobs/{job_id} | 200；持久准备回执、当前run及其最新固定报告 |
| POST /v1/premarket-jobs/{job_id}/retry | 202；仅原意图未成功部分，超期/取消/已完成冲突409 |
| POST /v1/premarket-jobs/{job_id}/cancel | 200；未冻结也保留取消标记，已完成研究保持终态 |

专用身份仅可访问这四个动作，不能列举全部任务、管理普通研究、查询共享报告或私人问答、修改参数/持仓/配置。对象、来源或研究workspace不符404，其他功能403，未认证401，跨站/Host校验保持。普通共享只读身份也不能读取盘前job。API_REQUESTS复用原账本记录独立前缀的原事件与run绑定，PREPARATION_ATTEMPTS记录失败；没有新表/迁移，普通CreateRun请求载荷保持。

原事件各自使用请求级PG锁领取回执；恢复与取消统一领取首次意图锁，SQL摘要CAS保留共享取消标记；重放先找原run，冻结后绑定前崩溃也可恢复映射；别名已ready但未绑定时，从已校验的PREPARATION_ATTEMPTS找回原run，不因后来配置变化重新准备。盘前首次准备还按共享快照范围串行，获得执行权后重新查找原run，见[准备互斥](stock_research_preparations.md#并发首次准备)。已取消意图及其别名不能由另一个API拥有者或后来的事件复活。取消已结束的partial任务只封存任务，保留原run/报告/初选，不把有效结果改写成空。恢复先保存pending状态，再排队执行。

## 客户端恢复与收尾

输入限16KiB，文件限runner授予的同一run目录，拒绝软链/越界；输出0600且不覆盖不同内容。HTTP禁代理/重定向，单次响应上限2MB，响应核对原job、计划、run与报告绑定；错误不回显正文/凭据。

准备失败或未确认按60秒、180秒两次退避，始终重放同一原意图。首次POST传输未知也使用该身份重放，不创建新事件。GET观察失败只重读原job，不据此认定终态或重启执行。仅明确临时的provider（含provider_stream_idle_timeout）/transport/quant超时或进程错误可局部恢复，先60秒再180秒间隔，并服从原总期限；结构/证据/配置错误和已耗尽运行预算不重试。截止或SIGTERM时，对已确认任务有界取消/读回；准备重放后仍未确认记录preparation_unconfirmed；无法确认受理或取消时保留admission_unconfirmed/cancel_unconfirmed，不能声称已停止。未知受理不通过新POST猜测取消。

execute可由operator明确启用[有界行情准备](stock_research_market_refresh.md)：PREMARKET_REFRESH默认false，MARKET_REFRESH_TIMEOUT_SECONDS默认90秒；在原准备/事件期限内执行只读PG同步，并仅刷新原已明确启用的三基准补充。准备回执market_refresh持久记录已确认步骤，不将更新完成当作质量通过；数据不足仍失败，不把旧数据当当日新研究。没有报告通知请求或自动投递。下一步统一旧写入/源数据补拉、原报告期限发布与通知稳定业务键/unknown回查，再完成唯一workflow六处登记、cron所有权和正式Mac运行包/网络/自然交易日验收。

研究幂等键按研究workspace、目标日、来源能力/trigger_kind和完整配置版本计算；事件ID、job ID和新事件时刻不进入研究键。每个事件保留自身job/plan/准备回执，`origin`给出首次PremarketBinding（job_id、schedule、execution_deadline）；未冻结则为null。相同配置的新manual事件返回同一run与固定报告，不重算量化或成功初选、不刷新运行期限，原冻结清单不改写。新事件plan的预算仅描述该事件窗口，实际研究仍服从origin和原run预算；客户端不在origin截止后恢复。不同trigger_kind或配置仍创建独立研究。

强制重分析使用operator的POST /v1/research-runs，显式force=true和parent_run_id并提供新的Idempotency-Key；新run保留父链，重放同一请求仍返回原新run，原报告/初选保持。workflow身份不能调用普通创建或指定force/父链，避免新manual事件被隐式当作强制重算。

MVP通知也按manual与scheduled分开正文目录和Personal业务键，同范围重放不替换正文；定点原业务键保持，旧无范围正文停止等待核对。该边界独立于研究run复用和done标记，不能靠完成标记分开就认为通知已隔离，详见[MVP通知范围](stock_research_mvp.md#通知范围与旧记录)。

同配置原run的查找已前移到行情准备之前：先核对实际生效参数、成分/分类、模型和源码配置，再查原冻结身份。别名沿用原清单的成功质量回执，即使当前行情文件暂不可用也不重读；不新增补拉回执、快照或量化，原期限和取消约束照常执行。首次冻结及配置变化仍通过完整质量门槛；盘前并发等待也计入原准备/事件预算，真正输入失败仍保留失败回执及原重放规则。上海11月25日09:55正式API/tick已采用八项CI成功的633固定包，代码与采用证据分别记录。详见[准备回执](stock_research_preparations.md)。

量化进程信号退出或无终帧非零退出已准确映射quant_process_failed，默认研究内部先沿原总期限补算一次，仍失败时才由盘前既有恢复规则处理。未受信号中断的完整非法协议、输出超限及明确计算/源码错误不因此进入临时错误白名单；恢复不另给原事件新的期限。
