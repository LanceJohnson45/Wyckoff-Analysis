# 长程任务宪法：盘前计划时刻与固定入口（2026-11-24）

## 一、目标

继续plan.md第10/11节。首先补上真实direct cron→固定argv→结构化输入的原计划时刻，研究入口按该时刻的纽约日期确定目标交易日，输出日历/期限与剩余预算；延迟执行不改成启动当天。通过真实原生PG网关及子进程输入/输出验收。

## 二、范围

services src/direct_cron.py、src/cron_producer.py及其测试/运行契约/Personal认领证据镜像；研究stock_research/premarket.py及独立CLI、测试、README/GLOSSARY/plan/交接文档。新增显式pass_schedule_context布尔开关，仅direct可启用；旧配置默认不传、原action及幂等保持。纯计划入口不读取研究.env或身份、不发网络/模型/通知，不冒称研究执行或正式调度已接通。

## 三、依赖文档

AGENTS.md、plan.md第10/11/16节；docs/stock_research_personal.md；../openclaw/dsh-superagent/AGENTS.md、docs/DEVELOPMENT_STANDARD.md、docs/NOW.md；../openclaw/dsh-superagent-services/docs/api/run-contract.md、src/direct_cron.py、src/cron_producer.py及src/runs.py的input.json契约。上述路径已读回核对。

## 四、强制skill

coding-redline、long-task-constitution、code-repair-verify-gate；完整项目结束另一个session执行long-task-review-gate。

## 五、红线

先认领，保留他人dirty。只传固定白名单元数据，不传事件正文/外部路径/argv/凭据。上下文与事件时刻/ID核对，非法配置或元数据失败关闭，不回退Agent总线。原42计划/运行配置不启用新开关，不改变secrets/身份/权限/端口/cron所有权，不重启共享宿主/网关，不调用真实模型、云写入或通知。

## 六、验收清单

- [x] 默认direct action字节/语义保持；开关严格布尔且仅direct；启用后原计划UTC时刻、event_id、workspace、schedule_id经真实input.json可读，重放复用同一run。
- [x] 研究入口按纽约原计划日，覆盖夏冬令时、节假日/半日市、延迟跨日、手动触发、早于计划/预算耗尽/08:50停止；不把节假日移到下一交易日。
- [x] 网关原生PG→固定argv→研究计划入口→output.json真实联调；无.env读取/网络/模型/通知或研究写入。
- [x] focused +全量Python/services及红线/格式/质量/工作流/依赖/daily smoke；README/GLOSSARY/计划/交接、Personal证据/待办/三份镜像同步；wiki缺失明确记录。

## 七、检查点

先完成可核验的计划输入再接API有限写身份、数据刷新、唯一workflow六处登记及通知；这些仍属总体plan待办。本批不缩小总体goal，提交前重读verify-gate，不把本机通过当作远端CI/自然交易日或整体审查。
