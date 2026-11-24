# 长程任务宪法：盘前事件与研究身份（2026-11-24）

## 一、目标

对齐plan第9节：同工作区、市场、目标日、配置版本和trigger_kind的新盘前事件复用首次冻结的研究；原事件各自保留请求/准备回执，运行、期限、成功初选和固定报告不被后来的事件替换。显式强制重分析继续使用operator的force+parent_run_id，不扩大workflow身份。

## 二、范围

仅研究schema、pipeline幂等键、请求账本恢复、盘前服务/客户端恢复、相关原生PG测试与README/GLOSSARY/plan/交接。Personal只同步受影响契约文档，先认领；生产调度、凭据、共享进程和真实主workspace数据不修改。无新表、迁移或依赖。

## 三、依赖文档

AGENTS.md；plan.md第9–11节；docs/stock_research_premarket.md、stock_research_api.md、stock_research_quant_execution.md；stock_research/request_store.py、run_store.py、pipeline.py、premarket_service.py；Personal AGENTS.md及DEVELOPMENT_STANDARD.md。

## 四、强制skill

coding-redline、code-repair-verify-gate；完整目标结束后另session执行long-task-review-gate。

## 五、红线

原绑定仍指首次事件，新增回执明确给出origin。别名事件不能延期、改输入、重算成功初选或绕过原取消标记。重试/取消串行化于原意图锁，跨进程竞争不能假报成功。旧普通API/CLI幂等键保持。显式重分析必须保留父链、原报告和初选。真实模型/通知不调用，原dirty保留。

## 六、验收清单

- [x] 不同manual事件同配置复用同run/报告，独立SQL确认首次绑定、期限和原产物不变；新owner可读。
- [x] 不同trigger_kind/配置分开；并发不同事件只有一个run和一组量化；冻结绑定中断仍可恢复。
- [x] 别名取消后所有事件不可复活，原期限已耗尽时别名不重试；原身份/跨workspace/旧API回归。
- [x] operator真实force+parent产生新run/父链、重放仍原新run，workflow身份仍拒绝普通创建。
- [x] 源码冻结后83专项、6112全量、静态门禁和daily dry-run；两事件真实网关/argv/HTTP/PG读回同一报告，主workspace26表摘要守卫通过；文档同步，Personal镜像另行提交，wiki checkout缺失。

## 七、检查点

提交前重读verify-gate。行情更新协调、通知/唯一workflow六处注册与部署、Mac/TCC、网络/多用户、自然交易日、PIT/效果评估、正常浏览器、远端CI及整体独立审查仍保留，整个plan目标不缩减。
