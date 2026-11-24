# 长程任务宪法：盘前有限API与真实执行（2026-11-24）

## 一、目标

继续完整plan.md第10/11节，把已有固定argv计划入口接至研究API的实际启动、任务状态/固定报告、局部恢复及取消。专用身份只管理其盘前任务，按原事件和原期限幂等，任务、准备失败及报告均可独立读回；不复用操作员或Personal网关身份。

## 二、范围

研究schemas/premarket、premarket服务/HTTP/客户端/设置、RunManager及pipeline/quant_jobs共同预算、CLI入口、测试和配置示例/文档。已有API_REQUESTS保存盘前原意图与run绑定，PREPARATION_ATTEMPTS保存失败，冻结run可选premarket绑定；旧请求载荷和旧清单保持兼容。不增加表/迁移。只有实际需要的调度消费契约文档涉及services，先认领再改。

## 三、依赖文档

AGENTS.md、plan.md第9–12/16节；docs/stock_research_premarket.md、stock_research_api.md、stock_research_quant_execution.md、stock_research_source_guard.md；stock_research/request_store.py、run_manager.py、pipeline.py、quant_jobs.py；../openclaw/dsh-superagent/AGENTS.md及docs/DEVELOPMENT_STANDARD.md；../openclaw/dsh-superagent-services/docs/api/run-contract.md。

## 四、强制skill

coding-redline（Python/SQL/API/安全/配置/测试/跨项目）、long-task-constitution、code-repair-verify-gate；完整交付另session执行long-task-review-gate。

## 五、红线

只接受既定计划上下文，不接受命令、路径、模型、租户或任意deadline。专用token与操作员/只读身份不同且来源工作区固定，默认关闭；仅允许盘前任务路由，不能管理普通研究或读取私人问答。第一次意图/初选/报告不被重放覆盖，恢复不延长期限；取消/超时回收本次进程组。真实主workspace、凭据和共享Personal进程/端口/调度不修改；不调用真实模型或发通知，原dirty保留。源码冻结后再测试/执行，当前研究API只在零活动守卫后由本线程重启。

## 六、验收清单

- [x] 原生PG/ASGI验证独立身份、来源绑定、对象隔离、未知/普通任务拒绝、既有身份矩阵与跨站保护。
- [x] API原意图/准备失败/固定run持久化；并发/未知POST/进程重建重放复用，旧CreateRun幂等载荷保持；过期/假日有回执不启动。
- [x] 共同原期限实际约束量化和模型阶段，重试不能重置；取消及后台任务收尾验证。
- [x] 固定argv客户端只读专用配置、loopback/无代理重定向/有界报文；真实子进程→HTTP→隔离PG启动/等待/固定报告读回，无真实模型/通知。
- [x] 完整Python6105、focused76、Ruff/1283文件格式/质量/红线108文件/hygiene/daily smoke通过；README/GLOSSARY/计划/交接同步。受影响Personal契约与镜像另行提交，独立wiki checkout缺失。

## 七、检查点

每个提交前重读verify-gate并核对真实证据。行情来源更新协调、通知适配/去重回查、六处清单与唯一cron所有者、正式Mac运行包/网络/多用户、自然交易日、PIT/效果评估、正常浏览器和远端CI/独立整体审查仍属完整目标；不以有限执行接通宣称全计划完成。

本批只保证同一原事件幂等。跨新手动事件的同配置去重、强制重分析及parent_run_id仍须对齐plan第9节，不能据当前事件绑定宣称已经完成。
