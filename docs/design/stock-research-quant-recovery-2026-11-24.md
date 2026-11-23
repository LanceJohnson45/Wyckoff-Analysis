# 局部量化失败恢复：本批约束

目标继承plan.md第10/14/16节：某策略量化失败时另一策略及其初选继续，失败不冒充空池，显式重试只补失败计算/模型阶段，成功产物及历史报告保持。涉及本仓库stock_research、测试、既有Agent和操作说明；属于既有单页的状态/回执扩展，沿用PDP豁免，不新增页面/角色；不改变策略公式/参数/名单规则，不修改Personal仓库、身份、通知或部署。依赖AGENTS.md、plan.md、stock_research_preparations.md、stock_research_api.md。应用coding-redline、long-task-constitution、code-repair-verify-gate；整体独立审查须另一个session。

实现顺序：先替换已接入API/CLI run的线程量化边界为可限时回收的独立进程；再冻结共同输入并为两策略追加可恢复量化回执；最后接通部分报告、查询、名单保留及Agent的局部失败/补算。首步不视为局部降级完成，不以worker库存代替实际入口接线。

量化worker只收到operator固定快照路径、原摘要和完整冻结参数，不继承PG/行情/API/模型身份，在独立临时工作目录运行原compute_martin/compute_wyckoff。父进程持有输入管道；正常取消/超时回收自建进程组，父进程异常退出时worker在输入EOF后终止。临时报告只有worker终态成功且父端重新校验快照/参数/引擎摘要后，才原子写入既有缓存。半写入/损坏/错误输出不可升级成成功或默认数据；请求/诊断/产物均有尺寸边界。量化源码改变仍受原source_guard约束。

量化账本保存每策略状态、独立尝试和成功产物；快照、参数、源码、引擎和模型偏好固定。补算不能套用当前维护状态、重做成功计算或覆盖初选；旧报告保存当时绑定，不因后来补齐获得未来产物。两策略均失败也可读失败回执；缺计算/量化失败/空合格池分别表达，观察状态保留。不得为失败创建假空产物、模型结论或BUY。

验证清单：

- [x] worker实际运行原两策略、父子双端绑定、缓存复用、错误/限额拒收；既有API/CLI run实际接线。
- [x] 实际子进程取消/超时/输入EOF后终态退出，端点继续可用；环境不继承身份，临时目录清理。
- [x] 原生PG量化幂等/成功不可覆盖、单策略失败继续、双失败回执、运行中断恢复/并发/取消及原成功产物独立读回。
- [x] 固定旧报告与新修订、查询、问答约束、名单保留和Agent接线；浏览器控件正常账号认证另验。
- [x] 全量Python6010、Web406、API221通过及两项默认外部集成跳过；完整Web gate、Ruff/函数/红线/hygiene/dry-run通过。文档同步于本批本地提交，wiki checkout缺失；远端CI/正式部署另验。

隔离工作区使用原真实行情快照，Martin实际worker连续两次超时后Wyckoff成功空池及初选仍可读；重试只补Martin，冻结参数文件移除后仍使用原run参数。原run输入payload与旧报告不变，独立数据库及前端客户端经HTTP代理读回四次尝试。该验证刻意不配置模型，补算后的报告仍为partial/executor_configuration_failed，不代表新的完整模型研究验收。旧主工作区只有历史产物，quant_attempts返回recorded=false，不补造旧尝试；保存的报告内容不变，当前API新增只读legacy量化元数据。

接续架构约束：共同输入先冻结，新增quant_jobs/quant_attempts保存每策略状态和追加尝试；两表由显式init-schema维护。RESEARCH_RUNS原冻结payload不因补算改写，summary从成功job投影当前factor绑定，publish把当时绑定保存到新报告修订。validate_context用当前已提交绑定重建，固定旧报告继续读自己的manifest；legacy有两份产物的运行保持原契约。补算需在原run锁/总deadline内执行，完成job直接复用；失败不生成假FactorResult。缺失策略的股票/整池/市场/规则/分组/锚点和名单投影都须显式表达不可用，不能只改执行端而让读取端崩溃或把缺计算当空池。
