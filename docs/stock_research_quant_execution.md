# 量化执行与局部恢复

API新建研究和CLI run先固定共同快照、参数、源码/引擎、模型偏好及预算，再分别执行Martin与Wyckoff量化。运行、两份pending任务和首份pending报告在同一事务保存，202受理后可读取实际待执行状态；输入ready不代表量化或模型完成。

## 独立任务与固定报告

quant_jobs保存每策略当前状态，quant_attempts保存独立尝试。每次running先提交，再启动worker；成功因子、任务绑定与尝试终态在同一事务提交。失败没有FactorResult，不制造空池、空初选或零因子。某策略失败后另一策略计算及初选继续；互评只由具备成功量化的策略，对本run实际已提交的另一策略初选执行。

成功任务不重算，成功初选不覆盖。显式retry补未成功任务与模型阶段；量化超时/进程中断可再试一次，计算/校验错误不自动重试，均服从原run创建时起的总预算。进程中断遗留running尝试记previous_executor_stopped，持有run锁后以新尝试恢复。取消等候worker退出后保存cancelled；两策略都失败也有可读报告及逐项回执。

RESEARCH_RUNS原冻结payload保持不变，当前summary从已提交job投影成功绑定；publish把当时绑定和量化状态保存到新修订。旧修订不读后来补齐的产物，也不读当前参数/主题/自选。缺计算、量化失败与成功空池分别表达：未知in_pool、分组和计数为null，不能补为false/零。模型配置不可用时仍保留量化事实；真正空池可由程序完成，需要模型的阶段显示执行器错误。

旧运行保留原两份产物；quant状态从原绑定说明成功，id/attempt_count为null，不补造独立尝试。当前尝试查询recorded=false不表示从未失败。源码/提示词变化仍要求force新运行；冻结时模型配置错误不能靠后来配置默默替换，修复后创建新版本。

同内容快照由PG按workspace归并，UUID被另一workspace使用时另建本workspace身份。内部预计算入口只对新产物副本重绑canonical snapshot_id并重算摘要，不修改原对象或旧报告。缓存v2包含快照UUID及内容摘要，避免同内容的不同存储身份串用；共同输入先固定后量化，worker始终使用该canonical ID。

## 进程监督

独立Python worker直接调用原compute_martin和compute_wyckoff，不复制信号算法。同步analyze命令仍在前台直接运行，复用缓存身份与校验。父进程发送固定快照路径/ID/摘要、源码摘要和完整冻结参数；补算不读取后来修改的参数文件。

worker在0700临时目录启动，显式环境白名单不继承PG、行情、API、模型或HOME配置；实际导入文件读取审计未见.env/.dsh身份文件，不实例化Settings、不读DB或行情/模型网络。每worker默认120秒且不超过run剩余预算；正常取消/超时回收自建进程组并等候终态，必要时SIGKILL。父进程异常退出使控制管道EOF，独立原生读取线程以75退出，避免孤儿计算。

请求128KiB、协议响应64KiB、快照512MiB、报告64MiB。stdout仅有启动和结果两帧，错误为固定代码；临时输出0600。父端确认worker成功终态，并重验报告、快照ID/摘要、参数/引擎和结果摘要后，才原子发布缓存。并发同输入采用校验后的首份完整缓存，损坏不覆盖或回退默认。前后source_guard检查仍生效。

## 查询与Agent

RunView.quant及固定报告包含两策略当时状态、错误、尝试次数和成功绑定。股票/整池/市场/规则/分组/锚点查询都区分不可用与成功空结果。失败策略的持续名单保留最后有效状态、日期和证据，另标not_reviewed及quant_unavailable；不得当作结构失效。成功策略按既有策略状态继续更新。

GET /v1/research-runs/{run_id}/quant-attempts按当前run读取追加历史，page=1～1000000、page_size=1～20，SQL按create_time/UUID倒序稳定分页，独立只读身份可读。它独立于某份旧报告，不能把当前补算结果写回旧修订。ResearchReader.quant_attempts与CLI提供同一查询：

```bash
.venv/bin/python -m stock_research.query_cli quant-attempts --run-id "$RUN_ID" --page 1 --page-size 10
```

既有Agent报告显示每策略量化状态，并可展开当前尝试历史；续跑仍通过原显式动作。固定报告问答八工具不增加维护或重算工具；提示词明确未成功量化不是空池/否定股票，null不是零，run_status后来补齐仅作为当前进展。旧问答不重写。

显式init-schema补建quant_jobs和quant_attempts，schema定义26表。启动API只检查，不自动迁移。任务与最新尝试、每次历史回执、参数/快照绑定均校验；损坏503，不冒充pending或空历史。

## 验证边界

原生PG与实际worker验证局部继续、非空正式初选、双失败、并发CAS、遗留running、取消/补算、成功不可覆盖、当前参数文件缺失仍用冻结值、跨workspace同内容快照、旧修订不变、HTTP身份/SQL分页及损坏拒收。模型阶段仅使用明确受控执行器验证引用和提交，不替代真实模型验收；原真实四阶段报告仍保留。

最终全量门禁、本机隔离工作区实测及未验收项见[交接检查点](stock_research_handoff_2026-11-23.md)。浏览器操作仍需正常账号认证；Personal、行情更新协调、远端CI、wiki、独立整体审查和正式部署分别验收。


盘前有限API创建的run另冻结原job、事件及绝对截止时间。量化worker和模型阶段共同取原run创建时预算与该截止的较小者，恢复不延长；普通历史run没有该字段，继续原规则。临时量化失败可在原期限内补算；取消封存盘前首次意图及全部事件别名，不会覆盖已有成功初选/报告。新manual事件同配置返回首次run和origin期限，不给量化增加预算；operator显式force+parent_run_id才产生新分析。完整调用边界见[盘前入口](stock_research_premarket.md)。阶段证据摘要使用与Pydantic模型相同的期限序列化，UTC的Z/+00:00等价表示不再导致自校验失败；不改变策略数值或交易参数。

量化与[行情更新worker](stock_research_market_refresh.md)共用processes.py的环境白名单、父端控制管道和进程组回收，量化协议、缓存和预算语义保持；原量化取消、硬退出和缓存回归继续通过。
