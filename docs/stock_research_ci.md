# 研究分支完整CI

`codex/stock-research` 与当前fork的main有较长的独立历史。该开发分支的每次push及以它为base的PR，由Stock Research CI调用同一仓库、同一提交的ci.yml，保留主线验证实现，避免另写缩减版研究检查。续接开发可发布为codex/stock-research-runtime评审分支，以原codex/stock-research为base；本地开发分支保持。调用显式要求完整Web门禁；普通main/PR入口保持原条件。

标准CI继续负责Ruff/格式、函数质量、workflow/dependency hygiene、全量Python与coverage产物、合成daily dry-run，以及TS strict/Web/API测试和Pages函数构建。push不是PR，PR-policy job保持其既有事件条件；正式PR仍须满足其Summary/Validation和文件策略，分支检查不替代合入验收。

调用者只授予contents/pull-requests读取权限，不传递生产secrets，没有定时任务、模型调用或部署步骤。分支并发组取消已过期的前一轮检查；A股漏斗/Step4共享组保持。已有部署监听限定main，研究分支检查不启用生产计划、通知或部署。

本地成功不是远端结果。核对对应head SHA的实际Actions运行和各job终态；queued/in_progress/失败或未覆盖必要范围的skip均不能当成发布门禁已通过。独立Wiki解释分支隔离目的，具体验证实现以ci.yml为准。

Web门禁同时构建公开站点与`build:research`专用研究产物，并继续构建Pages函数。专用构建会拒绝云端登录、遥控和站点分析依赖进入本机运行包；标准Python构建把浏览器白名单JSON作为包资源。构建检查不替代实际同进程HTTP、会话及原提交核对的页面验收，详见[静态入口](stock_research_static_web.md)。
