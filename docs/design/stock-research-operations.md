# 工作台通知状态与最近成功时间

计划§13已有要求，不扩展功能范围。只读状态不发送/重试通知、不触发研究、不接管原42计划。先核对真实Personal账本再接Agent。

- 研究API新增operator私有GET操作状态，按目标美国交易日查询；直接只读/盘前身份拒绝，浏览器共享固定路由清单增加GET。返回小投影，不读取完整快照。
- 最近完整研究与通知最近确认送达分开表达。研究从本workspace未删除completed运行按实际账本更新时间确定；不把partial、准备完成或程序退出0视为完整成功。
- 独立固定status子进程沿既有MVP配置访问Personal的固定workspace/task/window；只加载PG与workspace配置，在REPEATABLE READ/READ ONLY事务查询target initial/recovery及最近sent。不得调用reserve/ready/deliver、飞书发送或读取正文/收件人。账本更新时间明确叫确认更新时间，不冒充原生消息创建时刻。
- 未配置、没有记录、不可访问、preparing/sending/ready/retry/unknown/failed/silent/sent分别表达。sent必须有已确认消息引用；unknown不变成成功，结果矛盾明确unconfirmed。
- 固定配置目录只来自operator `.env`，API正文/查询不能提供路径、SQL、目标身份或服务地址。配置错误脱敏503。
- 使用每API进程小型单次并发合并/短TTL，避免刷新工作台造成Personal进程/DB负担；请求取消/服务关闭等待所属子进程退出。历史target不能造成无限缓存。
- 现有Agent运行区展示当前目标日initial/recovery、最新完整研究及已确认通知时间；与历史报告修订分开标注，旧API404明确不支持，不显示伪造空状态。

行动前验证清单：

- [x] 临时原生PG真实SQL和状态投影，不采用共享或生产库写测试。
- [x] 缺配置、无记录、unknown、部分成功、相同更新时间排序、跨workspace/任务/日期边界。
- [x] 固定status worker走真实子进程；证明READ ONLY事务，禁止发送函数，取消/超时实际回收。
- [x] API身份、参数验证、正文/收件人/凭据/路径不出响应；同源路由与前端契约对齐。
- [x] 并发合并、TTL/失效/上游失败、单目标缓存界限，不读大快照。
- [x] 实际Agent控件/刷新/目标日切换与旧服务，生产固定业务/通知记录保持。
- [ ] 完整必要门禁、文档/独立wiki、当前head成功CI与部署分别记录。

实现使用本研究解释器和现有SQLAlchemy/asyncpg读取既有Personal地址；地址只在状态子进程中加载，保留DSN的SSL语义，不引入Personal业务模块或新依赖。前端日期切换实测发现新增组件与准备回执重复key而残留旧区域，已去掉无状态组件多余key；以实际页面区域数由3变1复验。详见[运行契约](../stock_research_operations.md)。

本批本地6250项Python/无skip，44项相关后端聚焦，Web465/API221及两个既有外部Upstash条件skip通过；锁定安装/TS/公开与专用研究/Pages函数构建、Ruff/格式/quality、workflow/dependency hygiene、daily dry-run通过。首次全量因ENOSPC写证据失败；回收可重建依赖缓存后完整重跑，未排除测试。独立wiki已另提交6ff6afb；当前head CI和部署分别核对。
