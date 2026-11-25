# 研究与通知状态

现有Agent运行区新增小型状态读取，目标美国交易日沿用上方运行表单。最近完整研究、该日首报/恢复通知和最近确认送达记录分别展示；历史报告保持原修订。状态查询不创建研究、续跑或投递任务。

本机当前API/tick为2026-11-26 02:10采用的`doctor-wiring-c0ae327b`；09:55采用的633b4dd2是历史检查点，其精确代码八项CI、包内139项及切换空闲/进程保护通过。新增并发首次准备互斥；当时15次状态读全成功/P95 0.434秒，权限保持401/403/403/422。当前包切换后的15次readyz全成功/P95 12.727ms，doctor在Python 3.11.16返回ready，窗口外tick退出0。以下保留06:38通知采用检查点：24日首报/恢复与25日无记录的15次状态读全成功/P95 0.262秒，权限401/403/403/422；两个已核实定点旧文件只追加scope，原账本/原生消息/正文保持，未发送。采用细节见[MVP](stock_research_mvp.md#2026-11-25并发准备采用)，不以状态读代替真实unknown或自然日。

`GET /v1/operations?target_session=YYYY-MM-DD`为operator私有接口，只接受一个目标日参数。只读工具身份和盘前身份均返回403，缺身份401；无效日期、额外参数或重复目标日422。浏览器同源固定清单只开放此GET，直接接口仍须Bearer。返回沿用code/message/data/correlation_id。

- `latest_completed`：本研究workspace未删除的completed运行，按更新时间、UUID倒序取一条并校验清单摘要；仅返回run_id、target_session、update_time。它覆盖全部日期，partial/failed不计完整成功。查询不读取行情快照。
- `delivery`：未配置状态来源时为null，表示查询未配置，不表示没有发送；配置后返回目标日定点initial/recovery、账本核对时间和全日期最近确认定点送达记录。仅匹配`us:YYYY-MM-DD:initial|recovery`；独立`manual`键不纳入两阶段及最近定点送达，即使其时间更新、更有原生回执。
- 两阶段的state保留not_recorded、preparing、ready、sending、retry、unknown、failed、silent、sent、unconfirmed。没有行才是not_recorded；sent缺有效原生引用转为unconfirmed，其他状态即使有引用也不自动变为sent。正文、收件人、原生消息ID、凭据及路径不出API。
- `ledger_update_time`是Personal账本更新时间，不是原生消息创建时刻。最近确认送达只选择sent且有有效原生引用的固定股票任务记录，按更新时间、window_key倒序确定。

新版本启动仍只检查原研究schema；本批不新增表，也不复制Personal通知账本。`delivery_references`等完整计划存储要求仍独立审查；显示只读状态不能替代送达、unknown恢复或自然交易日验收。

## 配置和只读边界

研究API自己的0600环境配置新增可选绝对路径：

```dotenv
STOCK_RESEARCH_MVP_STATUS_CONFIG_PATH=/absolute/private/mvp.env
STOCK_RESEARCH_PREMARKET_SOURCE_WORKSPACE=operator-personal-workspace
```

第一项引用已安装MVP的配置，第二项使用既有固定Personal来源workspace；它与研究库workspace分别限定，不能由请求覆盖。MVP配置及其中路径沿用[单用户MVP](stock_research_mvp.md)。状态子进程只从固定Personal环境文件提取SA_PG_URL、SA_PERSONAL_WORKSPACE并比对工作区，不加载通知目标、模型或发送组件。

状态进程使用本研究解释器和已有SQLAlchemy/asyncpg依赖。既有Personal PostgreSQL地址交给asyncpg原生DSN解析，包含原SSL设置；SQLAlchemy负责固定投影和参数化查询。连接通过[async_creator](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#sqlalchemy.ext.asyncio.create_async_engine.params.async_creator)建立，[asyncpg连接参数](https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.connect)按原地址处理。没有新增连接密钥或第三方依赖。

两次Personal查询共用REPEATABLE READ、READ ONLY事务：只读固定workspace、stock-research-premarket任务及规范window。父进程只持有MVP配置路径；业务请求没有SQL、文件或服务地址入口。上游错误、坏协议、非零退出、越界或超时返回脱敏503，不能伪装成空账本。未配置是明确的可选能力状态；研究readyz继续检查自身账本，不能解释为Personal可用。

每API进程最多一个状态子进程、一个目标日缓存。成功缓存10秒，失败合并5秒，切日期替换缓存；核对时间保留原值。锁等待和读取合计限15秒，子进程限8秒；取消/关闭须先终止并wait拥有的进程组，必要时KILL。通知查询失败不展示旧的成功状态，也不影响已有报告查询；页面可手动刷新，正常可见页面每30秒核对，失败停止周期读取。旧服务404明确提示不支持。

## 验证范围

原生临时PG测试验证实际只读/可重复读标志、跨workspace/任务/日期隔离、全部状态、缺原生引用、配置缺失、SQL小投影、权限与严格参数；真实子进程验证非零退出、坏协议、越界、超时和停止后回收。并发用例验证同日合并、10/5秒TTL、单目标缓存及关闭中的等候者。

本机另行只读查询了现有Personal账本，原首报与恢复均有sent和原生引用；未调用投递函数。浏览器与完整门禁结果另记，测试不产生收益结论。正式运行包是否采用本批以部署记录为准，不能因源代码有接口就声称已上线。


本批实际浏览器验收使用临时原生PG、真实Python同源服务和状态子进程：刷新、打开最近完整研究、目标日切换、无记录、来源失败隐藏旧成功、恢复为sent、未配置及实际旧09bdd3cb服务提示均已操作。日期切换残留旧区域的复现/修复证据保留；修复后始终单一区域。两份研究、七份报告及初选/互评/因子/快照逐行摘要不变，问答/评估仍0；浏览器全为GET。只有隔离脚本将一条合成恢复记录改成sent，未发送消息，所有所属服务/PG/状态进程已收尾。

正式09bdd3cb入口另补只读页面验收：518成员整池/AAPL搜索、320根图表与锚点切换、历史partial修订与实时completed切换、评估空历史和发布基线读取；未创建正式研究/评估或模型问题。此范围不冒充全部写控件或Tailscale验收。正式通知记录与两个原生消息逐项保持，该浏览器验收时运行包仍09bdd3cb；后续采用另记。

完整本地Python6250通过/无skip，相关后端聚焦44项，Web465/API221及两个既有外部条件skip、锁定安装/TS/公开与研究/Pages构建、fast/操作dry-run通过。首次全套ENOSPC中止日志保留；可重建依赖缓存回收后完整重跑，不以子集代替。当前提交CI和采用分别登记。

本批验收后正式15次业务读取全部成功，P95 0.373秒，五类业务摘要保持；首报/恢复账本和原生消息证明与采用09bdd3cb之前逐项相同。独立wiki已同步本批验证范围，本地提交6ff6afb；当前源码提交CI及运行包采用另记。

实现6fc65f46的[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45483827399)全部成功，正式API/股票tick已于04:55采用固定独立包；无迁移，四类全workspace活动0/worker退出保护通过。15次状态读取全成功（分三组并发5次），P95 0.206秒；24日sent、25日无记录及最近完整研究保持独立，匿名401、只读/盘前403、额外参数422。包内44项测试及真实只读子进程核对通过，通知原生证明与前两基线逐项一致。正式新状态区域的浏览器复验、自然日及整体审查另记；收尾文档提交的CI不借用实现提交结果。

独立wiki的采用记录已另提交79f95e4，继续只保留在其独立本地仓库；本仓库收尾只更新文档，不改变已运行的6fc65f46实现或其固定资产。

正式6fc65f46页面现已补验：刷新、24/25日切换，25日无记录而全日期最近研究/送达仍为24日，历史partial修订11跳转最近完整14并返回24日；始终只有一个状态区域，浏览器无error。只操作读取控件，原研究/报告/初选/快照摘要保持；未新建模型问题、研究、评估或测试通知。此正式页面证据与隔离合成截图分别保留，不替代自然日、Tailscale或完整写控件验收。
