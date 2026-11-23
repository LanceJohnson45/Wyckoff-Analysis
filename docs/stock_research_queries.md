# 固定股票查询与外部只读工具

`GET /v1/stocks/{symbol}/observations?run_id={UUID}&limit=60` 查询指定运行的股票事实。run_id必填，可选report_id固定该修订当时的初选/互评，跨run或workspace的报告拒绝；未指定则读取当前run已提交意见。响应回显report_id或null。symbol为大写规范代码（类别股使用连字符），历史范围1～320。代码必须属于该运行冻结的股票池，不能借此读取任意证券或刷新行情。

响应保留公司名、snapshot_id/hash、实际截止日、两策略产物与参数摘要、是否计算/进入量化池、原标量引用、完整参数/因子定义/Wyckoff有效配置、量化排除、行业与已确认主题事实、截止日日线、已提交初选及交叉意见。未入选不等于无计算，缺计算不补零。history_total指该快照内可用记录数，返回最近limit根。初选与评议对象保留原完整名单及ID；它们不是已按个股裁剪的新初选。行业得分与成员范围继续沿原量化产物。

查询从PostgreSQL读取运行绑定的snapshot与factor_results，并核对每份摘要、策略与快照关系；不读取当前行情、当前参数或模型，不因新代码版本变化重写历史。跨workspace或池外对象404，非法范围422，产物损坏/绑定不一致503。

## 独立查询身份

API进程可配置 `STOCK_RESEARCH_API_READ_TOKEN`，至少32字符且必须与operator的API_TOKEN不同。未配置时不会生成默认只读身份。只读身份仅允许共享研究数据GET，私有chat-turns/人工维护历史/冻结输入来源的GET及所有写入方法403，不能启动、续跑、取消运行或改变配置；既有Host/Origin校验仍有效。身份只保存在受保护的operator配置，不进入URL或研究正文。

外部客户端使用自己的 `.stock-research/query.env`（见config/research/query.env.example，权限0600），包含QUERY_HOST=127.0.0.1、QUERY_PORT与QUERY_TOKEN。该token值与API的READ_TOKEN匹配；客户端不加载根.env、不持有PG、模型或Personal网关配置。也可由operator通过同名环境变量注入。默认不读取 ~/.dsh/secrets，不自动注册到Personal部署。

从研究仓库运行：

```bash
.venv/bin/python -m stock_research.query_cli latest-report
.venv/bin/python -m stock_research.query_cli latest-report --target-session 2026-11-23
.venv/bin/python -m stock_research.query_cli run-status --run-id "$RUN_ID"
.venv/bin/python -m stock_research.query_cli stock --symbol ACGL --run-id "$RUN_ID" --limit 60
.venv/bin/python -m stock_research.query_cli stock --symbol ACGL --run-id "$RUN_ID" --report-id "$REPORT_ID" --limit 60
.venv/bin/python -m stock_research.query_mcp
```

CLI成功输出白名单JSON，失败退出1且stderr不输出HTTP身份或驱动详情。MCP启动时专用配置无效或文件不可用，也只输出固定脱敏错误并退出1，不打印Pydantic异常、配置原值或traceback。Personal使用的query_mcp入口采用stdio并只注册三项带readOnlyHint的工具：stock_research_latest_report、stock_research_stock、stock_research_run_status；不提供执行、shell、文件资源或通用HTTP。客户端固定loopback，禁环境代理与重定向，单次20秒、响应最多2MB。

最新报告可为partial或late，应先固定其run再追问股票。模型须区分量化合格池、正式初选、交叉意见及数据缺失，引用返回的snapshot和ref；这些工具不提供交易许可。

## 接线与验证范围

API、HTTP客户端、只读CLI与stdio工具均已实际调用：本机完整运行f77a7db4-b830-4310-9463-2e97565b9c17通过新进程读回，三项MCP工具各调用一次，股票历史60根及两策略事实与同一快照匹配。临时原生PG测试覆盖原数值、截止日、池外/跨workspace、绑定损坏及全部写方法拒绝；不调用外网或模型。

现有Agent面板仍通过固定报告上下文讨论，尚未把三项工具注册到其模型循环；Personal也未注册这些工具、计划或通知。后续按其六处能力清单接入并独立验收，不能以stdio握手代替生产注册。实际页面操作仍待现有账号登录。

## 研究专用固定报告查询

以下查询以report_id固定修订，回显run、快照ID/摘要、实际截止日和目标日。它们从原产物读取，不因当前发布、行情更新或当前源码变化重算历史。

| HTTP入口 | 内容 |
|---|---|
| GET /v1/reports/{UUID}/market | 原Wyckoff市场标量引用，SPY/QQQ/IWM最后limit根冻结日线，各策略量化池及公司名；limit=1～320 |
| GET /v1/reports/{UUID}/groups | 原Martin行业/主题强度、成员和有效分母；page>=1、page_size=1～100，可按kind及含前缀的name精确过滤 |
| GET /v1/reports/{UUID}/compare | 重复symbols参数，2～8个不同规范代码，同一快照比较；每股最近limit=1～60根日线，默认20 |
| GET /v1/reports/{UUID}/rules | 当次完整参数、Martin因子出处、Wyckoff有效配置和冻结源码/提示词摘要 |
| GET /v1/reports/{UUID} | 原固定报告，供研究客户端重读明确修订 |

组名以industry:或theme:开头，分数降序、组名升序，缺分数NULL排最后。为了校验完整摘要先读取完整量化产物，再将已验证分组JSON作为参数绑定到PostgreSQL；过滤、排序、总数和分页都在SQL中执行，分页不改变原成员/有效分母。低样本或覆盖不足的组保留原null，不补零。姓名/组名不用于SQL标识符。

比较保持请求顺序，所有股票共享一次验证后的修订、快照和策略产物。池外任一代码404，重复/非法代码或范围422，不返回混合成功；不生成跨策略合并得分。quantitative pool是量化复核池，不是正式初选，也不是完整股票池枚举。规则内容中的工程默认值和出处文字不构成收益验证。

研究入口使用 `.venv/bin/python -m stock_research.research_mcp`，与Personal原query_mcp入口分开。它复用同一专用只读配置，在原三项工具外增加stock_research_report_read、stock_research_market_overview、stock_research_group_strength、stock_research_compare_stocks、stock_research_strategy_rules，共八项，全部readOnlyHint，不增加写操作、通用HTTP或文件能力。HTTP客户端逐字段验证并核对报告ID，继续禁代理/重定向、单次20秒和2MB上限。

原生PG/ASGI验证了原产物逐字段对拍、NULL排序、SQL分页、特殊组名、同快照比较、当前参数发布不改旧规则、跨workspace/池外拒绝和损坏绑定503；真实stdio握手验证研究八项及原Personal三项仍分离。实际调用与最终门禁见交接检查点。固定报告问答另以绑定八工具接入独立模型循环，见[研究问答](stock_research_chat.md)；整池浏览另见[股票池查询](stock_research_universe.md)，Personal部署注册仍需接续，不能以查询端点或stdio握手代替生产接线。

本批实际验证：全量Python5983（无skip、50旧警告）、查询/HTTP/源码12通过；Web386、API221/两项默认Upstash跳过及完整Web gate通过。真实report cf8da076修订3的八工具各调用一次，126个原分组、两股比较、三基准5根及规则一致；另验证8股各60根，JSON约371KB，未超过2MB客户端上限。代理读200/跨站403，原报告不变。浏览器和聊天模型循环未据此声明完成，wiki不存在且远端CI/部署未验收。

[完整冻结股票池](stock_research_universe.md)新增GET universe及ResearchReader.universe，SQL字面搜索/分页，保留缺数与初选未提交。绑定问答的market_overview使用view=universe，默认市场模式与独立研究八工具/Personal三工具保持原契约。
