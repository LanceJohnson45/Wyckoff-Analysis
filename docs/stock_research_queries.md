# 固定股票查询与外部只读工具

`GET /v1/stocks/{symbol}/observations?run_id={UUID}&limit=60` 查询指定运行的股票事实。run_id必填，symbol为大写规范代码（类别股使用连字符），历史范围1～320。代码必须属于该运行冻结的股票池，不能借此读取任意证券或刷新行情。

响应保留公司名、snapshot_id/hash、实际截止日、两策略产物与参数摘要、是否计算/进入量化池、原标量引用、量化排除、行业与已确认主题事实、截止日日线、已提交初选及交叉意见。未入选不等于无计算，缺计算不补零。history_total指该快照内可用记录数，返回最近limit根。初选与评议对象保留原完整名单及ID；它们不是已按个股裁剪的新初选。行业得分与成员范围继续沿原量化产物。

查询从PostgreSQL读取运行绑定的snapshot与factor_results，并核对每份摘要、策略与快照关系；不读取当前行情、当前参数或模型，不因新代码版本变化重写历史。跨workspace或池外对象404，非法范围422，产物损坏/绑定不一致503。

## 独立查询身份

API进程可配置 `STOCK_RESEARCH_API_READ_TOKEN`，至少32字符且必须与operator的API_TOKEN不同。未配置时不会生成默认只读身份。只读身份允许GET，所有其他方法403，不能启动、续跑、取消运行或改变配置；既有Host/Origin校验仍有效。身份只保存在受保护的operator配置，不进入URL或研究正文。

外部客户端使用自己的 `.stock-research/query.env`（见config/research/query.env.example，权限0600），包含QUERY_HOST=127.0.0.1、QUERY_PORT与QUERY_TOKEN。该token值与API的READ_TOKEN匹配；客户端不加载根.env、不持有PG、模型或Personal网关配置。也可由operator通过同名环境变量注入。默认不读取 ~/.dsh/secrets，不自动注册到Personal部署。

从研究仓库运行：

```bash
.venv/bin/python -m stock_research.query_cli latest-report
.venv/bin/python -m stock_research.query_cli latest-report --target-session 2026-11-23
.venv/bin/python -m stock_research.query_cli run-status --run-id "$RUN_ID"
.venv/bin/python -m stock_research.query_cli stock --symbol ACGL --run-id "$RUN_ID" --limit 60
.venv/bin/python -m stock_research.query_mcp
```

CLI成功输出白名单JSON，失败退出1且stderr不输出HTTP身份或驱动详情。MCP使用stdio并只注册三项带readOnlyHint的工具：stock_research_latest_report、stock_research_stock、stock_research_run_status；不提供执行、shell、文件资源或通用HTTP。客户端固定loopback，禁环境代理与重定向，单次20秒、响应最多2MB。

最新报告可为partial或late，应先固定其run再追问股票。模型须区分量化合格池、正式初选、交叉意见及数据缺失，引用返回的snapshot和ref；这些工具不提供交易许可。

## 接线与验证范围

API、HTTP客户端、只读CLI与stdio工具均已实际调用：本机完整运行f77a7db4-b830-4310-9463-2e97565b9c17通过新进程读回，三项MCP工具各调用一次，股票历史60根及两策略事实与同一快照匹配。临时原生PG测试覆盖原数值、截止日、池外/跨workspace、绑定损坏及全部写方法拒绝；不调用外网或模型。

现有Agent面板仍通过固定报告上下文讨论，尚未把三项工具注册到其模型循环；Personal也未注册这些工具、计划或通知。后续按其六处能力清单接入并独立验收，不能以stdio握手代替生产注册。实际页面操作仍待现有账号登录。
