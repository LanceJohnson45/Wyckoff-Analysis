# 双策略研究执行预览

`stock-research run --market us --target-session 2026-11-23 --preview` 使用前一正式交易日的合格快照。可用 `--snapshot-file /absolute/snapshot.json` 固定已有快照。命令计算并冻结Martin与Wyckoff产物，独立初选各最多五只，事务提交后才交叉评议；不会发送通知或创建订单。研究API、持续观察名单、Agent工作台及services调度仍待接通。

```bash
uv sync --frozen --python 3.12 --extra dev --extra research
.venv/bin/stock-research init-schema
.venv/bin/stock-research run --market us --target-session 2026-11-23 --preview --snapshot-file /absolute/snapshot.json
.venv/bin/stock-research read-run --id <run-uuid>
```

退出码0为完整完成，2为partial/failed等未完整状态，1为配置、文件或数据库错误。空量化池和空互评目标由程序记录为成功，`source=empty_pool/empty_target`，不伪造模型原文。非空阶段失败保留错误，不能把量化候选当成正式初选。

当前执行器固定本机实测DSH `0.2.1-alpha.2`，通过SDK JSON-RPC工作。每次尝试建立独立home、工作目录、会话和Agent目录，只注册 `mcp__research__evidence` 与 `mcp__research__history`。真实工具目录与SDK调用事件双重核验；禁用终端、任意文件、联网、SQL、资源枚举和子代理工具。历史来自固定快照且不超过截止日，每阶段最多二十次查询，包括无效查询与一次结构修正；模型自身HTTP连接由受信Harness持有。

配置沿用 `.env.example` 的 `STOCK_RESEARCH_*`。`MODEL_PROFILE_PATH` 与 `MODEL_ENV_PATH` 二选一；前者只从现有profile安全解析静态provider/model及凭据引用，动态JS不执行，凭据文件默认为 `~/.dsh/.credentials.yaml`；后者用于operator已信任的shell文件，受限固定argv读取 `DSH_E2E_MODEL_*`。这两个路径都是部署配置，不能接受聊天或API传入。模型凭据只传给独立子进程，不落入公开配置或报告。`MODELS_BY_ROLE` 可指定 `martin.select`、`martin.review`、`wyckoff.select`、`wyckoff.review`，模型须在同一可信连接注册；默认沿用已选模型。默认输出预算8192、阶段180秒、运行3000秒。

初选上下文只含自身量化池、策略事实及共享历史；互评只含已提交目标名单、本评议策略的事实和共享历史。每股理由、风险、确认条件必须引用固定证据；数值进入claims并逐值核验。验证最多五只、池内去重、context摘要、当前股票引用和不可变target_selection_id。一次JSON结构修正仍失败则终止；这些检查不能证明自由文本推理正确，文字立场仍是模型意见。

运行幂等键包含workspace、目标交易日、代码/生产算法/提示词、参数、股票池与分类版本、模型配置和预算。相同配置续跑读取首次冻结快照，不用后来修订行情替换已成功初选。`--force --parent-run <uuid>` 显式创建新运行。PostgreSQL运行锁保证同一运行只有一个执行者；成功阶段不可覆盖，遗留running阶段在持锁恢复时标记失败，续跑仅补未成功阶段。网络/超时等暂时错误最多再试一次，配额、输出截断、越权或结构错误不自动重试。每次尝试使用新目录；取消和输出超限均回收独立进程组。

新增账本表为strategy_versions、parameter_sets、factor_results、research_runs、stage_runs、strategy_selections、selection_items、cross_reviews、research_reports。`init-schema` 在已有schema上只补建不存在的表，原五张数据表与快照schema_version=1不变；不能用它修改已有列。DDL正本仍为Python schema，不提交一次性SQL。research_reports按revision追加，初选与评议另表保存并校验payload摘要。

本地忽略目录保存context、完整SDK请求/响应/事件、用量和脱敏stderr及摘要，文件权限0600。API凭据字面值在JSON编码前递归脱敏；不要提交这些原始产物。`read-run` 使用独立研究连接重读已提交结果，不启动模型。

本机没有 `wiki_repo_new/`，未声称独立wiki同步或远端CI通过。研究执行验证不代表历史收益、成本后表现、生产通知或自然交易日部署验收。
