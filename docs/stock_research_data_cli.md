# 独立美股研究数据命令

本页描述 `codex/stock-research` 的数据阶段实现。后续已接通[Martin量化观察](stock_research_martin_cli.md)和[Wyckoff固定快照适配](stock_research_wyckoff_cli.md)，[模型初选与互评预览](stock_research_executor.md)已接通；[研究API](stock_research_api.md)已接通本机预览；定时任务和飞书发送尚未接通。旧同步入口保留在独立来源工作树，配置见[Mac mini 行情接入说明](stock_research_data_bootstrap_2026-11-23.md)。

本机现已启用按Git提交冻结的公开成分及独立供应商基准，并生成通过门槛的快照。新 `refresh-versions`、`fetch-benchmarks` 的来源、配置和实测见[公开成分与基准补充](stock_research_public_data.md)。云端QQQ/IWM仍未补写，当前使用显式供应商文件解决研究数据缺口，保留全部出处限制。

## 本机配置

已准备的本机文件均为 Git 忽略文件，权限 `0600`：

| 文件 | 待填写内容 |
|---|---|
| `/Users/wxjer/Documents/github/Wyckoff-Analysis-source/.env` | 已从用户更新的研究根目录 `.env` 选择性同步 `PG_*` 行情键并成功读取；不包含其他应用密钥 |
| `/Users/wxjer/Documents/github/Wyckoff-Analysis/.env` | 已配置本机独立 `stock_research` 数据库和同名角色；随机密码仅保存在此文件 |

行情 `.env` 中 `PG_DSN` 留空时使用离散配置。两份文件已分别以 `LOCAL_MARKET_DB_PATH` 和 `STOCK_RESEARCH_LOCAL_MARKET_PATH` 指向 `/Users/wxjer/Documents/github/Wyckoff-Analysis/data/local_market.sqlite3`。行情账号和研究状态账号职责不同，不能互换。本机研究角色无superuser/createdb/createrole，研究数据库内仅研究schema可创建对象，public不可创建。权限已用该账号实际查询，不表示已审计本机其他数据库的默认公共授权。

源工作树已在 `dc5fec0e` 建立并同步其独立 Python 3.12 依赖。已实际完成首次复制；后续在来源工作树执行：

```bash
.venv/bin/python scripts/sync_local_market.py --market us --full
# 后续增量同步
.venv/bin/python scripts/sync_local_market.py --market us
```

这只复制云端已有数据。旧同步器自动带入的benchmark只有SPY。新研究分支已提供独立入口，可直接在研究根目录执行：

```bash
.venv/bin/stock-research sync-market --market us
# 显式忽略新范围水位重读
.venv/bin/stock-research sync-market --market us --full
```

新入口复用根目录 `PG_*`，拒绝非空 `PG_DSN`，连接和源事务均为只读；不执行云端补拉或写入。查询含 `INDEX:US:SPY/QQQ/IWM` 三个独立基准键，同名 `US:QQQ` 保留stock身份。新范围 `research:us:all:benchmarks-v1` 不沿用旧SPY-only水位，初次使用会完整复制已有历史。后续按更新时间并重叠一天，包含历史修订；每批原子提交，全部读取成功并退出源事务后才推进水位，取消可重跑。不同云端身份不能混写已有文件，身份摘要算法保留来源工作树的JSON编码约定。

本机首次同步结果为195132行、520只股票、1只SPY基准，最新日期2026-11-20。WBD在2026-10-05的1根异常日线已隔离，同步返回2。只读云端检查未发现 `INDEX:US:QQQ/IWM` 或 `US:QQQ/IWM` 数据，须继续实现源数据补齐。新同步器已经在同一文件运行，未解除异常隔离；研究库已初始化，真实数据快照提交和独立重读已完成。

## 股票池与分类版本

从有来源的两份独立指数 JSON 清单构建版本，每行至少含 `symbol` 和 `name`。人工自选可选；原始供应商代码保存在 aliases 中。行业输入兼容来源分支的 `yahoo_sector_map_cache.json`，保留原取得时间，主题保持未分配。

```bash
.venv/bin/stock-research build-versions \
  --sp500 /ABSOLUTE/PATH/sp500.json \
  --nasdaq100 /ABSOLUTE/PATH/nasdaq100.json \
  --classification-cache /ABSOLUTE/PATH/yahoo_sector_map_cache.json \
  --version reviewed-20261123-v1 \
  --effective-date 2026-11-23 \
  --source-reference reviewed-source-reference \
  --output-dir .stock-research/versions/reviewed-20261123-v1
```

有效日期须由输入来源确定。构建命令不会在线取得或验证指数成分；490–530 / 95–110 的数量范围只是明显截断保护。已保存的同名文件不可覆盖，更新使用新版本和目录，研究报告引用完整版本及摘要。

本机已用保留分支静态清单试跑，候选文件位于 `.stock-research/bootstrap/source-cache-20261123-v1/`，合并得到516个代码、512条缓存分类。**这是旧缓存接线产物，未核验当前成分，不是正式股票池**；未安装到默认 `config/research/` 路径。其生效日为本次导入日，不适用于2026-11-20回放。正式使用需更新/核对成分来源；历史成员或分类不足时保留存活偏差与取得时间限制，不回填成严格历史版本。

对经核对的完整版本执行导入，或将 `.env` 中两个路径指向对应的版本文件：

```bash
.venv/bin/stock-research import-version --kind universe --input /ABSOLUTE/PATH/universe.json
.venv/bin/stock-research import-version --kind classifications --input /ABSOLUTE/PATH/classifications.json
```

## 快照与质量

```bash
.venv/bin/stock-research doctor
# 审阅DDL（仅打印，不访问数据库）
.venv/bin/stock-research schema-ddl
# 不连接研究状态库的显式离线物化
.venv/bin/stock-research snapshot --market us --as-of 2026-11-20 --offline
# 在授权的研究库显式初始化，再提交并独立重读
.venv/bin/stock-research init-schema
.venv/bin/stock-research snapshot --market us --as-of 2026-11-20
.venv/bin/stock-research read-snapshot --id UUID_FROM_OUTPUT
.venv/bin/stock-research read-snapshot --file /ABSOLUTE/PATH/snapshot.json
```

同样可用 `.venv/bin/python -m stock_research.cli`。退出码0表示该命令通过，2表示数据未就绪，1表示命令/配置/存储错误。失败质量报告仍保存；结构化输出的 `persisted` 区分文件物化与 PostgreSQL 提交。`doctor` 检查依赖、交易日历、输入模型、当日源库可读性及研究表可读性，不代表覆盖、源数据新鲜度、API进程或调度验收通过；输出不含 DSN、密码或驱动堆栈。

快照使用 SQLite backup API 固定含 WAL 的一致视图，源库只读。读取窗口为配置的至少320个正式交易日；行情、源修订、隔离信息、完整版本、阈值、质量清单和来源身份摘要均参与内容摘要。操作ID与生成时间不参与摘要。重复输入可重用，历史修订生成新快照，重读会校验摘要、日期与日线唯一性。没有分时数据、拆股事件或供应商单位元数据时，不能据此声称已经验证真实复权算法及所有单位问题；当前只接受来源库明确标记的 `qfq`。

质量门槛为：当日股票日期覆盖至少95%，只有绑定当日证据的停牌才从分母扣除；空分母失败。缺行业、异常OHLCV、日期重复、非交易日日线或源隔离记录排除该股票，具体原因保留。Martin需要连续100个交易日，Wyckoff需要连续320个交易日；缺一根不能以更早的K线凑数。SPY、QQQ、IWM在各自 `kind=benchmark` 下必须覆盖整个读取窗口，任一缺失则整体失败。主题缺失不阻断行业研究。

进程锁约束同运行目录、同市场日期的新同步和快照命令。新同步另在云端持有事务级advisory lock，同来源使用新入口的跨机器任务互斥，源事务结束或取消自动释放。旧来源脚本和Actions尚未参与协议，不声称它们已受锁保护；整个更新体系的协调仍待接入。一致备份保证分析读到固定版本，不能替代更新端的任务协调。

## 研究 PostgreSQL schema

数据阶段以 `stock_research/schema.py` 为DDL正本，通过 `schema-ddl` 导出，不在Git保存SQL副本。导出文本供具有建schema权限的维护身份审阅/执行；已有schema的应用身份可用 `init-schema` 初始化表，不需要数据库CREATE权限。数据阶段创建 `stock_research` 下的universes、universe_members、classification_versions、group_members、market_snapshots；执行阶段另补建[运行账本表](stock_research_executor.md)，保存UUID4、来源全文、有效日期及操作时间。当前采用完整快照JSON保存行情和质量，后续查询性能有需求时再建立额外索引。CLI初始化不自动授予账号权限，不修改行情源或Personal schema；以后表结构变更须维护Python迁移定义，`create_all` 不能升级已有列。

每次保存将版本、成员与快照放在同一事务；workspace/version和workspace/content_hash唯一约束处理并发重试。同名不同内容版本使事务回滚。读取校验 workspace；连接及关闭都有超时。当前CLI是本机受信进程入口；API已有独立操作员、只读及有限盘前身份，见[研究API](stock_research_api.md)。

回滚应用时保留研究schema及历史文件，不运行 DROP、不删除行情源。集成测试使用临时原生PostgreSQL簇和 `_test` 数据库，经专用Unix socket运行；不修改现有服务，不使用SQLite替代研究状态库。缺少 initdb/pg_ctl/createdb 时测试会明确skip，不能将这种结果报告成数据库验收通过。

## Personal 定位

本机平台仓库为 `/Users/wxjer/Documents/github/openclaw/dsh-superagent-platform`。旁边的 `/Users/wxjer/Documents/github/openclaw/dsh-superagent-services` 已存在，读取时分支为 `codex/personal-backlog-20261123`，提交为 `d405f961bf69d8c54a9d00c7e786df7f9c10551a`，backup远端匹配计划的services仓库。此记录只证明checkout定位；没有核验部署进程版本，没有修改、重启或注册任何服务。后续接入须遵循该工作区的认领和同步规则。

默认run现先解析[人工输入发布](stock_research_input_maintenance.md)并按摘要物化股票池/分类，再生成快照；显式--snapshot-file保持原版本。单独snapshot --offline仍只读operator路径，不连接PG读取维护配置。

API/CLI run量化已使用可取消/限时回收的独立worker，完整校验后才发布缓存；同步analyze保持前台执行。共同输入先冻结，单策略失败已在run内独立记录并继续另一策略，补算复用成功产物，详见[量化执行边界](stock_research_quant_execution.md)。

盘前API现可显式启用同一只读同步和已有供应商补充的[有界行情准备](stock_research_market_refresh.md)，默认关闭；独立CLI为refresh-market --as-of。更新、质量和研究状态分别读取，旧云端写入/Actions尚未参与统一互斥。
