# 行情来源协调协议

单用户MVP已将来源候选883872cb安装为独立后台副本，由MVP子进程使用既有授权PG配置执行云端补拉；API的market_refresh仍只读。早先“无云写入/未自动派发”的工程证明是启用前检查点，不能代表当前MVP配置；旧wxjer Actions及未升级写入器仍未自动采用锁协议。运行与验收状态见[单用户MVP](stock_research_mvp.md)。

研究复制在原来源/范围锁之外，取得数据库范围的US任务与行情数据两把独占事务锁：`int.from_bytes(b"WYKUS01", "big")`、`int.from_bytes(b"WYKDAT1", "big")`。协议不随日期、分支、host别名或角色变化。忙碌时明确失败，保留本机已提交行情且不推进水位；退出事务释放锁。来源连接仍强制只读及可重复读，没有增加写身份或云端DDL。

来源候选工作树从实际生产wxjer@b5cb2d79建立，独立分支codex/stock-research-market-source。其US维护CLI持有独占任务锁；PG缓存US键的upsert/裁剪及可能影响US的全表维护删除使用共享数据事务锁。任务和写事务分开，允许一项任务的多个写连接并行。旧本地同步器使用同一复制协议。既有源worktree dc5fec0e没有被切换或覆盖，候选工作树没有复制根.env。

源端候选还提供受信operator的固定研究池更新，复用原正式下载/写入及AAPL/SPY就绪检查，目标日不滚动，显式股票列表不回落到S&P；三个INDEX:US基准分别写入。既有手动US Daily Bar Refresh可成对接收as_of/symbols，既有US写Actions使用同一不取消队列；未新增cron。详细源端契约以其docs/us_market_coordination.md为准。

## 接线与采用状态

两端源码接线不代表云端已经采用：在源候选合入/部署前，wxjer旧任务、PostgREST、手写SQL及其他未升级客户端不自动参与。研究侧新锁仍只约束参加协议的连接，不能据此声称全体系互斥。源端命令持有任务锁，直接导入函数只受其写事务保护；组合流程须使用源端us_market_job。

源端更新是独立可信运维入口，研究API仍只有现有只读PG_*；盘前refresh worker没有得到源写身份，也尚未自动派发新的Actions补拉。完成报告前仍必须通过固定日、完整历史及覆盖门槛。源更新/传输完成不等于快照质量通过，更不等于模型研究或通知完成。多票更新不是原子大事务，失败时已经提交的个股保留，重试检查原日期。

部署及自然交易日验收、必要时补拉的自动调用/unknown回查、云端QQQ/IWM已补后的增量、WBD源端隔离现状、正式/PIT资料与全plan其他项继续分别验收。此次开发不修改云数据、根.env、共享服务或Personal计划。

原生跨市场边界另外验证：US复制持有数据锁时，US upsert被拒绝，CN:000001的同表upsert仍正常提交；CN/HK单市场复制不取得US协议锁。

源端候选已单独提交[08c38450](https://github.com/LanceJohnson45/Wyckoff-Analysis/commit/08c3845097177cd477cc9a2454c6389dae392eec)；本机原生跨工作树验证的供应商边界使用合成行情，未写云端或主研究库。源端完整tests/为950passed/94条件skip（4真实旧行情、7真实模型、83文档型指标）；原OpenD手工脚本已逐字保留到scripts，不计作离线单测。研究全量6129passed，无skip，原生协调专项12passed。两端daily dry-run、研究完整质量/hygiene、源端6项workflow hygiene及新源模块Ruff/format通过。源端窄输入红线扫描的3条PS-07为目录完整性误报，实际目录分别有31/60/47个py文件；裸dict实际问题已改为TypedDict，无新增豁免。远端CI、源端生产采用和全plan终态仍须分别核对。

源端独立草稿[PR #11](https://github.com/LanceJohnson45/Wyckoff-Analysis/pull/11)以wxjer为base；研究续接继续[PR #10](https://github.com/LanceJohnson45/Wyckoff-Analysis/pull/10)，base仍为原研究分支。两者不是同一部署范围，实际CI按各自head核对。

源候选08c38450的[Market Source CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45310016730)已completed/success，完整源端测试与daily dry-run均在同一job成功。另以现有授权云端连接实际取得两把协议锁，transaction_read_only=on，退出释放、未写数据；股票最新仍20日，云端QQQ/IWM仍0行。该结果验证云端只读协议兼容，不代表源候选已部署或缺口已修复。
