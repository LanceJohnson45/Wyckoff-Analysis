# US行情维护与研究复制协调

本页描述从wxjer@b5cb2d79建立的源端候选分支。源码接线、测试、远端门禁和生产采用是四项不同状态；未合入wxjer前，既有云端任务继续旧代码，不能宣称已经互斥或补齐缺口。

## 数据库协议

同一PostgreSQL数据库内使用固定bigint协议键，不按分支、目标日期或连接别名拆分：`int.from_bytes(b"WYKUS01", "big")`为US任务锁，`int.from_bytes(b"WYKDAT1", "big")`为行情数据锁。维护CLI用单独事务持有独占任务锁，内部并行写连接用共享数据事务锁；研究复制独占两把锁并保持原只读、可重复读事务。两把锁分离，避免更新父任务持锁而内部写事务与自己死锁。锁使用try形式，繁忙立即失败，重试保持原目标日；事务提交、回滚或连接关闭自动释放。

已有US sp500 maintenance、yfinance backfill、funnel prewarm及基准刷新可执行入口参与整项任务锁；CN/HK与dry-run/help入口不取得US锁。`upsert_rows(stock_hist_cache)`中包含US:/INDEX:US:键的批次、对应stock_cache PG裁剪，以及会涉及US行的全表清理/db_maintenance PG行情删除参与数据事务锁。CN/HK单市场写入及本机复制不取得US锁，避免US研究复制影响其日常写入。旧本地PG复制也参与协议。直接调用导入函数只有其实际写事务保护，不代表整项任务加锁；组合更新须使用us_market_job。原始SQL、PostgREST和未升级客户端不自动参与，部署前须核对实际写入入口。不存在“已知代码有锁，所以所有外部写入也受锁”的保证。

任务锁不把多票写入合并为一个原子事务。已经提交的股票不会因后续失败被撤销；复制失败不推进本机水位，恢复按原目标日重新检查。正式研究仍通过固定快照的完整日、320根历史、OHLCV、分类和覆盖质量门槛。传输或源端更新完成不能代替质量验收。

## 固定研究池更新

```bash
.venv/bin/python scripts/us_research_refresh.py \
  --as-of 2026-11-20 --symbols AAPL,MSFT --trading-days 360
```

这是受信operator入口，使用源端已有PG写身份；不会把该身份交给研究API/worker。股票列表必须显式提供，规范化去重后保存数量/摘要，不回落到当前S&P名单。目标日必须已经收盘且是US交易日，不自动滚到今天或其他日期。复用现有us_sp500_maintenance批量前复权历史下载/写入和us_market_target的AAPL/SPY就绪检查，再将SPY/QQQ/IWM写入三个`INDEX:US:*`键，和同名`US:*`股票分开。

多票下载没有ticker标签时拒收，不能把一份历史复制给多只股票；数据按固定请求窗口裁剪。源端维护日历仍为既有US近似日历；研究侧继续正式交易所日历，不将近似日历升级为PIT或正式成分证据。最终独立查询目标日有效股票，任一下载/写入缺失、目标日缺失或基准不足320根返回incomplete/退出2。成功退出0仅表示更新步骤，`quality_checked=false`。异常只在最终回执给类型，不输出连接串；已经提交的部分结果保留，不能将失败推断为零写入。

既有US Daily Bar Refresh新增可选`as_of`及`symbols`，必须同时提供才选择此入口；都不提供时保持S&P刷新。无新增cron。既有US基准CLI一次更新三基准，`benchmark_rows`仍为主基准SPY行数，新增`benchmarks`分别计数；CN函数默认和000300保持。

## Actions与部署

五个既有US写工作流共用market-data-us队列且不取消正在执行的任务，数据库锁另外覆盖本机/跨ref参与者。队列和cron间隔都不代表生产者/消费者的先后；需要同批产物的步骤仍在同一workflow顺序执行。bootstrap及成分sync入口统一PG写入，不再回落Supabase，移除旧凭据片段诊断并用冻结uv环境。未更改既有cron、失败通知或Personal的计划注册。

Market Source CI只读、无生产secrets，安装锁定的历史源分支环境，跑完整源端Python与daily dry-run并保留日志；源分支原有Streamlit测试属于其历史环境，不向main或独立研究运行包加入该runtime。生产采用前需完成两端真实门禁，核对所有写入口及source role，再部署同一锁协议的研究版本。本文不构成部署完成、自然交易日验收或自动补拉接线声明。

## 源分支测试收集修正

原tests/test_futu_client.py并非离线单测，而是连接实际OpenD、订阅行情的手工脚本，其host/port/code参数由命令行main提供而非pytest fixture；默认环境也未声明futu SDK。现原样移到scripts/futu_client_smoke.py保留手工入口，不给回归用例加skip或伪造SDK。全量计数仅为tests/离线套件，不把这个依赖实际OpenD的手工脚本计为已验收；本批没有启动OpenD或访问Futu网络。

研究池补拉先在同一协调锁内检查截止日及所需长度，已提交完整股票窗口不重复下载或重写；三基准先于缺失股票补齐，避免慢股票耗尽预算使基准一直缺失。该缓存检查不代替研究端按正式交易日的质量门禁。
