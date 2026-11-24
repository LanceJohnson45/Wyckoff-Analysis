# 长程任务宪法：盘前有界行情准备（2026-11-24）

## 一、目标

继续完整plan第10/16节，将已有只读PG增量同步及明确启用的三基准补充接入盘前真实准备链。更新回执先持久化，固定原as_of及剩余期限；数据质量通过后才冻结，失败/取消不制造成功研究。

## 二、范围

研究Settings/示例、准备schema/账本、隔离行情worker与监督、RunManager实际消费、CLI及Agent现有准备条目的更新回执/契约/测试和文档。提取已有量化进程的共用隔离与回收边界，保持其行为。Personal本批无代码或运行面变更，受影响契约先认领再同步。无新表、依赖或迁移；根.env、云端行情及共享服务不改。

## 三、依赖

AGENTS.md；plan第4/9–11/16–19节；docs/stock_research_data_cli.md、stock_research_public_data.md、stock_research_premarket.md、stock_research_preparations.md、stock_research_source_guard.md；market_sync/market_writer/benchmarks、quant_execution/quant_worker、run_manager及preparations。

## 四、强制skill

coding-redline、code-repair-verify-gate；完整目标结束后另session整体审查。

## 五、红线

更新默认关闭，由operator明确配置；worker只收到只读PG连接，不继承operator/研究库/模型/Personal身份，不读取.env。凭据只经匿名控制管道，不入argv/回执/文件/日志。冻结目标日不滚动、恢复不延期。原始异常不外泄，完成前确认进程终止；父端死亡不能留下后台写入。仅源事务确认及传输成功后推进水位，后续取消不回退已提交成功，已确认同步与未知后续步骤区分。基准供应商仍须原显式启用、不可变文件和SPY对拍；不自动放宽质量。pytest无外网，PG使用原生隔离簇。

## 六、验收清单

- [x] 真实子进程只读原生PG→隔离SQLite→准备回执独立读回，更新成功后走正式快照/冻结路径。
- [x] 原as_of/来源/源码/进程绑定与有界协议；无身份继承；坏配置/协议/基准失败明确回执，无新run。
- [x] 取消/超时/父端死亡验证PID及进程组回收，已确认同步保留、未知步骤不补零；水位和已有产物不假报完成。
- [x] 原量化路径与既有API/CLI保持；专项、全量、Ruff/format/质量/红线/hygiene/daily dry-run通过。
- [x] 授权云端只读到隔离文件的实测与主workspace只读守卫；README/GLOSSARY/计划/交接及受影响Personal契约同步，独立Wiki按README地址恢复并同步，提交独立，未向上游推送。

## 七、仍属完整目标

本批只接已有授权只读来源，不替代云端补拉或旧写入端的统一互斥。旧来源维护/Actions协调、云端QQQ/IWM及WBD隔离、正式/PIT资料、通知/unknown回查、唯一workflow六处注册/cron单所有者、正式Mac/TCC/网络/多用户、自然交易日、效果记录、正常浏览器、远端CI及整体独立审查继续推进；不能据此宣布全plan完成。

最终本地验证：6125 Python、35专项，Web412/API221及2原Upstash条件跳过；Ruff/1292文件格式、114文件红线、完整质量/hygiene/daily dry-run通过。真实只读传输与三基准/历史快照、原生网关→HTTP→更新worker→固定报告，以及主workspace26表摘要守卫均有独立回执。Personal镜像同批另行提交。

续接验证：为独立研究分支增加复用标准CI的push/PR入口及full_web布尔契约，不传生产secrets、不部署。原fork main与开发历史相差810提交，评审head使用codex/stock-research-runtime，base保留codex/stock-research。该门禁与数据功能分别提交，远端结果按真实run核对。
