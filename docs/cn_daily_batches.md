# CN 日更分批执行

## 执行方式

CN Daily Market Data 按一份冻结计划分批维护主板及创业板日线，保留既有 ST、上市边界、停牌及人工排除规则。准备作业读取一次证券名单和交易窗口，生成 `cn-daily-plan` 产物，含目标交易日、窗口、交易日列表、完整证券名单及计划指纹。各批次按排序去重后的名单切分，每批最多 400 只；4606 只对应 12 批。

批次以 Actions 矩阵运行：`max-parallel: 1` 保证串行，保留 TickFlow 账号统一限频；`fail-fast: false` 使一批失败后其余批次继续。每批独立使用原有 60/65/70 分钟派发、收尾、外层保护预算，作业上限 90 分钟。整轮 `cancel-in-progress: false`，新定时运行排队，不会取消还在处理的批次。

分批解决单个作业预算不足，不提高上游配额。按每只 6.2 秒估算，400 只约 42 分钟，4606 只约 8 小时，另外还有准备、启动和网络开销；实际以来源诊断日志为准。单批仍可能因大量补历史或网络故障未完成，不能凭分批配置保证全量成功。

## 检查点及重跑

每批检查点为 `data/funnel_prewarm_state.batch-<编号>.json`，缓存 key 包含批次编号、分支、窗口长度、run_id 和 run_attempt。不同批次不会在保存时剪掉对方的 ready 记录；每批复用自己的有效同日检查点，失败、部分补齐和 pending 仍需重验。配置 `FUNNEL_PREWARM_RECENT_GAP_MAX_AGE_DAYS=0` 时也能正确识别 ready 检查点。

优先使用 Actions 的 **Re-run failed jobs**：准备作业及成功批次保留，失败批次继续使用本轮冻结计划；跨午夜不会换成下一目标日。完整重新运行或发起新工作流会重新准备计划和目标日，仍会根据检查点中的目标日和名单验证有效性，不把旧日 ready 当作新日完成。

批次产物为 `cn-daily-batch-<编号>`，包含控制台日志、coverage/review/summary 和 `batch_metadata.json`。同一运行重跑某批时替换该批产物，其他成功批次产物保留。缓存和产物需要在 GitHub 保留期内可用；产物过期会明确报错，不能凭空恢复。

## 全池结果

最后的报告作业下载冻结计划和所有批次产物，验证每批计划指纹、目标日、证券集合、状态及摘要计数。缺少批次产物时，为该批所有证券生成 pending；日期漂移、重复、漏股或摘要不一致时拒绝汇总。仅所有批次完整且无警告时报告成功；仍有失败或未完成时退出 2，通知明确为未完整完成。

全池覆盖、审核清单和通知使用汇总结果，不取最后一批摘要代替全池。指数只在汇总作业更新一次，并显式使用冻结的目标日。准备或批次作业失败也会尝试报告，保留失败通知兜底。

## 本地入口

```bash
uv run --frozen --no-dev python -m scripts.market_data_batches plan \
  --target-date 2026-11-05 --trading-days 320 --batch-size 400 \
  --output logs/cn_daily_plan.json

uv run --frozen --no-dev python scripts/funnel_prewarm.py --market cn \
  --batch-plan logs/cn_daily_plan.json --batch-id 0 --trading-days 320

uv run --frozen --no-dev python -m scripts.market_data_batches merge \
  --plan logs/cn_daily_plan.json --source batch-results --output logs
```

前两条命令会读取真实名单/行情，预热会写入已配置数据库；汇总只读批次产物。`--batch-id` 从 0 开始，不允许同时指定 `--symbols` 或 `--limit` 截掉计划中的股票。

## 验证范围

回归覆盖：4606 只不重不漏分成 12 批、批次入口冻结名单和日期、独立检查点、部分失败后只重跑失败批、缺产物保留 pending、产物计划/日期不一致拒绝汇总，以及计划 CLI 输出与汇总结果文件。测试隔离外部行情与数据库写入，不等同于云端实库取数验证。

发布与实际运行状态以对应提交和 Actions 运行记录为准；开发分支通过测试不代表定时分支已经生效。上线后需核对每批耗时和最终全池覆盖数。

本次开发验证：Python 3.12.9、`uv.lock` 锁定依赖下，上述相关范围共 120 项测试通过（未跳过，保留 4 条存量 NumPy 日期弃用警告）；Python 3.13 下同范围也通过。actionlint v1.7.7、12 段 Shell 的 `bash -n` 及 `git diff --check` 通过。汇总 CLI 已在独立进程实测：完整产物返回 0；缺少一批覆盖表返回 2，并将该批股票列为 pending。未连接真实 PostgreSQL 或启动云端全池维护。
