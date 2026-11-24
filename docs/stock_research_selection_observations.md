# 固定正式初选的后续观测

`observe-selections`消费已保存报告的正式初选，不重跑量化或模型，也不补拉行情、发送通知、创建订单或发布参数。它与`evaluate`的历史量化池回放分别标记，当前先提供CLI和不可变文件；持久作业/API/Agent展示仍须后续接线，不能据此称完整阶段六完成。

```bash
.venv/bin/python -m stock_research.cli observe-selections \
  --report-id UUID_FROM_SAVED_REPORT \
  --cutoff 2026-11-24
```

使用现有研究PG离散配置、`WORKSPACE`、`LOCAL_MARKET_PATH`与`ARTIFACTS_PATH`，无新配置项。报告必须是当前工作区可读取的UUID4，截止日必须已完成XNYS正式收盘。退出0表示文件观测完成，pending/missing_data不是命令执行失败；不存在的报告、错误身份、坏摘要/文件或未来/非交易日截止返回非零。

## 固定报告与实际提交时刻

同一REPEATABLE READ、READ ONLY事务读取报告、运行及报告已引用的初选。逐项核对workspace、run、snapshot、strategy、载荷摘要、context和初选提交时间。旧partial修订中没有的初选不能从当前run补入，后来互评或修订也不改变原名单。preview是现有只读研究模式，不是合成模型标记；初选保留`source=model/empty_pool`，空池不会伪造模型观点。

观测参考日取不早于报告目标日、且开盘时刻严格晚于初选提交时刻的首个XNYS交易日，保存为每个初选的`reference_session`。若初选在开盘或之后才提交，参考日顺延，不倒填当天开盘，也不把选股前已发生的隔夜涨跌算入后续收益。相同股票被两策略选中时分别保留初选身份与观察窗口。

第1/3/5/10/20日窗口从参考日开盘到第N个正式交易日收盘，第1日包含参考日。股票和SPY均使用同日开盘作分母；净收益扣正式`round_trip_cost_pct(<symbol>.US)`估算，净超额减SPY毛收益。MFE/MAE是参考开盘后窗口内日线高低相对该开盘的价格变化，包含零，未扣费用。百分数以百分点计，不是委托/持仓/成交回测或实盘alpha。

窗口尚未走完时保留pending，不读取截止日后的标签；已成熟但缺股票或SPY任一所需交易日时保留missing_data及具体日期，不跳日寻找替代价格，不用dropna缩小分母。已有有效行但同时存在来源隔离记录的日期仍不可信。无正式初选与明确empty_pool分别保存，不把缺失策略变成成功空名单。

## 文件与读取边界

每次显式CLI在本地SQLite做一次有30秒进度期限的一致备份；这会复制源文件，不是只复制命中的行。随后SQL先按初选证券、SPY和最多两个20交易日窗口过滤，再限最多440行；不同kind、非qfq、池外证券与其他日期不占该观测输入。来源身份、原日线修订/取得时间及隔离原因保存，当前修订行情不能冒充严格时点数据。该期限不是整个CLI的进程硬超时，未来API须独立采用有界worker；GET不执行此备份或计算。

结果位于`ARTIFACTS_PATH/selection-observations/<input_hash>/`：

- `request.json`保存报告/初选身份、日期、源码、来源、成本及输入摘要。
- `input.json`保存上述绑定和本次必要行情/隔离原因。
- `report.json`保存`kind=formal_selection_observations`、`algorithm=selection-forward-open-v1`、固定初选范围及逐股五窗口。

每个文件最多1MiB，完整写入后原子发布，再独立重读校验。同输入重启复用并拒绝损坏产物；后来源修订、成本、截止日或源码变化产生新目录，旧产物不覆盖。研究库只读，无新表、迁移或原报告更新。本批还未将这些文件接入持久作业结果表，HTTP和Agent不因CLI可用而自动具备该能力。

CLI的`missing_strategies`明确报告缺少的策略；`manual_review=pending`始终保留。人工每策略10入选/10排除仍需实际审查，同日重分析和快照修订不能当独立样本。研究结论与比较证据应放GitHub Issue，不提交收益结果到仓库文档。设计和行动前清单见[观测边界](design/stock-research-selection-observations.md)。当前实现提交CI、实际运行包采用与整份计划分别验收。

验收检查点：相关47项、两次完整Python6273项/无skip（后一轮原始无探针命令）、Ruff/格式/函数质量/hygiene及同CI假配置daily dry-run通过。真实既有报告完成实际CLI两次/独立重算，原研究/报告/初选/快照及正式15次读取的五类摘要保持；文件上限另以一次性副本实测，不改原证据。首次全套的并发准备失败日志保留，单跑与两轮完整重跑通过，根因未证实且未宣称修复。当前实现提交CI和运行包采用分别跟踪，正式API/tick仍6fc65f46。

独立wiki已另提交6f6310e，保留在其独立本地仓库；当前实现提交CI另验，不视为已部署。
