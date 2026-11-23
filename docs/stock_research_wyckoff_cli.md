# Wyckoff固定快照量化CLI

`analyze --strategy wyckoff --no-llm` 已接通美股生产漏斗的离线计算部分。与Martin使用同一质量通过快照，保存实际L1～L4、原排序及水温限制，默认最多30只进入模型复核池。[模型初选、互评及运行账本](stock_research_executor.md)已接通预览，[研究API](stock_research_api.md)已接本机预览，自动执行仍待接入。

## 运行与配置

在仓库根目录使用已同步的Python 3.12环境，前置数据配置见[数据CLI](stock_research_data_cli.md)。不需要新增凭证；默认参数文件为 `config/research/wyckoff-us-v1.json`，可用 `STOCK_RESEARCH_WYCKOFF_PARAMETERS_PATH` 指向新版本。不要与Martin的 `STOCK_RESEARCH_PARAMETERS_PATH` 混用。

```bash
# 固定并持久化当前本地行情快照后计算；不自动联网更新行情
.venv/bin/stock-research analyze --market us --as-of 2026-11-20 --strategy wyckoff --no-llm

# 直接重放snapshot命令返回的文件
.venv/bin/stock-research analyze --market us --as-of 2026-11-20 --strategy wyckoff --no-llm --snapshot-file "$SNAPSHOT_FILE"
```

输出JSON摘要包含报告 `path`、快照/参数/内容摘要、参与计算数量和候选池。报告文件保存在忽略的 `.stock-research/analyses/`，重读校验完整摘要；相同快照、参数、引擎源码和NumPy/pandas版本复用原产物，变更则生成新文件，已有不同内容拒绝覆盖。`model_selection=null`，`execution_permission=not_evaluated`。

参数包含version、algorithm、pool_size及overrides。pool_size默认30，可显式研究调整为1～100。默认完整配置来自 `funnel_config_for_market('us')`；覆盖调用已有 `apply_funnel_config_overrides`，之后才按SPY水温调参。报告保存实际生效的完整FunnelConfig。未知字段、非字段属性名、字符串布尔和其他类型错误拒绝执行。当前窗口固定320根，较长快照也仅取最近320根；禁止增加缺失市值/A股板块硬门槛，成交金额硬门槛须为0。

离线适配不继承 `MARKET_FUNNEL_KLINE_COUNT` 或 `MARKET_FUNNEL_MIN_AVG_AMOUNT` 的隐式覆盖；真正传入层计算的窗口与金额门槛固定，研究变化须写入参数文件。其余RuntimeConfig中的网络批量/报价路径不参与本命令。

## 复用边界

正式美股任务入口是 `workflows.market_funnel_job.run_funnel_for_ranked`。研究适配直接复用其中的 `_run_layers`、`_candidate_rows` 和 `_cap_candidates_for_regime`，保留其基准调参、RPS横截面、行业共振、L4触发、原始分数和排序；没有重写Spring/SOS/LPS/EVR等公式。原有生产默认行为保持不变，新增可选diagnostics只返回已计算的层级、实际配置、L2诊断及预点火观察。

输入证券使用固定研究股票池，不执行TickFlow实时全市场报价预筛，也不等于重现线上当天的完整输入。个股仅使用质量层的Wyckoff合格集合，要求连续320日；基准使用独立kind的SPY，行业来自冻结分类。QQQ/IWM仍由快照质量检查，但不替换上游使用的SPY。分类限制和当前池回溯偏差继承到报告，见[公开数据说明](stock_research_public_data.md)。

报告中的 `observations` 保留所有参与计算证券的层级真值；`excluded` 包含数据质量排除、首次未通过层级及原因。L2使用上游实际诊断；L1/L3/L4不额外发明细分原因，记录生产层未通过。`candidates` 为L4正式命中经水温限制后的原排序，池只是其前N只；预点火、趋势观察和水温排除单列在auxiliary中，不用于补足名额。水温CRASH的候选上限为0，研究适配遵守此限制。

快照没有可核验的成交金额，复权价格与成交量兼容性也未确认，因此禁止非零成交金额硬门槛。为保留上游原始结果，候选辅助入场评分仍会按生产逻辑计算close×volume代理；报告显式标记该限制。`candidates/metrics` 内的actionable、action_label、actionable_count及入场等级是上游诊断原值，不能当作研究交易许可或真实成交可行性。市值、财务、新闻及分钟触发也未核验；本阶段不生成BUY。

## 验证

离线测试用合成横盘、下探收复样本真实调用生产入口，逐字段比较候选和指标，并验证非空Spring、分数排序、截断、CRASH空池、逐层排除、非法覆盖、失败质量和CLI子进程。测试不调用外部行情、模型或通知。

真实2026-11-20固定快照已完成490只计算：L1为490、L2为305、L3为47、L4正式命中为0；SPY水温为CRASH，最终空池。该结果只证明本次输入与生产计算路径接通，不能证明策略收益、参数优劣或完整线上任务相同。数值/运行产物保留在忽略目录，不提交研究绩效证据。
