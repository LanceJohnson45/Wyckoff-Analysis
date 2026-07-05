# 指标提取规则

本目录下的指标 JSON 统一面向“日线 O/H/L/C/V 数据上的规则引擎”设计，目标是判断“最新一个交易日是否满足某个指标形态”。

## 数据范围

- 仅可依赖日线字段：`open`、`high`、`low`、`close`、`volume`
- 如果原文提到 15 分钟、30 分钟、60 分钟、周线、月线，但可以用日线近似实现，则优先输出日线版本，并记录到 `query/logs/daily_approximation.json`
- 如果无法仅靠日线 `O/H/L/C/V` 计算，必须标记 `computable_from_daily_ocvhl = false`，并记录到 `query/logs/not_computable_from_daily_ocvhl.json`

## JSON 结构

每个指标必须遵循固定结构：

```json
{
  "name": "指标名称",
  "status": "structured",
  "timeframe": "1d",
  "source_pages": [],
  "source": "指标描述",
  "source_summary": "一句话概括原文含义",
  "comment": "需要注意的地方",
  "computable_from_daily_ocvhl": true,
  "required_fields": [],
  "warmup_bars": 0,
  "indicator_type": "pattern_rule / trend_rule / breakout_rule / volume_price_rule / candlestick_rule / other",
  "params": {},
  "series": [],
  "events": [],
  "rules": {
    "latest_match": {}
  },
  "calculation_steps": [],
  "signal_day_definition": "说明哪一天算信号完成日",
  "manual_review_needed": [],
  "implementation_notes": [],
  "explain": "给开发者看的简洁说明"
}
```

## 文件命名

- `query/` 下的指标文件名统一使用英文 slug
- 例如：`jiatu.json`、`liangtuo.json`、`duofangpao.json`
- JSON 内的 `name` 字段仍可保留中文指标名，便于和原文对应
- `extraction_progress.json`、测试脚本、校验脚本中的 `--spec` 参数，都以英文文件名为准

## 原文定位

- 每个指标 JSON 必须包含 `source_pages`
- `source_pages` 使用 PDF 的原始页码数组，例如 `[6, 7, 8]`
- 如果一个规则只来自某一章节中的局部段落，也要尽量把页码范围收紧，不要泛化写整章

## 变形拆分

- 如果原文同一章下包含“标准形态 + 多个变形/强弱分类/位置分类”，优先拆成多个 spec
- 不要把多个语义明显不同的变形硬塞进同一个 `latest_match`
- 推荐做法：
  - 主 spec：标准形态
  - sibling spec：潜伏式、两阳夹两阴、强势版、弱势版等
- 主 spec 的 `comment` / `manual_review_needed` 中要明确说明“当前覆盖了哪些变形、未覆盖哪些变形”

## series / events / rules 分工

- `series`：只放数值序列，例如 `sma`、`ema`、`highest`、`lowest`、`sum`
- `events`：只放“某一天是否发生”的事件，例如 `cross_up`、`cross_down`、`breakout_up`
- `rules`：直接组合条件、序列比较、事件、窗口关系
- 普通布尔条件不要塞进 `events`
- 尽量避免 `custom`，除非确实无法拆成通用算子；优先把逻辑表达成通用规则组合

## 参数化要求

- `params` 中定义的参数必须在 `rules`、`events`、`series`、`warmup_bars` 推导说明里被引用
- 不允许“定义了参数，但规则里写死数值”
- 对原文中的模糊词，例如“明显”“较大”“长期”“短期”“有效突破”“附近”，必须转成参数，并写入 `manual_review_needed`

## 规则表达要求

- 上穿统一用 `cross_up`
  - 前一交易日 `left <= right`
  - 当前交易日 `left > right`
- 下穿统一用 `cross_down`
  - 前一交易日 `left >= right`
  - 当前交易日 `left < right`
- 最近 `N` 日内出现，用 `within`
- 连续 `N` 日满足，用 `consecutive`
- 某段时间内按顺序出现多个事件，用 `sequence`
- 某条件至少出现 `K` 次，用 `count`
- 如果背景必须先于信号发生，显式使用 `before`
- `sequence` 必须定义是否允许同一天发生多个事件，使用 `allow_same_day`

## 背景条件约束

像“长期下跌背景”“空头排列背景”“缩量整理背景”这类条件，不能只写解释，必须写成规则，并尽量绑定在信号事件之前。

例如：

```json
{
  "op": "before",
  "condition": {
    "op": "compare",
    "left": "ma_slow",
    "operator": ">",
    "right": "ma_mid"
  },
  "event": "cross_fast_mid",
  "window_param": "background_lookback",
  "min_count_param": "background_min_bearish_days"
}
```

## warmup_bars

- `warmup_bars` 需要由最长均线窗口、背景窗口、序列窗口、以及交叉所需前一日共同推导
- 同时在 `implementation_notes` 中写出推导逻辑

例如：

```text
warmup_bars >= max(ma_slow_window + sequence_lookback + 1, ma_slow_window + background_lookback + 1)
```

## 日志要求

- 无法提取：`query/logs/extract_failed.json`
- 无法仅靠日线 `O/H/L/C/V` 计算：`query/logs/not_computable_from_daily_ocvhl.json`
- 可提取但公式/参数需要人工补充：`query/logs/manual_formula_review.json`
- 原文为其他周期，当前用日线近似实现：`query/logs/daily_approximation.json`

## 提取后校验

每新增或修改一个指标后，都要顺手跑一遍规则校验，尽早发现字段引用、参数引用、规则歧义、引擎算子缺失等问题。

推荐顺序：

1. 优先跑真实数据校验脚本，直接通过项目现有数据入口拉真实日线数据来验证规则：

```bash
./.venv/bin/python scripts/validate_indicator_specs.py --market cn --code 000001 --spec jiatu
```

如果你希望只使用项目已有缓存、不去在线行情源补拉，可以加 `--cache-only`：

```bash
./.venv/bin/python scripts/validate_indicator_specs.py --market cn --code 000001 --spec jiatu --cache-only
```

2. 跑通用 smoke test，确保所有 `query/*.json` 至少可以被引擎加载并执行：

```bash
./.venv/bin/pytest tests/test_indicator_specs_smoke.py -q
```

3. 跑规则引擎语义测试，确保 `before`、`sequence`、`within` 等关键算子行为没有回归：

```bash
./.venv/bin/pytest tests/test_indicator_rule_engine.py -q
```

4. 对于重要或复杂指标，新增专门测试，用可控样本精确验证信号完成日、latest_match、生效窗口、失效窗口等行为。

说明：

- 真实数据校验脚本优先用于发现“规则能否在项目真实日线数据上运行”的问题。
- pytest 里的语义测试仍然保留，用来稳定保护解释器行为，避免规则引擎回归。

## 当前执行共识

- 目标是服务一个可扩展到 300 个指标的通用规则系统
- 规则必须尽可能无歧义、可解释执行
- 不直接输出 Python 代码，优先输出标准化 JSON 算法描述
