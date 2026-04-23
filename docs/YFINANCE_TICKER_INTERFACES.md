# yfinance Ticker 接口能力文档

[← 返回 README](../README.md)

## 文档目的

本文基于本仓库的本地 probe 结果，整理 `yfinance.Ticker` 常用接口在 A 股、US、HK 三地股票上的返回形态、可用性差异与接入建议，供后续做数据源评审与项目集成。

对应产物：

- Probe 脚本：[scripts/yfinance_ticker_interfaces_probe.py](/Volumes/E/github/Wyckoff-Analysis/scripts/yfinance_ticker_interfaces_probe.py:47)
- Probe 结果：[data/yfinance_ticker_interfaces_probe.json](/Volumes/E/github/Wyckoff-Analysis/data/yfinance_ticker_interfaces_probe.json)

## 测试基线

本轮文档基于以下一次真实联网 probe：

- `generated_at`: `2026-04-23T15:08:23.911505`
- `yfinance_version`: `1.3.0`
- 样本标的：
  - A 股：`000001.SZ`
  - US：`MSFT`
  - HK：`9988.HK`
- 总调用数：`216`
- 成功返回：`216`
- 调用异常：`0`

说明：

- 这里的“成功返回”表示 Python 调用本身成功，不代表业务上一定“有数据”。
- 部分接口会返回空 `DataFrame`、空 `dict`、空列表，或者对象存在但属性访问报 “No Fund data found”。
- 因为 `yfinance` 底层数据来自 Yahoo Finance，不同市场、不同股票、不同日期的覆盖度会波动。

## 总体结论

按本轮样本看：

| 市场 | 非空接口数 | 空结果接口数 | 结论 |
|---|---:|---:|---|
| A 股 | 54 | 18 | 基础行情、财务、分析师预期可用；SEC、ESG、部分 insider/评级数据偏弱 |
| US | 66 | 6 | 覆盖最完整，最适合做主接入市场 |
| HK | 60 | 12 | 整体可用，介于 A 股与 US 之间；SEC/评级变更/部分拆股数据较弱 |

适合优先接入本项目的能力：

- `history` / `get_history_metadata`
- `info` / `fast_info`
- `dividends` / `actions` / `get_shares_full`
- `income_stmt` / `balance_sheet` / `cashflow` 及季度、TTM 变体
- `calendar` / `earnings_dates`
- `earnings_estimate` / `revenue_estimate` / `earnings_history`
- `eps_trend` / `eps_revisions` / `growth_estimates`
- `major_holders` / `institutional_holders` / `mutualfund_holders`
- `news`

不建议优先作为项目主能力依赖的接口：

- `get_earnings` / `earnings`
- `get_capital_gains` / `capital_gains`
- `get_sustainability` / `sustainability`
- `get_funds_data` / `funds_data`
- `get_sec_filings` / `sec_filings`
- `get_upgrades_downgrades` / `upgrades_downgrades`

## 命名规律

绝大多数接口同时提供两种访问方式：

- `get_xxx()`：方法式
- `xxx`：属性式

在本轮 probe 中，大部分 `get_*` 与属性版返回结果一致，适合在项目中统一选一种风格使用。建议：

- 需要显式传参时，优先用方法，如 `history(period="6mo")`、`get_earnings_dates(limit=12)`。
- 纯只读、无参数接口，项目里统一用属性版更简洁，如 `ticker.info`、`ticker.news`。

## 接口分组说明

### 1. 标识与基础行情

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `get_isin` / `isin` | 标量字符串 | ✓ | ✓ | ✓ | 返回 ISIN，适合做跨市场证券唯一标识 |
| `history` | `DataFrame` | ✓ | ✓ | ✓ | 最核心行情接口，包含 OHLCV、分红、拆股 |
| `get_history_metadata` | `dict` | ✓ | ✓ | ✓ | 包含币种、symbol、exchangeName 等元信息 |
| `get_info` / `info` | `dict` | ✓ | ✓ | ✓ | 信息最全但结构较松散、字段稳定性一般 |
| `get_fast_info` / `fast_info` | `FastInfo` 对象 | ✓ | ✓ | ✓ | 轻量行情摘要对象，适合快速读取现价/市值/成交量 |

典型字段：

- `history`：`Date/Open/High/Low/Close/Adj Close/Volume/Dividends/Stock Splits`
- `get_history_metadata`：`currency`、`symbol`、`exchangeName`
- `fast_info`：`lastPrice`、`marketCap`、`dayHigh`、`dayLow`、`shares`

接入建议：

- 日线/周线/回测：优先 `history`
- 轻量实时摘要：优先 `fast_info`
- 需要更多公司标签：补 `info`

### 2. 公司行为

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `get_dividends` / `dividends` | `Series` | ✓ | ✓ | ✓ | 分红历史 |
| `get_splits` / `splits` | `Series` | ✓ | ✓ | 空 | 拆股历史；HK 样本为空 |
| `get_actions` / `actions` | `DataFrame` | ✓ | ✓ | ✓ | 分红 + 拆股合并视图 |
| `get_capital_gains` / `capital_gains` | `Series` | 空 | 空 | 空 | 普通股票样本均为空，基金场景更有意义 |

接入建议：

- 项目如需做复权校验、股息回测，可接 `actions`
- `capital_gains` 对本项目股票分析场景意义不大，可先不接

### 3. 股本与持仓结构

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `get_shares_full` | `Series` | ✓ | ✓ | ✓ | 时间序列股本，适合估算稀释变化 |
| `get_major_holders` / `major_holders` | `DataFrame` | ✓ | ✓ | ✓ | 主要持有人汇总 |
| `get_institutional_holders` / `institutional_holders` | `DataFrame` | 空 | ✓ | ✓ | 机构持仓，A 股样本为空 |
| `get_mutualfund_holders` / `mutualfund_holders` | `DataFrame` | 空 | ✓ | ✓ | 公募/基金持仓，A 股样本为空 |
| `get_insider_purchases` / `insider_purchases` | `DataFrame` | ✓ | ✓ | ✓ | 最近 6 个月内部人购买摘要 |
| `get_insider_transactions` / `insider_transactions` | `DataFrame` | 空 | ✓ | ✓ | 内部人逐笔交易，A 股样本为空 |
| `get_insider_roster_holders` / `insider_roster_holders` | `DataFrame` | 空 | ✓ | ✓ | 内部人名册，A 股样本为空 |

典型字段：

- `institutional_holders` / `mutualfund_holders`：
  `Date Reported`、`Holder`、`pctHeld`、`Shares`、`Value`、`pctChange`
- `insider_transactions`：
  `Insider`、`Position`、`Transaction`、`Shares`、`Value`

接入建议：

- US / HK 可以考虑纳入“筹码结构”或“机构关注度”模块
- A 股在这组接口明显偏弱，不适合作为统一主链路

### 4. 新闻与公告

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `get_news` / `news` | 列表 | ✓ | ✓ | ✓ | 新闻列表，含标题、摘要、来源、链接、时间 |
| `get_sec_filings` / `sec_filings` | 列表或空 `dict` | 空 | ✓ | 空 | SEC 文件明显偏 US 市场 |

`news` 的单条典型结构包含：

- `title`
- `summary`
- `pubDate`
- `provider.displayName`
- `canonicalUrl.url`

接入建议：

- `news` 很适合给 RAG、防雷、事件提示做轻量补充
- `sec_filings` 可做 US 专项增强，但不适合做跨市场统一接口

### 5. 财务报表

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `get_income_stmt` / `income_stmt` | `DataFrame` | ✓ | ✓ | ✓ | 年度利润表 |
| `quarterly_income_stmt` | `DataFrame` | ✓ | ✓ | ✓ | 季度利润表 |
| `ttm_income_stmt` | `DataFrame` | ✓ | ✓ | ✓ | TTM 利润表 |
| `get_balance_sheet` / `balance_sheet` | `DataFrame` | ✓ | ✓ | ✓ | 年度资产负债表 |
| `get_cashflow` / `cashflow` | `DataFrame` | ✓ | ✓ | ✓ | 年度现金流量表 |
| `quarterly_cashflow` | `DataFrame` | ✓ | ✓ | ✓ | HK 样本可用，但字段明显更少 |
| `ttm_cashflow` | `DataFrame` | ✓ | ✓ | ✓ | HK 样本同样较薄 |

结构特点：

- 行索引是财务科目，如收入、利润、现金流项
- 列是财报日期
- A / US / HK 三地都能拿到较完整年报与季报

接入建议：

- 如果项目要补财务质量、盈利质量、现金流质量，这组接口最值得接
- 使用前建议做一层字段标准化映射，不要直接依赖 Yahoo 原始行名

### 6. 财报日历与分析师预期

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `calendar` | `dict` | ✓ | ✓ | ✓ | 分红日、财报日、预估区间 |
| `get_earnings_dates` / `earnings_dates` | `DataFrame` | ✓ | ✓ | ✓ | 历史/未来财报日期与 Surprise |
| `get_analyst_price_targets` / `analyst_price_targets` | `dict` | ✓ | ✓ | ✓ | 当前、最高、最低、均值目标价 |
| `get_earnings_estimate` / `earnings_estimate` | `DataFrame` | ✓ | ✓ | ✓ | EPS 预期 |
| `get_revenue_estimate` / `revenue_estimate` | `DataFrame` | ✓ | ✓ | ✓ | 营收预期 |
| `get_earnings_history` / `earnings_history` | `DataFrame` | ✓ | ✓ | ✓ | 财报 surprise 历史 |
| `get_eps_trend` / `eps_trend` | `DataFrame` | ✓ | ✓ | ✓ | EPS 预期修正趋势 |
| `get_eps_revisions` / `eps_revisions` | `DataFrame` | ✓ | ✓ | ✓ | 上修/下修统计 |
| `get_growth_estimates` / `growth_estimates` | `DataFrame` | ✓ | ✓ | ✓ | 公司与指数增长趋势 |

这组接口是本轮 probe 里最惊喜的一组：

- 三地样本都拿到了非空结果
- 结构稳定，比较适合直接转成评分因子
- 对“市场预期变化”分析非常有价值

接入建议：

- 非常适合接入项目，用于增强基本面预期维度
- 可用于构建：
  - 预期上修因子
  - 收益 surprise 因子
  - 目标价偏离度
  - 财报事件风控

### 7. 评级与卖方观点

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `get_recommendations` / `recommendations` | `DataFrame` | ✓ | ✓ | ✓ | 买入/持有/卖出汇总 |
| `get_recommendations_summary` / `recommendations_summary` | `DataFrame` | ✓ | ✓ | ✓ | 与 `recommendations` 基本同构 |
| `get_upgrades_downgrades` / `upgrades_downgrades` | `DataFrame` | 空 | ✓ | 空 | 升降级历史明显偏 US |

典型字段：

- `recommendations`：`period`、`strongBuy`、`buy`、`hold`、`sell`、`strongSell`
- `upgrades_downgrades`：`GradeDate`、`Firm`、`ToGrade`、`FromGrade`、`Action`

接入建议：

- 跨市场统一时，可只接 `recommendations`
- `upgrades_downgrades` 更适合做 US 市场增强项

### 8. 其他接口

| 接口 | 返回类型 | A | US | HK | 说明 |
|---|---|---|---|---|---|
| `get_earnings` / `earnings` | 空结果 | 空 | 空 | 空 | 对股票样本基本不可用，可视为废弃 |
| `get_sustainability` / `sustainability` | 空结果 | 空 | 空 | 空 | ESG 覆盖不足，不建议依赖 |
| `get_funds_data` / `funds_data` | `FundsData` 对象 | 弱 | 弱 | 弱 | 对普通股票访问对象存在，但属性会报 `No Fund data found` |

说明：

- `get_funds_data` 更像 ETF / Fund 专用接口，不适合本项目普通股票主链路
- `earnings` 在 `yfinance` 中已偏废弃，建议直接改用 `income_stmt`

## 空结果接口清单

### A 股样本为空

- `get_capital_gains` / `capital_gains`
- `get_earnings` / `earnings`
- `get_sec_filings` / `sec_filings`
- `get_upgrades_downgrades` / `upgrades_downgrades`
- `get_sustainability` / `sustainability`
- `get_insider_transactions` / `insider_transactions`
- `get_insider_roster_holders` / `insider_roster_holders`
- `get_institutional_holders` / `institutional_holders`
- `get_mutualfund_holders` / `mutualfund_holders`

### US 样本为空

- `get_capital_gains` / `capital_gains`
- `get_earnings` / `earnings`
- `get_sustainability` / `sustainability`

### HK 样本为空

- `get_splits` / `splits`
- `get_capital_gains` / `capital_gains`
- `get_earnings` / `earnings`
- `get_sec_filings` / `sec_filings`
- `get_upgrades_downgrades` / `upgrades_downgrades`
- `get_sustainability` / `sustainability`

## 项目接入建议

结合本项目现状，建议分三层引入。

### 第一层：值得优先接入

适合直接增强现有选股、诊断、报告能力：

- 行情与基础信息：
  - `history`
  - `get_history_metadata`
  - `fast_info`
  - `info`
- 公司行为：
  - `dividends`
  - `actions`
  - `get_shares_full`
- 财务：
  - `income_stmt`
  - `balance_sheet`
  - `cashflow`
  - `quarterly_income_stmt`
  - `quarterly_cashflow`
  - `ttm_income_stmt`
  - `ttm_cashflow`
- 预期与财报事件：
  - `calendar`
  - `earnings_dates`
  - `earnings_estimate`
  - `revenue_estimate`
  - `earnings_history`
  - `eps_trend`
  - `eps_revisions`
  - `growth_estimates`
- 新闻：
  - `news`

### 第二层：按市场选择性接入

- US / HK：
  - `institutional_holders`
  - `mutualfund_holders`
  - `insider_transactions`
  - `insider_roster_holders`
  - `recommendations`
  - `upgrades_downgrades`（US 优先）
  - `sec_filings`（US 专项）
- A 股：
  - 可以保留 `recommendations`、`analyst_price_targets`
  - 不建议强依赖机构/内部人接口

### 第三层：暂不建议纳入主链路

- `get_earnings` / `earnings`
- `get_capital_gains` / `capital_gains`
- `get_sustainability` / `sustainability`
- `get_funds_data` / `funds_data`

## 实现建议

如果后续把这些接口纳入项目，建议统一做一层适配器，避免业务代码直接依赖 Yahoo 原始结构。

推荐做法：

1. 统一封装到 `integrations/` 下，例如 `integrations/yfinance_ticker_adapter.py`
2. 对返回做标准化：
   - 时间列统一成 `datetime`
   - 财务字段统一英文键名或项目内部中文键名
   - 对空结果与异常返回统一结构
3. 加缓存：
   - `info` / `news` / `holders` 不必每次实时拉取
4. 区分市场能力：
   - US 最完整
   - HK 可用但要容忍部分空值
   - A 股应主要使用“行情 + 财务 + 预期”，少依赖治理/披露类接口

## 一句话结论

如果目标是给本项目增加跨市场基础面与预期类能力，那么 `yfinance.Ticker` 很值得接入，但应聚焦在 `history/info/fast_info/financials/earnings_estimates/news/holders` 这些高价值接口上，而不是试图把所有 `Ticker` 接口都纳入主链路。
