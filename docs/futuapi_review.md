# Futu API Review

本文只 review `futuapi` 中非交易、非期权相关接口，并结合当前项目的数据需求做筛选。

## 结论

优先接入的接口：

- `request_history_kline`
  - 作用：可作为 A 股个股/指数历史日线兜底源。
  - 适配度：高，但需注意额度。
  - 当前用途：`integrations/data_source.py` 中最后兜底、`scripts/analyze_sector_reversal.py`。
  - 注意：历史 K 线月额度仅 300，不应作为主数据源。

- `get_market_snapshot`
  - 作用：替代单票快照、基础报价、部分静态信息。
  - 适配度：高。
  - 当前用途：可用于盘中补最新价、开高低收、成交量/额、名称、市值。

- `request_trading_days`
  - 作用：替代交易日历。
  - 适配度：高。
  - 当前用途：`integrations/fetch_a_share_csv.py`。

- `get_stock_basicinfo`
  - 作用：替代股票基础清单。
  - 适配度：高。
  - 当前用途：A 股股票列表缓存刷新。

- `get_plate_list`
  - 作用：获取行业/概念/地区板块列表。
  - 适配度：高。
  - 当前用途：行业板块枚举、板块研究入口。

- `get_plate_stock`
  - 作用：获取板块成分股。
  - 适配度：中高。
  - 当前用途：后续可替代部分行业映射和自定义板块筛选。

- `get_owner_plate`
  - 作用：查询个股所属板块。
  - 适配度：中高。
  - 当前用途：可替代部分行业/概念归属逻辑，但需要批量化和缓存策略。

## 可保留为增强项的接口

- `get_market_state`
  - 用途：盘前/盘中/盘后状态判断。
  - 适合接到风控、定时任务、页面状态提示。

- `get_global_state`
  - 用途：一次性查看多市场状态与 OpenD 登录状态。
  - 更适合健康检查，不适合作为主行情源。

- `get_stock_filter`
  - 用途：条件选股。
  - 适合做增强版筛选器，但当前 Wyckoff 漏斗已有自己的筛选逻辑，不建议直接替换核心策略。

- `get_stock_quote`
  - 用途：订阅后的实时报价。
  - 更适合盘中实时页，不适合当前以日线为主的批处理。

- `get_rt_data`
  - 用途：分时数据。
  - 对盘中监控有价值，对当前主流程不是刚需。

- `get_rt_ticker`
  - 用途：逐笔成交。
  - 更适合盘中微观结构分析。

- `get_order_book`
  - 用途：买卖盘。
  - 更适合交易执行/盘口分析，不是当前核心需求。

- `get_capital_flow`
  - 用途：资金流向。
  - 可作为情绪/风格因子补充，但不能直接替代历史 OHLCV。

- `get_capital_distribution`
  - 用途：资金分布。
  - 更偏补充分析，不是主数据入口。

- `get_rehab`
  - 用途：复权因子。
  - 当项目后续需要自定义复权口径时很有价值。

- `get_history_kl_quota`
  - 用途：检查历史 K 线额度。
  - 适合作为生产环境监控项。

## 当前不建议作为主路径的接口

- `get_user_info`
  - 和项目行情需求关系弱。

- `get_user_security_group`
  - 更像用户自选股管理，不是公共行情入口。

- `modify_user_security`
  - 属于用户自选写操作，不建议混入研究流水线。

- `get_price_reminder`
  - 提醒查询，不是历史数据源。

- `set_price_reminder`
  - 写操作，且容易把策略和用户提醒状态耦合。

- `get_ipo_list`
  - 可做新股专题，但不是 Wyckoff 主流程基础依赖。

## 与原 Tushare 需求的映射

- `pro_bar` / `daily` / `index_daily`
  - 替代为 `request_history_kline`
  - 但在本项目里只建议做最后兜底，不建议做默认主路径

- `trade_cal`
  - 替代为 `request_trading_days`

- `stock_basic`
  - 替代为 `get_stock_basicinfo`

- `index_classify` / `sw_daily`
  - 替代思路：`get_plate_list` + 板块代码的 `request_history_kline`
  - 说明：Futu 的板块体系不等于申万行业体系，研究结果会有口径差异

- `daily_basic`
  - 部分替代：`request_history_kline` 自带 `turnover_rate`
  - 缺口：若以后要更多估值/流通字段，需要另外补接口或缓存层

## 本项目里的取舍

- 立即用 Futu 替换：
  - 历史日线
  - A 股指数日线
  - 交易日历
  - 股票基础列表
  - 行业板块研究脚本

- 先不替换：
  - 交易接口
  - 期权接口
  - 盘中逐笔/摆盘类接口
  - 用户自选与提醒类接口

## 风险

- Futu 依赖本地 OpenD，部署环境必须能连接 `127.0.0.1:11111` 或显式配置环境变量。
- 历史 K 线有额度限制，生产环境应补充额度与错误监控。
- Futu 板块口径与申万不完全一致，板块研究结果不能与旧 Tushare 结果直接同比。
