# 腾讯沪深个股日线备用源（2026-10-22）

本次在现有 `fetch_stock_hist` 接入腾讯沪深个股日线。普通沪深个股顺序为 Yahoo → TickFlow → 腾讯 → 东财/AKShare → BaoStock → efinance。已有北交所新浪路径及美港路径保持原逻辑，腾讯不支持北交所时直接跳过，不发送错误市场请求。

## 数据口径

- 腾讯接口只请求不复权日线，按不超过 640 个自然日分段，避免超过单页根数而静默截断；三个公开入口在网络或响应格式异常时依次回退，每次请求超时 8 秒。
- 请求 `qfq` 时，原始 OHLC 除以新浪前复权因子；`hfq` 则乘以后复权因子。按因子生效日匹配，空因子、不覆盖历史、重复因子或非正因子均失败并继续下一数据源。未直接使用腾讯等差复权价格。
- 科创板 688/689 成交量已为股；其余沪深个股由手乘 100 转为股。复权不修改成交量。
- 拒绝重复/越界日期、非有限数值、非正价格、非法 OHLC 关系及负成交量。现有 `required_dates` 再检查必需交易日；不完整仍继续切源。
- 腾讯适配器没有实际成交额及换手率时返回缺失值。现有 `normalize_hist_df` 会沿用项目原有成交额估算逻辑，因此标准化后的 amount 不是腾讯真实成交额，不能据此做精确成交额对账。
- 新开关 `DATA_SOURCE_DISABLE_TENCENT=1` 可关闭；默认启用，已同步 `.env.example`。不用 API Key，也不增加 Python 依赖。

## 验证

Python 3.12.9，项目现有虚拟环境：101 项相关测试通过，保留 4 条现有 NumPy/pandas 日期运算弃用警告。覆盖腾讯解析、复权、单位、跨窗口、错误输入、备用入口、生产取数入口接线、缺失交易日转下一源，以及既有维护、基准、美港路径回归。

只读联网验证：强制跳过 Yahoo 和 TickFlow，通过生产取数入口请求 600519、000001、688981 的 2026-10-09～2026-10-21 前复权日线。三只均使用腾讯源、各 9 根，经过 `normalize_hist_df` 和 `check_kline_quality`，质量检查通过。结果在 `logs/tencent_daily_live_2026-10-22.json`。

复权口径对照：600519、000001 的 2026 年 6 月日线各对照 Yahoo 21 根 OHLC，最大相对误差分别约 `8.01e-8`、`1.10e-7`。这是两个样本窗口的证据，不代表全部证券、除权事件或历史区间均已对账。结果在 `logs/tencent_yahoo_adjustment_comparison_2026-10-22.json`。

复现相关测试：

```sh
.venv/bin/python3.12 -m pytest tests/integrations/test_tencent_daily.py tests/integrations/test_data_source_tickflow_fallback.py tests/test_daily_data_failures.py tests/test_data_source_cn_index.py tests/test_market_data_maintenance.py tests/test_us_funnel_data_guardrails.py -q
```

已接入本机代码，尚未提交、推送或部署，未改写云端行情、执行全市场更新或运行新的 GitHub Action。部署后的全市场成功率及耗时仍需由实际任务确认；本次也未重算既有缓存的复权历史。
