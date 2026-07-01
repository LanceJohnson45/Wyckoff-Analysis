# yfinance 分钟级 K 线测试说明

## 当前状态

yfinance 当前存在速率限制（"Too Many Requests. Rate limited. Try after a while."），
这可能是由于网络环境或 yfinance 服务器端的限制导致的。

## 测试脚本

脚本位置：`tests/test_yfinance_intraday.py`

运行方式：
```bash
python tests/test_yfinance_intraday.py
```

## yfinance 限制说明

### period/interval 组合限制

| period | 可用 interval | 最大范围 |
|--------|--------------|---------|
| 1d | 1m, 2m, 5m, 15m, 30m, 60m, 90m | 最多 7 天 |
| 5d | 1m, 2m, 5m, 15m, 30m, 60m, 90m | 最多 60 天 |
| 1mo | 1m, 2m, 5m, 15m, 30m, 60m, 90m | 最多 5 年 |

### 速率限制

- yfinance 本身没有明确的速率限制文档
- 当前网络环境下存在速率限制（每分钟最多约 2-3 次请求）
- 建议在请求之间添加 30-60 秒的间隔

### 解决方案

1. **等待一段时间后重试** - 速率限制会自动解除
2. **使用更少的数据** - 只拉取最近 10 分钟的数据
3. **使用替代数据源** - 如 akshare、baostock 等

## TickFlow 限制对比

项目中 TickFlow 的日线 K 线限频为 **10/min**（即每分钟最多 10 次请求），
最小间隔为 6.2 秒。

## 当前测试结果

由于 yfinance 速率限制，当前测试无法成功获取分钟级数据。
建议等待 5-10 分钟后重试。

## 相关文件

- `scripts/yfinance_ticker_interfaces_probe.py` - yfinance 接口探针
- `data/yfinance_ticker_interfaces_probe.json` - 探针结果
