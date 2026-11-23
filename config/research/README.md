# 研究版本配置

本目录用于经核对的股票池、分类和后续参数版本。当前没有将来源分支的旧缓存视为正式输入。

本机 `.env` 已指向本目录下 `universe.json`、`classifications.json`；通过 `stock-research import-version` 安装经核对的完整版本，已有不同内容文件不可覆盖。更新时优先写新版本目录，并修改对应的本机路径。

构建格式、质量门槛、历史时点限制与命令见 [数据CLI](../../docs/stock_research_data_cli.md)。不在此目录放置密码、本地数据库或运行报告。
