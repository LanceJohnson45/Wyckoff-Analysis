# 研究版本配置

本目录保存可审阅的研究参数。`baseline-v1.json` 是当前Martin日线工程基线，包含窗口、条件开关、作用方式、权重及出处；未经参数优化或收益验证。操作见 [Martin CLI](../../docs/stock_research_martin_cli.md)。

股票池和分类通过 `stock-research refresh-versions` 或 `import-version` 生成到独立版本目录；本机 `.env` 当前指向忽略目录 `.stock-research/versions/` 下的固定版本。已有不同内容文件不可覆盖，更新时写新目录并修改本机路径。不要将来源分支旧缓存视为当前认证成分。

构建格式、质量门槛、历史时点限制与命令见 [数据CLI](../../docs/stock_research_data_cli.md)。不在此目录放置密码、本地数据库或运行报告。
