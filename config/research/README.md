# 研究版本配置

单用户盘前编排的固定路径配置见 `mvp.env.example`，身份仍由各自服务读取；流程、定点发布及飞书状态见[单用户MVP](../../docs/stock_research_mvp.md)。

本目录保存可审阅的研究参数。`baseline-v1.json` 是当前Martin日线工程基线，包含窗口、条件开关、作用方式、权重及出处；未经参数优化或收益验证。操作见 [Martin CLI](../../docs/stock_research_martin_cli.md)。

`wyckoff-us-v1.json` 使用美股生产漏斗默认配置和空覆盖，参数路径由 `STOCK_RESEARCH_WYCKOFF_PARAMETERS_PATH` 指定。未知字段和错误类型拒绝运行；当前金额硬门槛固定0，完整生效配置与水温调参保存到报告，见[Wyckoff CLI](../../docs/stock_research_wyckoff_cli.md)。

股票池和分类通过 `stock-research refresh-versions` 或 `import-version` 生成到独立版本目录；本机 `.env` 当前指向忽略目录 `.stock-research/versions/` 下的固定版本。已有不同内容文件不可覆盖，更新时写新目录并修改本机路径。不要将来源分支旧缓存视为当前认证成分。

构建格式、质量门槛、历史时点限制与命令见 [数据CLI](../../docs/stock_research_data_cli.md)。不在此目录放置密码、本地数据库或运行报告。

后台部署可用同目录两份 `com.wxjer.stock-research-*.plist.example`，替换 `__RELEASE__`、`__STATE__`、`__HOME__`、`__WORKSPACE__` 后安装到LaunchAgents。API常驻，盘前tick每60秒启动检查纽约slot；真实配置和状态目录须在Documents之外。
