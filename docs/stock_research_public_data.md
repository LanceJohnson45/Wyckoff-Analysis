# 公开成分版本与独立基准补充

研究分支的数据阶段现支持实际取数、冻结出处与独立存取。公开成分来自聚合仓库，不能将数据阶段通过解释为指数提供方认证或严格历史时点。[模型执行预览](stock_research_executor.md)已接通；[研究API](stock_research_api.md)已接本机预览，调度尚未接通。

## 成分与行业

```bash
.venv/bin/stock-research refresh-versions \
  --as-of 2026-11-20 --allow-current-universe \
  --version public-20261123-v1 \
  --output-dir .stock-research/versions/public-20261123-v1
```

命令经固定GitHub API查询文件最近提交，再按不可变commit读取完整文件，保存原文、取得时间、提交时间、SHA256、股票池、分类和完成清单。数量、重复代码、字段变化或请求失败会拒收，不回退旧静态缓存；只有全套文件验证完成才发布 `complete.json`。失败不修改生效路径。重新取得资料须使用新版本及新目录，已有内容不可覆盖。

默认来源为 [datasets的S&P清单](https://github.com/datasets/s-and-p-500-companies) 和 [Gary-Strauss的Nasdaq-100清单](https://github.com/Gary-Strauss/NASDAQ100_Constituents)。本机取得的文件提交时间分别为2026-11-05和2026-10-25，版本固定为各自完整提交SHA，不能声称这证明之后没有指数调整。默认拒绝文件修订时间超过35天或来自未来的来源；该门槛可配置，属于工程过期保护，不能证明清单及时性。

必须显式传 `--allow-current-universe`。清单的 `effective_date` 是本次研究适用日，不冒充指数真实调整日；取得时间晚于截止日时，快照继续记录当前池回溯和存活偏差。退选股票不进入新版本，旧版本、原报告及历史行情保留。刷新保留原版本中仍有效的人工自选及其分类，并保留既有主题，不自动提出或确认新主题。

行业只采用标普源明确的GICS细分行业，名称带 `GICS:` 前缀。Nasdaq聚合文件中的分类列语义存在歧义，本实现不采用；仅纳指成员没有可用行业时显示缺失并排除，不与GICS混成同一组。当前实际版本为518股并集、503条分类，未推断主题。

## 基准补充

```bash
.venv/bin/stock-research fetch-benchmarks --as-of 2026-11-20
```

固定yfinance 1.7.0，沿用来源分支的 `auto_adjust=True`、日线、原成交量、`repair=False` 口径，验证美元/ETF元数据和带时区的纽约交易日期。取得SPY、QQQ、IWM完整窗口后，保存SDK返回表、所用元数据、来源参数、规范化日线和全文摘要。`end`为开区间，按[库文档](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html)传截止日下一天。缺根、重复、坏OHLCV、未来日线及响应损坏均拒收。

结果保存为 `.stock-research/benchmarks/<hash>.json`。默认不启用补充文件；将根目录 `.env` 的 `STOCK_RESEARCH_BENCHMARK_PATH` 指向输出路径后才显式使用。原云端同步库保持只读，不将供应商数据写回原SQLite或云端，也不假装它来自云端。

启用前，三基准补充必须与目标截止日及窗口一致，SPY与云端至少重叠100根；逐日比较所有OHLC，默认最大相对偏差0.1%，成交量最大相对偏差1%。冲突直接阻断，不能为了通过而自动放宽阈值。对拍门槛及供应商限制进入快照质量记录，来源复合摘要绑定供应商文件摘要。三个基准整体选用同一补充版本，股票事实继续来自原行情库，不把同名stock记录当benchmark。

`source_revision` 是该适配器的取得版本时间，不能冒充Yahoo的官方修订发布时间。当前取得的复权历史也不等于当时可获得的复权版本。新供应商、其他股票或将来修订仍需独立核验；SPY对拍不能证明所有证券的复权算法和单位已完整验收。

## 本机实测

已取得三基准各320根，共960根，SPY与云端320个同日期OHLC最大相对偏差约 `1.93e-7`，并通过成交量检查。已将生效 `.env` 路径指向不可变的公开成分、分类和基准文件；`doctor`当前为ready，仍明确显示API命令已实现但未由doctor探测、scheduler未接通。

实际2026-11-20快照已提交研究库：518只中501只日期对齐，覆盖96.72%，达到95%的工程门槛；Martin历史可用491只，Wyckoff历史可用490只。28条质量问题仍保留，少数缺数、缺分类、历史不足和原隔离不会被填造或隐藏。这些数量是数据接线和质量检查，不是策略推荐或收益证据。

本批离线研究测试87项通过，无skip，覆盖公开源格式/版本/过期保护、取数失败不发布、人工自选和主题保留、补充文件摘要、时区、日期、价格及成交量冲突、CLI快照读写。真实取数和数据库检查在独立命令中执行，pytest不请求外网。完整pytest、前端与远端CI尚未执行。独立wiki checkout在本机不存在，未宣称已同步。
