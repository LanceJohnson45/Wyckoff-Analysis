# 固定报告完整股票池

既有Agent个股事实区增加股票池搜索与分页。它读取报告当时的全部有效并集，按代码去重，不以量化池或正式初选代替整池；缺行情和缺分类成员仍可打开固定事实。输入代码或公司名称搜索，点击公司将代码载入原个股区域，再可进入该报告的研究问答。切换报告重新初始化搜索/页码，查询不会启动量化、模型、行情同步或配置发布。

GET /v1/reports/{report_id}/universe只读，身份和workspace边界沿固定报告。page为1～1000000，page_size为1～100（Agent固定20），search可选、最多100字符且不能空白/NUL。代码和原公司名大小写不敏感，百分号、下划线及引号均按字面包含匹配。PG执行筛选、按代码稳定排序和分页；universe_total是原有效并集去重总数，total为搜索命中数，页外查询返回空items。

每项包含冻结名称、指数归属、当时行业/已确认主题、实际日线根数及最后日期，两策略各自的data_eligible、calculation_available、in_pool与selected。selected=null表示本修订尚无正式初选；false表示已提交初选没有该股。数据不足、未计算、量化池外与未正式入选各有含义，不将缺数补零因子。原universe/classification版本和数据限制同时返回；缺分类以null和空主题表达，不读取未来生效资料。

最小ResearchReader.universe使用类型化UniverseQuery。绑定问答八工具的market_overview增加view=universe及page/page_size/search，结果仍是本轮ChatToolOutput，原ref_prefix为universe/{symbol}；默认view=market继续返回市场与量化池。目录仍八项，模型不能替换报告。独立research_mcp的原市场工具及Personal三项契约保持原样；整池读取走HTTP客户端或已绑定问答工具。

原生PG/ASGI测试包含双指数去重、过期/未来成员、缺行情/分类、未来分类不泄入、字面搜索、SQL分页、初选null/false、个股逐字段对拍、只读身份、跨workspace和损坏产物拒绝。真实固定cf8da076修订3读取518只，保留17只无行情与15只无行业；Martin数据/实际计算491、量化池30、初选5，Wyckoff490/0/0。此为冻结数据结构核验，不是收益或完整正式成分/PIT验收。

实际客户端、MCP及最终门禁见[交接检查点](stock_research_handoff_2026-11-23.md)。正常登录后的搜索/翻页/打开股票仍另验；手工成分、主题提议与人工确认、锚点叠加、调度和正式部署继续开发。
