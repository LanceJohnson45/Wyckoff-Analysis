# 研究参数版本与发布

参数集创建后不可变。版本名在workspace/策略内唯一，相同名称与相同内容重复注册返回同一参数ID；同名不同内容409，需新版本名。当前支持Martin与Wyckoff原有参数模型，拒绝未知字段、数字/布尔字符串、错误算法和不支持的生产覆盖。版本名1～256字符，不能含NUL。

发布请求必须提供parameter_set_id和expected_revision。每策略的初始修订是0，发布成功追加新修订；并发时只有匹配当前修订的一方成功，过期请求409。发布历史不覆盖旧行。参数本身、历史运行、原初选和当天已采用的观察名单都不会因此重写。create_time为参数产物首次保存时间，维护列表按注册时间稳定排序。

尚无发布记录时继续使用STOCK_RESEARCH_PARAMETERS_PATH与STOCK_RESEARCH_WYCKOFF_PARAMETERS_PATH指向的operator基线文件；发布后使用数据库已发布版本，编辑基线文件不会覆盖已发布配置。数据库不可用或发布记录损坏时明确503，不回退到文件假装成功。API的operator身份可注册/发布，只读身份只可GET。

## 接口

| 入口 | 行为 |
|---|---|
| GET /v1/strategies | 两策略当前实际配置，含来源、修订、参数ID/摘要和完整值 |
| GET /v1/parameter-sets | 已注册版本SQL分页，可按strategy筛选，page_size=1～100 |
| GET /v1/parameter-sets/{UUID} | 不可变参数产物；可读取旧运行已有的参数ID |
| POST /v1/parameter-sets | 注册完整参数版本，成功201，重复复用ID |
| POST /v1/strategies/{strategy}/activate-parameters | CAS发布，成功200，过期修订409 |
| GET /v1/research-runs/{UUID}/parameters | 当次冻结参数和来源，独立于当前发布状态 |

创建请求形如 `{"strategy":"martin","parameters":{"version":"my-research-v2","support_atr":0.5}}`，省略的参数沿该策略模型默认值补齐；不会隐式继承当前已发布版本。需要在当前配置上改动时，先GET并保留完整parameters，只调整明确字段和版本名。发布请求形如 `{"parameter_set_id":"<UUID>","expected_revision":0}`。

这些是研究配置操作，不提供修改正式初选或强行补足五只的入口。参数允许调整不代表已经完成费用后、跨市场阶段或封存样本验证。

## 实际消费

CLI `stock-research run` 与HTTP新建研究均在首次量化前统一解析两策略配置，保存私有不可变参数文件后交给原量化入口。运行清单记录parameter_sources及完整parameters，模型过程不重新读当前配置。发布期间已有研究保留原输入；同一目标日新配置形成新的运行键。

`analyze --no-llm`和`benchmark --profile`继续按operator指定文件进行独立量化/对拍，不自动切到数据库发布版本。新旧来源应按各自命令区分。Stock observations同时返回完整原参数、Martin因子定义和Wyckoff当时的effective_config，供只读CLI/MCP追溯。

旧报告没有发布来源字段时，冻结参数仍从原运行清单读出，来源标为operator_baseline/revision=0；这不表示后来编辑过的文件可改写该报告。未注册的旧参数产物仍可按ID读出，发布前需先注册。

## 存储与验证

显式 `stock-research init-schema`补建parameter_profiles和strategy_parameter_activations，研究schema现有20张表。启动API不会自动迁移。发布事务取数据库锁、验证参数/注册记录/当前修订摘要后追加，参数物化文件权限0600，路径不进入公开响应。

原生PG与ASGI验证重复/冲突、并发CAS、跨workspace/策略、只读身份、SQL分页、损坏记录失败及API/CLI真实消费；基线文件随后改变不会影响已发布配置或旧运行。研究/质量聚焦183项通过。真实隔离workspace使用原基线完成创建201、发布200、旧修订409、独立读取200，数值与原基线完全相同；stocks主workspace发布数仍0，没有改动实际策略参数、启动模型或发送通知。

当前为后台与HTTP接线；既有Agent内的参数维护界面尚未实现，Node开发代理也尚未开放参数写路由。浏览器操作、Personal注册、自然交易日部署、独立wiki和远端CI仍需各自验收。
