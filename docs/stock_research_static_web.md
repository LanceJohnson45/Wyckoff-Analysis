# 同进程本机研究工作台

专用研究产物由 `stock-research serve` 的同一Python进程提供，HTML、资产和研究API共用实际loopback端口；不需要常驻Vite或Node。沿用既有 `/chat` Agent，公开站点构建及账号流程分别保留。

在 `web/` 按锁文件安装依赖，执行 `pnpm --filter @wyckoff/web build:research`，得到 `web/apps/web/dist-research/`。该构建只载入本机研究组件；打包检查拒绝云端登录、遥控及站点分析模块。发布时把产物复制到固定版本的应用目录，不从Documents开发目录运行后台服务。

在研究服务自己的0600配置中设置：

```dotenv
STOCK_RESEARCH_WEB_DIST_PATH=/absolute/release/web-dist
STOCK_RESEARCH_API_ALLOWED_ORIGINS=["http://127.0.0.1:8788","http://localhost:8788"]
```

`WEB_DIST_PATH`必须为绝对目录，包含专用 `research.html` 与 `assets/`。端口改变时同步调整Origin。未配置时仍为API-only；缺失、源码入口、公开站点产物或目录外符号链接明确拒启。启动命令及原数据库/Bearer身份配置见[研究API](stock_research_api.md)。访问 `http://127.0.0.1:8788/chat`；根路径及 `/chat/` 重定向到此，任意SPA路径不兜底。

### Tailscale Serve候选契约

2026-11-25 23:23补充显式Serve代码路径，尚未用真实远端浏览器验收。operator必须同时配置 `STOCK_RESEARCH_TAILSCALE_SERVE_HOST` 与 `STOCK_RESEARCH_TAILSCALE_SERVE_LOGIN`，并把 `https://<host>` 加入 `API_ALLOWED_ORIGINS`；缺任一项、主机形态无效或HTTPS Origin未白名单都拒绝启动。只用 `tailscale serve` 根路径反代 `127.0.0.1:8788`，不启用Funnel，不开放路径变体或额外端口。

通过Serve的请求必须来自loopback、携带单值Tailscale `X-Forwarded-For/Proto/Host`、`Proto=https`、`Host`与白名单Serve主机一致、`Tailscale-User-Login`等于配置身份，且Origin/Fetch-Site满足同源边界。该远端请求签发`Secure`会话cookie；显式配置Serve后，普通`127.0.0.1`本机入口和HTTP会话继续可用。按Tailscale文档，本机服务只把Host上的其他进程留在本地信任边界内；本配置不放松Bearer、浏览器路由和原工作区身份，也不把这条代码路径当真实入口验收。

`/chat`签发12小时、HttpOnly、SameSite=Strict、host-only、按端口命名的会话cookie，签名密钥只留在进程中，重启轮换。浏览器沿 `/api/stock-research/v1/...` 调用固定白名单；来源、方法、会话及正文上限通过后，在同进程内进入原Bearer鉴权链。直接 `/v1/...` 继续要求Bearer，cookie不能代替。API身份不会写入HTML、浏览器脚本或localStorage。入口设置同源CSP、禁止嵌入、nosniff及no-referrer；HTML/私有响应不缓存，带hash资产可长期缓存。

只有桥接在执行业务前返回的带 `X-Research-Web-Session: required` 的401可触发自动续会话：合并读取一次 `/chat`，确认 `ready` 标记，再以原正文、幂等键、取消信号和总超时重发一次。普通401、网络未知和5xx不触发自动重发。评估、输入维护及问答已结果未知时，后续核对收到任何错误也保留原意图和锁定状态；本次核对被拒绝不能证明早先未受理。确认找到原记录后才解锁。

隔离验收使用临时原生PG、合成行情与真实Python HTTP/worker：无Vite/Node的浏览器入口、固定报告/图表、评估窗口、回执截断、普通401核对、服务重启续会话均已实跑。评估保持3个用户创建、3个结果、180条观察且各一次执行；主题建议只有一条且未发布，问答只有一条。问答验收隔离模型凭据，因此验证受理/恢复及明确失败，未验证模型回答。旧研究/报告/初选/互评/因子/快照/参数发布摘要保持。

2026-11-25上海03:58本机正式API/股票tick已采用冻结`09bdd3cb`，其[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45474002419)全部成功。独立release、显式新表初始化与四类零活动/worker退出保护均已执行；原版本保留可撤回。采用前需当前提交全部CI成功、显式初始化所需新表、研究/问答/准备/评估均无活动任务且所属worker退出。实际Tailscale代理的外部来源与身份边界仍待验收；本机HTTP成功不表示远程入口完成。设计与边界见[静态入口设计](design/stock-research-static-web.md)。

本批本地完整Python6,232项通过、无跳过；Web458/API221通过（两个既有Upstash外部条件跳过），锁定安装、TS strict、公开/专用研究/Pages函数构建、wheel资源隔离导入、Ruff/格式/函数质量及workflow/dependency hygiene、同CI假配置daily dry-run通过。首次全量执行在pytest临时目录创建处报错，测试文件单跑及随后完整重跑均通过，未排除测试；首次错误日志保留。当前head远端门禁和正式部署仍分别核对。

采用后实际HTTP签发会话、Bearer直连及同源评估读取通过，正式评估记录仍0；15次固定业务读全成功、五类摘要不变。隔离合成截图与正式页面证据分开：正式09入口已补518成员搜索、320根图表/锚点、历史修订切换及空评估记录读取，写控件和Tailscale继续另验。部署细节见[单用户MVP](stock_research_mvp.md)。

04:55曾采用6fc65f46，新增只读通知/最近成功记录；该版本八项CI、直接/同源状态读取及四类空闲检查通过，状态区正式刷新/跨日/完整研究跳转与单区域保持已验。06:38继续采用八项CI成功的d42fa219，前端树及专用资产不变，无迁移；旧包保留。正式固定报告追问已得到真实模型回答，3次只读调用/20项声明逐值复核；新包重启后浏览器仍读同一答案，引用展开及控制台无error已验。其余写控件、Tailscale真实来源/身份及自然日仍待验，详见[采用记录](stock_research_mvp.md#2026-11-25通知隔离采用)。

23:23完成Tailscale Serve显式候选代码与隔离PG/ASGI回归：远端契约签发`Secure`会话并继续走同源桥接；坏Host、HTTP转发、非白名单Tailscale身份、跨站Fetch-Site、歧义Host、未同源写请求、配置缺身份和未白名单HTTPS Origin均拒收；配置Serve后本机HTTP入口仍签发普通会话。完整Python6343项通过、无skip及50条既有警告；Ruff/format、函数质量、workflow/dependency hygiene通过。`1802719f`的[八项CI](https://github.com/LanceJohnson45/Wyckoff-Analysis/actions/runs/45648628187)已全部成功并于00:07采用本机候选包；仍未配置Tailscale Serve，真实远端浏览器验收未开始。

07:03已补正式静态入口的真实行情评估创建到完成、独立PG/API/40产物核验、分页/策略/窗口/成本与重载原结果。此项不依赖Vite、不调用模型或通知，其他写控件及阶段六继续分别验收，见[正式评估记录](stock_research_evaluations.md#正式页面与真实行情验收)。


09:55正式API/tick继续采用八项CI成功的633b4dd2，前端树和5个专用资产与d42逐文件相同，无DDL。实际新PID/cwd及直接/同源入口、固定报告/评估读取通过；原27张表小投影和既有问题/结果保持。这是运行包及HTTP核验，没有新增页面浏览器操作；Tailscale入口与其余真实写控件继续独立验收。见[采用记录](stock_research_mvp.md#2026-11-25并发准备采用)。
