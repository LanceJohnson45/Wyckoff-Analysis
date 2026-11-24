# 同进程本机研究工作台

专用研究产物由 `stock-research serve` 的同一Python进程提供，HTML、资产和研究API共用实际loopback端口；不需要常驻Vite或Node。沿用既有 `/chat` Agent，公开站点构建及账号流程分别保留。

在 `web/` 按锁文件安装依赖，执行 `pnpm --filter @wyckoff/web build:research`，得到 `web/apps/web/dist-research/`。该构建只载入本机研究组件；打包检查拒绝云端登录、遥控及站点分析模块。发布时把产物复制到固定版本的应用目录，不从Documents开发目录运行后台服务。

在研究服务自己的0600配置中设置：

```dotenv
STOCK_RESEARCH_WEB_DIST_PATH=/absolute/release/web-dist
STOCK_RESEARCH_API_ALLOWED_ORIGINS=["http://127.0.0.1:8788","http://localhost:8788"]
```

`WEB_DIST_PATH`必须为绝对目录，包含专用 `research.html` 与 `assets/`。端口改变时同步调整Origin。未配置时仍为API-only；缺失、源码入口、公开站点产物或目录外符号链接明确拒启。启动命令及原数据库/Bearer身份配置见[研究API](stock_research_api.md)。访问 `http://127.0.0.1:8788/chat`；根路径及 `/chat/` 重定向到此，任意SPA路径不兜底。

`/chat`签发12小时、HttpOnly、SameSite=Strict、host-only、按端口命名的会话cookie，签名密钥只留在进程中，重启轮换。浏览器沿 `/api/stock-research/v1/...` 调用固定白名单；来源、方法、会话及正文上限通过后，在同进程内进入原Bearer鉴权链。直接 `/v1/...` 继续要求Bearer，cookie不能代替。API身份不会写入HTML、浏览器脚本或localStorage。入口设置同源CSP、禁止嵌入、nosniff及no-referrer；HTML/私有响应不缓存，带hash资产可长期缓存。

只有桥接在执行业务前返回的带 `X-Research-Web-Session: required` 的401可触发自动续会话：合并读取一次 `/chat`，确认 `ready` 标记，再以原正文、幂等键、取消信号和总超时重发一次。普通401、网络未知和5xx不触发自动重发。评估、输入维护及问答已结果未知时，后续核对收到任何错误也保留原意图和锁定状态；本次核对被拒绝不能证明早先未受理。确认找到原记录后才解锁。

隔离验收使用临时原生PG、合成行情与真实Python HTTP/worker：无Vite/Node的浏览器入口、固定报告/图表、评估窗口、回执截断、普通401核对、服务重启续会话均已实跑。评估保持3个用户创建、3个结果、180条观察且各一次执行；主题建议只有一条且未发布，问答只有一条。问答验收隔离模型凭据，因此验证受理/恢复及明确失败，未验证模型回答。旧研究/报告/初选/互评/因子/快照/参数发布摘要保持。

这是本机候选能力；正式运行包采用另记。采用前需当前提交全部CI成功、显式初始化所需新表、研究/问答/准备/评估均无活动任务且所属worker退出。实际Tailscale代理的外部来源与身份边界仍待验收；本机HTTP成功不表示远程入口完成。设计与边界见[静态入口设计](design/stock-research-static-web.md)。

本批本地完整Python6,232项通过、无跳过；Web458/API221通过（两个既有Upstash外部条件跳过），锁定安装、TS strict、公开/专用研究/Pages函数构建、wheel资源隔离导入、Ruff/格式/函数质量及workflow/dependency hygiene、同CI假配置daily dry-run通过。首次全量执行在pytest临时目录创建处报错，测试文件单跑及随后完整重跑均通过，未排除测试；首次错误日志保留。当前head远端门禁和正式部署仍分别核对。
