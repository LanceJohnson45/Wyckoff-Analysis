# 长程任务宪法：Personal三项只读股票工具（2026-11-24）

## 一、目标

落实plan.md第11节的Personal最新报告、股票观点和运行状态读取；复用stock_research.query_mcp与独立只读身份，在已有DSH Personal预设中注册。以实际三工具目录和真实报告/股票/运行读回验收，不调用模型、不发送通知。

## 二、范围

Wyckoff的Personal配置预览/安装CLI、配置回归与接入文档；services独立工作树中的operator-config/personal-agent.patch.yml、相关Personal文档/配置验证；Personal运行profile仅对该预设做有备份、写前摘要核对的增量添加。原personal_quotes及个人读写工具不变，股票API只读角色不能读取私有问答或发起研究。既有dirty保留，不修改platform源码、网关业务、行情源、凭据、权限、端口、cron、workflow或通知。

本批MCP查询不是新增workflow.run执行能力，不创建调度项；stock-research-premarket的六处清单留到其完整交付。当前checkout接入为同机开发路径，正式launchd运行包、TCC路径及自然交易日部署仍分别验收。

## 三、依赖文档

| 路径 | 用途 |
|---|---|
| AGENTS.md、plan.md、docs/stock_research_queries.md | 研究契约与最终范围 |
| ../openclaw/dsh-superagent/AGENTS.md、docs/DEVELOPMENT_STANDARD.md | Personal认领、镜像、验证与运行维护 |
| ../openclaw/dsh-superagent/docs/NOW.md | 认领及外部资产登记 |
| ../openclaw/dsh-superagent-services/operator-config/personal-agent.patch.yml | 已有预设配置正本 |

## 四、强制skill

coding-redline及配置/安全/跨项目/测试规则；long-task-constitution；code-repair-verify-gate；整体long-task-review-gate须另一个session。

## 五、红线

先登记认领；profile修改前确认无相关活动、保留0600备份并准备精确撤回。不复制PG/模型/Personal令牌给查询进程，不输出私有配置；用env -i和固定argv启动已有只读MCP，只加载股票专用查询配置。不重写整个运行YAML、不删除现有插件、不绕过认证。涉及secrets/权限/共享端口或停止其他线时遵守Personal明确的人类确认条款。

## 六、验收清单

- [x] 仓库片段与运行预设语义对齐，仅新增三项查询；旧Personal工具保持，进程环境不继承PG/模型/网关身份。
- [x] 实际DSH注册目录与stdio三工具调用，真实固定报告/个股/运行一致；无模型/通知/研究写入。
- [x] services隔离检查及bin/check-sync.sh，文档镜像、根README/GLOSSARY/计划/交接同步；研究源码变更跑完整相应门禁。
- [x] 运行配置写前摘要核对/备份/精确撤回预览、独立运行读取验证；只读验证进程组已退出。
- [ ] Personal真实界面及正式部署另验，不由SDK或HTTP代替。

## 七、检查点

每个提交前重读verify-gate，逐条记录命令与结果。本机通过不等于远端CI或整体独立审查通过；wiki缺失和浏览器认证单独保留，不缩小plan.md总体目标。
