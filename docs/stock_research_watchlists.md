# 持续研究观察名单

研究结束时，流水线从冻结的量化产物更新各策略已入选股票的观察状态。状态与事件、运行终态及报告修订在同一数据库事务提交；失败回滚不会留下半份名单。items、events和day来源保存在新增三张受维护的PostgreSQL表；现有数据库需先显式执行 `stock-research init-schema`，API启动不自动迁移。

状态为new（新入选）、waiting（结构仍有效，继续等待）、near（接近已定义观察区）、invalid（有数据和复核依据的结构失效）。data_insufficient与not_reviewed是独立标记，模型未提交初选、量化缺计算或未到结构判断层时保留上一有效状态及其原证据来源。状态不是BUY；未进入当日五只也不自动变为失效。

`watch-v1`按现有事实投影：Martin沿已计算分支的硬排除trend、early_structure和first_retest判断结构，接近观察区沿原support真值；单纯离开支撑带、暂无回调或延伸不自动结束结构。Wyckoff未通过L1时标记未复核，L2失败为结构失效；有效结构继续等待，只有生产候选的springboard_evidence.c_support.touch_dates包含截止日时标记near。水温禁买和暂无L4触发不直接变为失效。它们是已记录的研究跟踪语义，未修改生产信号或参数。

同一workspace/目标交易日/策略的第一份成功初选固定当日来源，空初选同样固定。后续force分析仍保留报告供对照，不自动覆盖已采用日名单；人工切换来源接口尚未实现。原失败运行续跑可补齐尚未成功的策略。旧日期不覆盖更新日期的当前投影，接入历史运行须按日期顺序处理。

WatchItem同时返回target_session/checked_run_id（最近检查）、state_session/state_run_id/state_snapshot_id/state_as_of（最后有效状态的来源）与evidence_refs。数据或模型失败时这两组日期/身份可能不同，这是保留旧状态的明确记录。事件按交易日、策略、symbol和事件类型去重，保留当时before/after，不以最新状态重写历史。

## 已接通入口

```bash
.venv/bin/stock-research init-schema
.venv/bin/stock-research track-run --id "$RUN_ID"
```

正常CLI/API研究终态会自动跟踪；track-run用于显式接入已有终态运行，不启动模型、同步行情或发送通知。重复处理不新增相同事件。命令返回processed_run_id和当前名单，不表示该运行取代了已有当日来源。

| HTTP入口 | 查询 |
|---|---|
| GET /v1/watchlists | strategy可选；page>=1、page_size=1～100 |
| GET /v1/watchlists/events | strategy、symbol可选；同样在SQL分页 |

查询继续固定workspace，独立只读身份可用。运行、名单和事件是不同事实，固定历史报告保持原样。

## 验证与边界

原生PostgreSQL验证了重复及并发去重、日来源不被其他run覆盖、跨日模型失败保留/恢复、事务回滚、连接重建与workspace隔离；Martin状态测试由真实计算路径生成，Wyckoff由正式Spring和CRASH输入验证。聚焦研究及质量测试171项通过，全量Python5962项通过，无skip、50条旧警告；Ruff、格式、函数/LOC门禁、红线扫描及daily dry-run通过。

本机真实完整运行f77a7db4-b830-4310-9463-2e97565b9c17显式接入后，独立进程和只读HTTP读回5条new及5条事件；重复接入仍为5条，原报告ID、revision和content_hash不变。当时schema为18表，现有21表。

持续名单已接到现有Agent研究面板，真实代理及前端解码已验证，浏览器操作待登录；Personal工具注册仍未完成。全局质量失败而尚未形成run时，当前名单保留，[独立输入准备回执](stock_research_preparations.md)已覆盖HTTP/CLI在run前的失败；行情同步协调、单策略量化局部降级和Personal通知仍未构成完整自动运行失败链路。人工日来源切换、跟踪规则参数发布及自然交易日验证也未交付。没有正式通知、订单、收益结论、远端CI或部署验收。
