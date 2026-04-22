# Web 端迁移方案：Streamlit → React

## 背景

Streamlit 作为全家桶已满足不了 Agent 交互体验（流式多轮对话、工具调用可视化、复杂布局），
需迁移到 React 前端 + Python API 后端的标准架构。

## 架构

```
┌──────────────┐     HTTP/WS      ┌──────────────┐      SQL       ┌──────────────┐
│  React 前端   │  ───────────→   │  Python API   │  ──────────→  │   Supabase   │
│  (Vercel)    │  ←───────────   │  (Render/Fly) │  ←──────────  │   (已有)      │
└──────────────┘   SSE 流式推送   └──────────────┘               └──────────────┘
                                        │
                                        ↓ LLM
                                   Gemini / OpenAI
```

## 技术选型

| 层 | 方案 | 免费额度 | 升级价格 |
|---|------|---------|---------|
| 前端 | **Vercel** (React + Next.js) | 无限站点，100GB/月带宽 | Pro $20/月 |
| 后端 | **Render** (FastAPI/Flask) | 免费实例，15分钟休眠 | Starter $7/月 |
| 备选后端 | **Fly.io** | 3 shared-cpu 免费 | 按用量 |
| 备选后端 | **Railway** | $5/月免费额度 | $5 起 |
| 数据库 | **Supabase** (已有) | 500MB，50K MAU | Pro $25/月 |
| 域名 | Vercel 免费子域 / 自购 | .com 约 ¥60/年 | — |

## 后端 API 设计

```
POST /api/chat          — Agent 对话（SSE 流式返回 thinking/tool/text）
GET  /api/portfolio     — 用户持仓
GET  /api/signals       — 信号确认池
GET  /api/market        — 大盘水温
POST /api/auth/login    — Supabase Auth 代理
```

核心逻辑直接复用现有 Python 模块（agents/chat_tools.py, core/, integrations/），
只需加一层 FastAPI 路由把 Agent 对话包装成 HTTP API。

## 前端页面

| 页面 | 说明 |
|------|------|
| 读盘室（Chat） | Agent 对话主界面，流式展示 thinking + tool 调用 + 回复 |
| 仪表盘（Dashboard） | 大盘水温 + 信号池 + 持仓概览，一屏总览 |
| 设置（Settings） | API Key、推送渠道、用户分层 |

## 迁移路径

```
Phase 0（当前）  CLI Agent 能力补齐，Streamlit 读盘室同步可用
Phase 1         搭 FastAPI 后端，把 Agent 对话包装成 SSE API
Phase 2         React 前端 MVP（Chat 页面），接 FastAPI
Phase 3         Dashboard + Settings 页面
Phase 4         接入支付（微信支付/Stripe），上线收费
```

## 成本估算

| 阶段 | 月成本 |
|------|-------|
| 早期（< 100 用户） | ¥0（全免费层） |
| 增长期（100-1000 用户） | ~¥200/月（Supabase Pro + Render Starter） |
| 规模期（1000+ 用户） | ~¥500+/月（按用量扩容） |
