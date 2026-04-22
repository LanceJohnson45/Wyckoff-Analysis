<div align="center">

# Wyckoff Trading Agent

**Wyckoff Volume-Price Analysis Agent for China A-Shares — Talk to it like a human, it reads the tape**

[![PyPI](https://img.shields.io/pypi/v/youngcan-wyckoff-analysis?color=blue)](https://pypi.org/project/youngcan-wyckoff-analysis/)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-AGPL--3.0-green.svg)](../LICENSE)
[![Streamlit](https://img.shields.io/badge/demo-Streamlit-FF4B4B.svg)](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)

[中文](../README.md) | [日本語](README_JA.md) | [Español](README_ES.md) | [한국어](README_KO.md) | [Architecture](ARCHITECTURE.md)

</div>

---

Talk to a Wyckoff master in natural language. He commands 10 quantitative tools, chains multi-step reasoning, and tells you whether to strike.

Web + CLI dual channel, Gemini / Claude / OpenAI pick one, GitHub Actions for fully automated daily runs.

## Features

| Capability | Description |
|---|---|
| Conversational Agent | Trigger diagnosis, screening, and reports in plain language; the LLM orchestrates tool calls autonomously |
| Five-Layer Funnel | Full market ~4 500 stocks -> ~30 candidates via six channels + sector resonance + micro triggers |
| AI Three-Camp Report | Logic Bankrupt / Reserve Camp / Springboard — LLM renders an independent verdict |
| Portfolio Diagnosis | Batch health check: MA structure, accumulation phase, trigger signals, stop-loss status |
| Private Rebalance | Synthesizes holdings + candidates, outputs EXIT / TRIM / HOLD / PROBE / ATTACK orders, pushes to Telegram |
| Signal Confirmation Pool | L4 trigger signals must pass 1-3 day price confirmation before becoming actionable |
| Recommendation Tracking | Historical picks auto-sync closing prices and compute cumulative returns |
| Daily-Bar Backtest | Replays post-funnel N-day returns; reports win rate / Sharpe / max drawdown |
| Pre-Market Risk | A50 futures + VIX monitoring with four alert levels |
| Multi-Channel Notifications | Feishu / WeCom / DingTalk / Telegram |

## Quick Start

### One-line Install (recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/YoungCan-Wang/Wyckoff-Analysis/main/install.sh | bash
```

Detects Python, installs uv, creates an isolated environment. Run `wyckoff` when done.

### Homebrew

```bash
brew tap YoungCan-Wang/wyckoff
brew install wyckoff
```

### pip

```bash
uv venv && source .venv/bin/activate
uv pip install youngcan-wyckoff-analysis
wyckoff
```

Once inside:
- `/model` — choose a model (Gemini / Claude / OpenAI) and enter your API key
- `/login` — sign in to sync cloud portfolio
- Start asking questions

```
> Compare 000001 and 600519 — which one is the better buy?
> Judge my portfolio
> What's the market temperature right now?
```

Upgrade: `wyckoff update`

### Web

```bash
git clone https://github.com/YoungCan-Wang/Wyckoff-Analysis.git
cd Wyckoff-Analysis
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Live demo: **[wyckoff-analysis-youngcanphoenix.streamlit.app](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)**

## 10 Tools

The agent's arsenal — each one wired to a real volume-price engine:

| Tool | Capability |
|---|---|
| `search_stock_by_name` | Fuzzy search by name, ticker, or pinyin |
| `diagnose_stock` | Structured Wyckoff diagnosis for a single stock |
| `diagnose_portfolio` | Batch portfolio health scan |
| `get_stock_price` | Recent OHLCV quotes |
| `get_market_overview` | Broad market temperature overview |
| `screen_stocks` | Five-layer funnel full-market screening |
| `generate_ai_report` | Three-camp AI deep research report |
| `generate_strategy_decision` | Hold/exit existing positions + new buy decisions |
| `get_recommendation_tracking` | Historical recommendations and follow-up performance |
| `get_signal_pending` | Query the signal confirmation pool |

Tool call order and frequency are decided by the LLM at runtime — no pre-choreography needed.

## Five-Layer Funnel

| Layer | Name | What It Does |
|---|---|---|
| L1 | Garbage Filter | Remove ST / BSE / STAR Market; market cap >= 3.5 B CNY; avg daily turnover >= 50 M CNY |
| L2 | Six-Channel Selection | Rally / Ignition / Stealth / Accumulation / Dry Volume / Support |
| L3 | Sector Resonance | Top-N industry distribution filter |
| L4 | Micro Triggers | Spring / LPS / SOS / EVR — four trigger signals |
| L5 | AI Verdict | LLM three-camp classification: Logic Bankrupt / Reserve / Springboard |

## Daily Automation

Built-in GitHub Actions cron jobs:

| Task | Schedule (Beijing Time) | Description |
|---|---|---|
| Funnel + AI Report + Rebalance | Sun–Thu 18:25 | Fully automated; results pushed to Feishu / Telegram |
| Pre-Market Risk | Mon–Fri 08:20 | A50 + VIX alert |
| Limit-Up Review | Mon–Fri 19:25 | Review stocks that rose >= 8% today |
| Recommendation Reprice | Sun–Thu 23:00 | Sync closing prices |
| Cache Maintenance | Daily 23:05 | Purge stale quote caches |

## Model Support

**CLI**: Gemini / Claude / OpenAI — switch with `/model`; any OpenAI-compatible endpoint works.

**Web / Pipeline**: Gemini / OpenAI / DeepSeek / Qwen / Kimi / Zhipu / Volcengine / Minimax — 8 providers total.

## Data Sources

Daily bar auto-fallback chain:

```
tickflow → tushare → akshare → baostock → efinance
```

When any source is unavailable the system silently falls back to the next — zero intervention required.

## Configuration

Copy `.env.example` to `.env`. Minimum required:

| Variable | Purpose |
|---|---|
| `SUPABASE_URL` / `SUPABASE_KEY` | Auth and cloud sync |
| `GEMINI_API_KEY` (or another provider's key) | Powers the LLM |

Optional: `TICKFLOW_API_KEY` (TickFlow real-time/intraday), `TUSHARE_TOKEN` (premium data), `FEISHU_WEBHOOK_URL` (Feishu push), `TG_BOT_TOKEN` + `TG_CHAT_ID` (Telegram push).

See the [Architecture doc](ARCHITECTURE.md) for the full config reference and GitHub Actions Secrets setup.

## Wyckoff Skills

Lightweight reuse of the Wyckoff analysis capability: [`YoungCan-Wang/wyckoff_skill`](https://github.com/YoungCan-Wang/wyckoff_skill.git)

Ideal for giving any AI assistant a quick "Wyckoff lens."

## License

[AGPL-3.0](../LICENSE) &copy; 2024-2026 youngcan

---

[![Star History Chart](https://api.star-history.com/svg?repos=YoungCan-Wang/Wyckoff-Analysis&type=Date)](https://star-history.com/#YoungCan-Wang/Wyckoff-Analysis&Date)
