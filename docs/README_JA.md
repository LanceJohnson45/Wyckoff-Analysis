<div align="center">

# Wyckoff Trading Agent

**中国A株ワイコフ出来高分析エージェント -- 自然言語で話しかけると、相場を読み解く**

[![PyPI](https://img.shields.io/pypi/v/youngcan-wyckoff-analysis?color=blue)](https://pypi.org/project/youngcan-wyckoff-analysis/)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-AGPL--3.0-green.svg)](../LICENSE)
[![Streamlit](https://img.shields.io/badge/demo-Streamlit-FF4B4B.svg)](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)

[中文](../README.md) | [English](README_EN.md) | [Español](README_ES.md) | [한국어](README_KO.md) | [アーキテクチャ](ARCHITECTURE.md)

</div>

---

自然言語でワイコフの達人と対話しよう。彼は10個の定量ツールを操り、多段階推論を自律的に連鎖させ、「仕掛けるべきか否か」を教えてくれる。

Web + CLI の二系統対応、Gemini / Claude / OpenAI から選択可能、GitHub Actions による完全自動化。

## 機能一覧

| 機能 | 説明 |
|------|------|
| 対話型エージェント | 自然言語で診断・スクリーニング・レポートを起動、LLM がツール呼び出しを自律編成 |
| 5層ファネルスクリーニング | 全市場 ~4,500 銘柄 → ~30 候補、6チャネル + セクター共鳴 + ミクロ狙撃 |
| AI 3陣営レポート | ロジック破綻 / 備蓄キャンプ / 発射台 -- LLM が独立審判 |
| ポートフォリオ診断 | 一括ヘルスチェック：移動平均構造、アキュムレーション段階、トリガーシグナル、ストップロス状態 |
| プライベート判断 | 保有 + 候補を総合し EXIT/TRIM/HOLD/PROBE/ATTACK 指令を出力、Telegram プッシュ対応 |
| シグナル確認プール | L4 トリガーシグナルを 1-3 日の価格確認後に操作可能 |
| レコメンド追跡 | 過去の推奨銘柄の終値を自動同期、累積リターンを算出 |
| 日足バックテスト | ファネルヒット後 N 日間のリターンをリプレイ、勝率 / シャープレシオ / 最大ドローダウンを出力 |
| プレマーケットリスク管理 | A50 + VIX モニタリング、4段階アラートプッシュ |
| マルチチャネル通知 | Feishu / WeCom / DingTalk / Telegram |

## クイックスタート

### ワンライナーインストール（推奨）

```bash
curl -fsSL https://raw.githubusercontent.com/YoungCan-Wang/Wyckoff-Analysis/main/install.sh | bash
```

Python の検出、uv のインストール、隔離環境の作成を自動で行います。完了後 `wyckoff` で起動。

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

起動後：
- `/model` -- モデル選択（Gemini / Claude / OpenAI）、API Key 入力
- `/login` -- アカウントログイン、クラウドポートフォリオ連携
- そのまま質問を入力して対話開始

```
> 000001 と 600519、どちらが買いか見てほしい
> ポートフォリオを審判して
> 今の相場の温度感は？
```

アップグレード：`wyckoff update`

### Web

```bash
git clone https://github.com/YoungCan-Wang/Wyckoff-Analysis.git
cd Wyckoff-Analysis
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

オンラインデモ：**[wyckoff-analysis-youngcanphoenix.streamlit.app](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)**

## 10個のツール

エージェントの武器庫。すべて実際の出来高分析エンジンに接続：

| ツール | 機能 |
|--------|------|
| `search_stock_by_name` | 名前 / コード / ピンインによるあいまい検索 |
| `diagnose_stock` | 単一銘柄のワイコフ構造診断 |
| `diagnose_portfolio` | ポートフォリオ一括ヘルスチェック |
| `get_stock_price` | 直近の OHLCV 相場データ |
| `get_market_overview` | 市場全体の温度感 |
| `screen_stocks` | 5層ファネルによる全市場スクリーニング |
| `generate_ai_report` | 3陣営 AI 詳細レポート |
| `generate_strategy_decision` | 保有銘柄の去就 + 新規エントリー判断 |
| `get_recommendation_tracking` | 過去の推奨銘柄とその後のパフォーマンス |
| `get_signal_pending` | シグナル確認プール照会 |

ツールの呼び出し順序と回数は LLM がリアルタイムに判断。事前編成は不要。

## 5層ファネル

| 層 | 名称 | 処理内容 |
|----|------|----------|
| L1 | ゴミ除去 | ST / BSE / STAR Market を除外、時価総額 >= 35億元、日次平均出来高 >= 5,000万元 |
| L2 | 6チャネル選別 | 主要上昇 / 点火 / 潜伏 / アキュムレーション / 閑散出来高 / 支持 |
| L3 | セクター共鳴 | 業種 Top-N 分布フィルター |
| L4 | ミクロ狙撃 | Spring / LPS / SOS / EVR の4大トリガーシグナル |
| L5 | AI 審判 | LLM による3陣営分類：ロジック破綻 / 備蓄 / 発射台 |

## 日次自動化

リポジトリ内蔵の GitHub Actions 定時タスク：

| タスク | 時刻（北京時間） | 説明 |
|--------|-----------------|------|
| ファネル + AI レポート + プライベート判断 | 日-木 18:25 | 完全自動、結果を Feishu / Telegram にプッシュ |
| プレマーケットリスク管理 | 月-金 08:20 | A50 + VIX アラート |
| ストップ高復習 | 月-金 19:25 | 当日騰落率 >= 8% の銘柄を復習 |
| レコメンド追跡リプライシング | 日-木 23:00 | 終値を同期 |
| キャッシュメンテナンス | 毎日 23:05 | 期限切れ相場キャッシュをクリーンアップ |

## モデル対応

**CLI**：Gemini / Claude / OpenAI、`/model` でワンタッチ切替。任意の OpenAI 互換エンドポイントに対応。

**Web / Pipeline**：Gemini / OpenAI / DeepSeek / Qwen / Kimi / Zhipu / Volcengine / Minimax -- 計8プロバイダー。

## データソース

個別銘柄の日足データは自動フォールバック：

```
tushare → akshare → baostock → efinance
```

いずれかのソースが利用不可の場合、自動的に次へ切り替え。手動操作は不要。

## 設定

`.env.example` を `.env` にコピーし、最小限の設定：

| 変数 | 説明 |
|------|------|
| `SUPABASE_URL` / `SUPABASE_KEY` | ログインとクラウド同期 |
| `GEMINI_API_KEY`（または他プロバイダーの Key） | LLM 駆動用 |

オプション：`TUSHARE_TOKEN`（高度なデータ）、`FEISHU_WEBHOOK_URL`（Feishu プッシュ）、`TG_BOT_TOKEN` + `TG_CHAT_ID`（Telegram プライベートプッシュ）。

全設定項目と GitHub Actions Secrets の詳細は [アーキテクチャドキュメント](ARCHITECTURE.md) を参照。

## ライセンス

[AGPL-3.0](../LICENSE) &copy; 2024-2026 youngcan

---

[![Star History Chart](https://api.star-history.com/svg?repos=YoungCan-Wang/Wyckoff-Analysis&type=Date)](https://star-history.com/#YoungCan-Wang/Wyckoff-Analysis&Date)
