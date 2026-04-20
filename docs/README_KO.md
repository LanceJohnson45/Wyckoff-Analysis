<div align="center">

# Wyckoff Trading Agent

**A주 와이코프 거래량-가격 분석 AI 에이전트 -- 자연어로 말하면, 차트를 읽어줍니다**

[![PyPI](https://img.shields.io/pypi/v/youngcan-wyckoff-analysis?color=blue)](https://pypi.org/project/youngcan-wyckoff-analysis/)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-AGPL--3.0-green.svg)](../LICENSE)
[![Streamlit](https://img.shields.io/badge/demo-Streamlit-FF4B4B.svg)](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)

[中文](../README.md) | [English](README_EN.md) | [日本語](README_JA.md) | [Español](README_ES.md) | [아키텍처](ARCHITECTURE.md)

</div>

---

자연어로 와이코프 전문가와 대화하세요. 10가지 거래량-가격 분석 도구를 자율적으로 조합하여 다단계 추론을 수행하고, "매수할 것인가, 관망할 것인가"에 대한 결론을 제시합니다.

Web + CLI 듀얼 채널, Gemini / Claude / OpenAI 중 선택, GitHub Actions 자동화 스케줄링 지원.

## 기능 개요

| 기능 | 설명 |
|------|------|
| 대화형 에이전트 | 자연어로 진단, 스크리닝, 리서치 리포트를 실행하며, LLM이 자율적으로 도구 호출을 편성 |
| 5단계 퍼널 스크리닝 | 전체 시장 ~4,500종목 → ~30 후보군, 6채널 + 섹터 공명 + 미시 저격 |
| AI 3진영 리포트 | 논리 파산 / 비축 진영 / 도약대, LLM이 독립적으로 판단 |
| 포트폴리오 진단 | 일괄 건강 검진: 이동평균 구조, 매집 단계, 트리거 시그널, 손절 상태 |
| 개인 리밸런싱 | 보유 종목 + 후보를 종합하여 EXIT/TRIM/HOLD/PROBE/ATTACK 지시 출력, Telegram 푸시 |
| 시그널 확인 풀 | L4 트리거 시그널이 1-3일 가격 확인 후에만 실행 가능 |
| 추천 추적 | 과거 추천 종목의 종가 자동 동기화, 누적 수익률 계산 |
| 일봉 백테스트 | 퍼널 적중 후 N일 수익률 재생, 승률/Sharpe/최대 낙폭 출력 |
| 장전 리스크 관리 | A50 + VIX 모니터링, 4단계 경보 푸시 |
| 다채널 알림 | Feishu / WeCom / DingTalk / Telegram |

## 빠른 시작

### 원라인 설치 (권장)

```bash
curl -fsSL https://raw.githubusercontent.com/YoungCan-Wang/Wyckoff-Analysis/main/install.sh | bash
```

Python 감지, uv 설치, 격리 환경 생성을 자동으로 수행합니다. 완료 후 `wyckoff`로 실행.

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

실행 후:
- `/model` -- 모델 선택 (Gemini / Claude / OpenAI), API Key 입력
- `/login` -- 계정 로그인, 클라우드 포트폴리오 연동
- 자연어로 질문을 입력하여 대화 시작

```
> 000001과 600519 중 어느 것이 더 매수 가치가 있는지 분석해줘
> 내 포트폴리오를 진단해줘
> 시장 온도가 어때?
```

업데이트: `wyckoff update`

### Web

```bash
git clone https://github.com/YoungCan-Wang/Wyckoff-Analysis.git
cd Wyckoff-Analysis
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

온라인 데모: **[wyckoff-analysis-youngcanphoenix.streamlit.app](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)**

## 10가지 도구

에이전트의 무기고 -- 각 도구는 실제 거래량-가격 분석 엔진에 연결됩니다:

| 도구 | 기능 |
|------|------|
| `search_stock_by_name` | 종목명 / 코드 / 병음 퍼지 검색 |
| `diagnose_stock` | 단일 종목 와이코프 구조 진단 |
| `diagnose_portfolio` | 일괄 포트폴리오 건강 스캔 |
| `get_stock_price` | 최근 OHLCV 시세 조회 |
| `get_market_overview` | 시장 온도 개요 |
| `screen_stocks` | 5단계 퍼널 전체 시장 스크리닝 |
| `generate_ai_report` | 3진영 AI 심층 리포트 |
| `generate_strategy_decision` | 보유 종목 유지/매도 + 신규 매수 의사결정 |
| `get_recommendation_tracking` | 과거 추천 및 후속 성과 |
| `get_signal_pending` | 시그널 확인 풀 조회 |

도구 호출 순서와 횟수는 LLM이 실시간으로 결정하며, 사전 편성이 필요 없습니다.

## 5단계 퍼널

| 단계 | 명칭 | 기능 |
|------|------|------|
| L1 | 불량 종목 제거 | ST / BSE / STAR 시장 제외, 시가총액 >= 35억, 일평균 거래대금 >= 5천만 |
| L2 | 6채널 선별 | 주상승 / 점화 / 잠복 / 매집 / 극저량 / 호가방어 |
| L3 | 섹터 공명 | 업종 Top-N 분포 필터링 |
| L4 | 미시 저격 | Spring / LPS / SOS / EVR 4대 트리거 시그널 |
| L5 | AI 심판 | LLM 3진영 분류: 논리 파산 / 비축 / 도약대 |

## 일일 자동화

리포지토리에 내장된 GitHub Actions 스케줄 작업:

| 작업 | 시간 (베이징) | 설명 |
|------|--------------|------|
| 퍼널 스크리닝 + AI 리포트 + 개인 결정 | 일-목 18:25 | 완전 자동, Feishu/Telegram 푸시 |
| 장전 리스크 관리 | 월-금 08:20 | A50 + VIX 경보 |
| 상한가 복기 | 월-금 19:25 | 당일 상승률 >= 8% 복기 |
| 추천 추적 가격 갱신 | 일-목 23:00 | 종가 동기화 |
| 캐시 유지보수 | 매일 23:05 | 만료된 시세 캐시 정리 |

## 모델 지원

**CLI**: Gemini / Claude / OpenAI, `/model`로 원클릭 전환, 임의의 OpenAI 호환 엔드포인트 지원.

**Web / Pipeline**: Gemini / OpenAI / DeepSeek / Qwen / Kimi / Zhipu / Volcengine / Minimax, 총 8개 제공사.

## 데이터 소스

개별 종목 일봉 데이터 자동 폴백:

```
tushare → akshare → baostock → efinance
```

어느 소스든 사용 불가 시 자동 전환되며, 별도 개입이 필요 없습니다.

## 설정

`.env.example`을 `.env`로 복사한 후, 최소 설정:

| 변수 | 설명 |
|------|------|
| `SUPABASE_URL` / `SUPABASE_KEY` | 로그인 및 클라우드 동기화 |
| `GEMINI_API_KEY` (또는 다른 제공사 Key) | LLM 구동 |

선택 설정: `TUSHARE_TOKEN` (고급 데이터), `FEISHU_WEBHOOK_URL` (Feishu 푸시), `TG_BOT_TOKEN` + `TG_CHAT_ID` (Telegram 개인 푸시).

전체 설정 항목 및 GitHub Actions Secrets 설명은 [아키텍처 문서](ARCHITECTURE.md)를 참조하세요.

## License

[AGPL-3.0](../LICENSE) &copy; 2024-2026 youngcan

---

[![Star History Chart](https://api.star-history.com/svg?repos=YoungCan-Wang/Wyckoff-Analysis&type=Date)](https://star-history.com/#YoungCan-Wang/Wyckoff-Analysis&Date)
