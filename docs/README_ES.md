<div align="center">

# Wyckoff Trading Agent

**Sistema cuantitativo Wyckoff para acciones A de China — hablas con naturalidad, el agente lee el mercado**

[![PyPI](https://img.shields.io/pypi/v/youngcan-wyckoff-analysis?color=blue)](https://pypi.org/project/youngcan-wyckoff-analysis/)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-AGPL--3.0-green.svg)](../LICENSE)
[![Streamlit](https://img.shields.io/badge/demo-Streamlit-FF4B4B.svg)](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)

[中文](../README.md) | [English](README_EN.md) | [日本語](README_JA.md) | [한국어](README_KO.md) | [Arquitectura](ARCHITECTURE.md)

</div>

---

Conversa en lenguaje natural con un agente IA experto en el metodo Wyckoff. Despacha de forma autonoma 10 herramientas cuantitativas, encadena razonamientos de multiples pasos y te da conclusiones accionables de trading.

Web + CLI, compatible con Gemini / Claude / OpenAI, automatizacion completa via GitHub Actions.

## Funcionalidades

| Capacidad | Descripcion |
|-----------|-------------|
| Agente conversacional | Diagnosticos, filtros y reportes activados con lenguaje natural; el LLM orquesta las herramientas |
| Embudo de 5 capas | ~4 500 acciones del mercado completo se reducen a ~30 candidatas mediante seis canales + resonancia sectorial + micro-disparo |
| Reporte IA de 3 campamentos | Logica rota / Reserva / Plataforma de despegue — el LLM clasifica de forma independiente |
| Diagnostico de cartera | Escaneo masivo: estructura de medias moviles, fase de acumulacion, senales de activacion, estado de stop-loss |
| Rebalanceo privado | Combina posiciones + candidatas y emite ordenes EXIT / TRIM / HOLD / PROBE / ATTACK, con push a Telegram |
| Confirmacion de senales | Las senales L4 pasan por 1-3 dias de confirmacion de precio antes de ser accionables |
| Seguimiento de recomendaciones | Sincroniza automaticamente el precio de cierre y calcula el rendimiento acumulado |
| Backtesting | Simula rendimiento a N dias tras el filtrado del embudo: tasa de aciertos, Sharpe, drawdown maximo |
| Riesgo pre-mercado | Monitoreo de A50 + VIX con cuatro niveles de alerta |
| Notificaciones multicanal | Feishu / WeCom / DingTalk / Telegram |

## Inicio rapido

### Instalacion en una linea (recomendado)

```bash
curl -fsSL https://raw.githubusercontent.com/YoungCan-Wang/Wyckoff-Analysis/main/install.sh | bash
```

Detecta Python, instala uv y crea un entorno aislado. Al finalizar, ejecuta `wyckoff`.

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

Dentro del agente:
- `/model` — elegir modelo (Gemini / Claude / OpenAI) e introducir API Key
- `/login` — iniciar sesion para sincronizar la cartera en la nube
- Escribe tu pregunta directamente

```
> Compara 000001 y 600519, cual conviene comprar?
> Revisa mi cartera
> Como esta el mercado general ahora?
```

Actualizar: `wyckoff update`

### Web

```bash
git clone https://github.com/YoungCan-Wang/Wyckoff-Analysis.git
cd Wyckoff-Analysis
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Demo en linea: **[wyckoff-analysis-youngcanphoenix.streamlit.app](https://wyckoff-analysis-youngcanphoenix.streamlit.app/)**

## 10 Herramientas

Arsenal del agente — cada una conectada a un motor real de analisis de volumen y precio:

| Herramienta | Capacidad |
|-------------|-----------|
| `search_stock_by_name` | Busqueda difusa por nombre, codigo o pinyin |
| `diagnose_stock` | Diagnostico estructurado Wyckoff de una accion |
| `diagnose_portfolio` | Escaneo masivo de salud de la cartera |
| `get_stock_price` | OHLCV reciente |
| `get_market_overview` | Panorama general del mercado |
| `screen_stocks` | Filtrado del mercado completo con embudo de 5 capas |
| `generate_ai_report` | Reporte IA profundo en 3 campamentos |
| `generate_strategy_decision` | Decision de permanencia / entrada en posiciones |
| `get_recommendation_tracking` | Historial de recomendaciones y rendimiento posterior |
| `get_signal_pending` | Consulta del pool de confirmacion de senales |

El orden y la frecuencia de las llamadas los decide el LLM en tiempo real, sin orquestacion previa.

## Embudo de 5 capas

| Capa | Nombre | Accion |
|------|--------|--------|
| L1 | Eliminar basura | Excluye ST / BSE / STAR Market; capitalizacion >= 3 500 M CNY; volumen diario >= 50 M CNY |
| L2 | Seleccion de 6 canales | Impulso / Ignicion / Latente / Acumulacion / Volumen minimo / Soporte |
| L3 | Resonancia sectorial | Filtro por distribucion Top-N de sectores |
| L4 | Micro-disparo | Cuatro senales clave: Spring / LPS / SOS / EVR |
| L5 | Juicio IA | Clasificacion LLM en 3 campamentos: Logica rota / Reserva / Plataforma de despegue |

## Automatizacion diaria

Tareas programadas con GitHub Actions integradas en el repositorio:

| Tarea | Hora (Beijing) | Descripcion |
|-------|---------------|-------------|
| Embudo + Reporte IA + Rebalanceo | Dom-Jue 18:25 | Totalmente automatico; resultados enviados a Feishu / Telegram |
| Riesgo pre-mercado | Lun-Vie 08:20 | Alerta A50 + VIX |
| Resumen de limit-up | Lun-Vie 19:25 | Revision de acciones con alza diaria >= 8 % |
| Repricing de recomendaciones | Dom-Jue 23:00 | Sincroniza precios de cierre |
| Mantenimiento de cache | Diario 23:05 | Limpia cache de cotizaciones expiradas |

## Soporte de modelos

**CLI**: Gemini / Claude / OpenAI — cambia al instante con `/model`; compatible con cualquier endpoint compatible con OpenAI.

**Web / Pipeline**: Gemini / OpenAI / DeepSeek / Qwen / Kimi / Zhipu / Volcengine / Minimax — 8 proveedores en total.

## Fuentes de datos

La descarga de datos diarios se degrada automaticamente:

```
tushare -> akshare -> baostock -> efinance
```

Si una fuente no esta disponible, se cambia automaticamente a la siguiente sin intervencion manual.

## Configuracion

Copia `.env.example` como `.env`. Configuracion minima:

| Variable | Descripcion |
|----------|-------------|
| `SUPABASE_URL` / `SUPABASE_KEY` | Login y sincronizacion en la nube |
| `GEMINI_API_KEY` (u otra clave de proveedor) | Motor LLM |

Opcional: `TUSHARE_TOKEN` (datos avanzados), `FEISHU_WEBHOOK_URL` (push Feishu), `TG_BOT_TOKEN` + `TG_CHAT_ID` (push Telegram).

Consulta la configuracion completa y los secretos de GitHub Actions en la [documentacion de arquitectura](ARCHITECTURE.md).

## Licencia

[AGPL-3.0](../LICENSE) &copy; 2024-2026 youngcan

---

[![Star History Chart](https://api.star-history.com/svg?repos=YoungCan-Wang/Wyckoff-Analysis&type=Date)](https://star-history.com/#YoungCan-Wang/Wyckoff-Analysis&Date)
