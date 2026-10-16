"""Martin 辅助扫描工作台，共用同一视图支持独立网页和项目内页面。"""

from __future__ import annotations

from datetime import date
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import re
from typing import Literal

import pandas as pd
import streamlit as st

from core.martin_luk import (
    COLUMN_ALIASES, MIN_HISTORY, RULE_VERSION, LukObservation, LukParameters,
    analyze_daily, normalize_daily_frame,
)
from integrations.local_market_store import local_market_path, local_market_summary, read_local_daily, read_local_issues
from utils.trading_clock import resolve_end_calendar_day_for_market

MAX_SYMBOLS = 20
MAX_CSV_BYTES = 5 * 1024 * 1024
LUK_DEFAULT_SYMBOLS = {"cn": "600519, 300750, 000333", "us": "AAPL, MSFT, NVDA"}
BENCHMARKS = {"cn": "000300", "us": "SPY"}
STATE_KEY = "martin_luk_scan"
INPUT_MODES = ("本地行情库", "上传日线 CSV")


@st.cache_data(max_entries=8, show_spinner=False)
def _cached_local_summary(market: str, path: str, revision: int | None) -> dict[str, str | int]:
    """大库覆盖统计只在文件更新时重算，避免切观察卡时重复扫描全表。"""
    return local_market_summary(market, path=Path(path))


def _local_summary(market: str) -> dict[str, str | int]:
    path = local_market_path()
    return _cached_local_summary(market, str(path), path.stat().st_mtime_ns if path.is_file() else None)


def parse_symbols(text: str, market: str) -> list[str]:
    """严格解析本市场代码，保留 A 股前导零并去重。"""
    if market not in LUK_DEFAULT_SYMBOLS:
        raise ValueError("不支持的市场")
    tokens = [s.upper() for s in re.split(r"[\s,，;；]+", text.strip()) if s]
    pattern = r"\d{6}" if market == "cn" else r"[A-Z][A-Z0-9]{0,9}(?:[.-][A-Z0-9]{1,4})?"
    if not tokens:
        raise ValueError("请输入至少一个股票代码")
    invalid = [s for s in tokens if not re.fullmatch(pattern, s)]
    if invalid:
        raise ValueError(f"代码格式不符：{', '.join(invalid[:3])}")
    symbols = list(dict.fromkeys(tokens))
    if len(symbols) > MAX_SYMBOLS:
        raise ValueError(f"每次最多分析 {MAX_SYMBOLS} 只自选股")
    return symbols


def read_daily_csv(content: bytes, market: str, single_symbol: str) -> dict[str, pd.DataFrame]:
    """检查上传边界并拆分多股票 CSV；不写盘或共享缓存。"""
    if not content or len(content) > MAX_CSV_BYTES:
        raise ValueError("CSV 不能为空，且大小不能超过 5 MB")
    try:
        frame = pd.read_csv(BytesIO(content), dtype=str, encoding="utf-8-sig")
    except (UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError) as exc:
        raise ValueError("无法读取 CSV，请使用 UTF-8 编码的逗号分隔文件") from exc
    frame.columns = [COLUMN_ALIASES.get(str(c).strip(), str(c).strip().lower()) for c in frame.columns]
    if frame.columns.duplicated().any():
        raise ValueError("CSV 存在重复或冲突的列名")
    if "symbol" not in frame:
        symbols = parse_symbols(single_symbol, market)
        if len(symbols) != 1:
            raise ValueError("单股 CSV 请指定一个股票代码")
        frame["symbol"] = symbols[0]
    if frame["symbol"].isna().any():
        raise ValueError("CSV 的 symbol 列不能缺失")
    frame["symbol"] = frame["symbol"].str.strip().str.upper()
    symbols = parse_symbols(",".join(frame["symbol"].unique()), market)
    frames = {symbol: frame.loc[frame["symbol"] == symbol].copy() for symbol in symbols}
    for item in frames.values():
        item.attrs["source"] = "用户 CSV（复权口径由上传者确认）"
    return frames


def load_daily(symbol: str, market: Literal["cn", "us"], as_of: date, *, benchmark: bool = False) -> pd.DataFrame:
    """只读取本机完整历史，扫描过程不发出网络请求。"""
    frame = read_local_daily(symbol, market, as_of, benchmark=benchmark)
    return normalize_daily_frame(frame, as_of=as_of, require_traded=not benchmark)


def render_local_sync(market: str, symbols_text: str) -> tuple[object, ...]:
    """显式同步与离线读取分开，避免扫描时隐式联网。"""
    with st.expander("本地行情库与云端同步", expanded=not local_market_path().is_file()):
        revision: tuple[object, ...] = (str(local_market_path()), "unavailable")
        try:
            summary = _local_summary(market)
            revision = (str(local_market_path()), summary["last_sync"], summary["rows"], summary["latest_date"], summary["issues"])
            a, b, c = st.columns(3)
            a.metric("本地股票", summary["symbols"])
            b.metric("最新日线", summary["latest_date"])
            c.metric("本地日线行数", summary["rows"])
            st.caption(f"最近成功同步：{summary['last_sync']}")
            if summary["issues"]:
                st.warning(f"本市场有 {summary['issues']} 根异常日线已隔离；受影响股票暂不生成分析结果。")
                with st.expander("异常日线明细（最多200行）"):
                    st.dataframe(read_local_issues(market), hide_index=True, width="stretch")
        except (RuntimeError, ValueError, OSError) as exc:
            st.error(str(exc))
        st.caption("Actions维护云端数据；此按钮只从数据库复制到本机。扫描与看图均只读本地，断网也可使用已同步数据。")
        scope = st.radio("同步范围", ["当前自选股", "本市场全部云端股票"], horizontal=True)
        if scope == "本市场全部云端股票":
            st.caption("首次全市场同步可能耗时较长；之后只同步变更行。本机同步不会调用行情接口补拉云端缺失数据。")
        if st.button("从云端同步到本机", key="luk_sync", width="stretch"):
            from integrations.market_data_sync import sync_market, sync_symbols

            try:
                symbols = sync_symbols(symbols_text, market) if scope == "当前自选股" else ()
                with st.status("正在只读云端数据库并写入本机…", expanded=True) as status:
                    detail = st.empty()
                    result = sync_market(market, symbols=symbols, progress=lambda rows: detail.caption(f"已写入/核对 {rows:,} 行"))
                    status.update(label=f"同步完成 · {result.rows:,} 行 · {result.batches} 批", state="complete", expanded=False)
                if result.rejected:
                    st.warning(f"本地数据已更新，但有 {result.rejected} 根异常日线已隔离，受影响股票暂不可分析。")
                else:
                    st.success("本地数据已更新，点击“开始扫描”使用新数据。")
                st.session_state.pop(STATE_KEY, None)
            except (RuntimeError, ValueError, OSError) as exc:
                st.error(f"同步未完成：{exc}")
        st.caption(f"本机文件：{local_market_path()}")
    return revision


def render_observation(observation: LukObservation) -> None:
    """展示一个候选的图表、证据和下一步，不生成买卖订单。"""
    frame = observation.frame
    latest = frame.iloc[-1]
    st.subheader(f"{observation.symbol} · {' / '.join(observation.tags)}")
    st.caption(f"日线截至 {observation.data_date} · {observation.source} · {RULE_VERSION}")
    currency = "CNY" if observation.market == "cn" else "USD"
    first, second, third, fourth = st.columns(4)
    first.metric(f"收盘 / {currency}", f"{latest['close']:.3f}")
    second.metric("距 EMA21 / ATR", f"{latest['distance_atr']:.2f}")
    third.metric("ADR20", f"{latest['adr20']:.2f}%")
    fourth.metric("20日相对基准", "未计算" if observation.rs20_pct is None else f"{observation.rs20_pct:+.2f} pp")
    price_tab, evidence_tab = st.tabs(["价格与均线", "OHLCV 明细"])
    with price_tab:
        chart = frame.tail(100)[["date", "open", "high", "low", "close", "ema9", "ema21", "ema50"]]
        up, down = ("#dc2626", "#16a34a") if observation.market == "cn" else ("#16a34a", "#dc2626")
        candle_color = {"condition": {"test": "datum.close >= datum.open", "value": up}, "value": down}
        st.vega_lite_chart(chart, {
            "height": 320,
            "encoding": {
                "x": {"field": "date", "type": "temporal", "title": None},
                "y": {"type": "quantitative", "scale": {"zero": False}, "title": "价格"},
                "tooltip": [{"field": "date", "type": "temporal", "title": "日期"}] + [
                    {"field": name, "type": "quantitative", "format": ".3f"}
                    for name in ("open", "high", "low", "close", "ema9", "ema21", "ema50")
                ],
            },
            "layer": [
                {"mark": "rule", "encoding": {"y": {"field": "low"}, "y2": {"field": "high"}, "color": candle_color}},
                {"mark": {"type": "bar", "size": 5}, "encoding": {"y": {"field": "open"}, "y2": {"field": "close"}, "color": candle_color}},
                {"transform": [{"fold": ["ema9", "ema21", "ema50"], "as": ["均线", "价格"]}], "mark": {"type": "line", "strokeWidth": 1.5}, "encoding": {
                    "y": {"field": "价格"},
                    "color": {"field": "均线", "type": "nominal", "scale": {"domain": ["ema9", "ema21", "ema50"], "range": ["#0d9488", "#d97706", "#6366f1"]}, "legend": {"orient": "top", "title": None}},
                }},
            ],
        }, use_container_width=True)
        st.caption("日K与 EMA9 / 21 / 50；A股红涨绿跌，美股绿涨红跌。日线无法还原盘中路径。")
        st.bar_chart(frame.tail(100).set_index("date")[["volume"]], color="#94a3b8", height=120)
        st.caption("成交量按原数据源单位展示；不同数据源的股／手单位不直接比较。")
    with evidence_tab:
        st.dataframe(frame.tail(100), hide_index=True, width="stretch")
    evidence, plan = st.columns(2)
    with evidence:
        st.markdown("**为什么进入观察**")
        for reason in observation.reasons:
            st.write(f"• {reason}")
    with plan:
        st.markdown("**下一步检查**")
        st.write(observation.next_step)
        if observation.trigger is not None:
            st.write(f"待突破参考：{observation.trigger:.3f} {currency}")
        if observation.invalidation is not None:
            st.write(f"结构失效参考：{observation.invalidation:.3f} {currency}")
        st.caption("参考位按行情价格口径计算，不能直接当作券商委托价或保证成交的止损价。")
    for warning in observation.warnings:
        st.warning(warning)


def render_rules(parameters: LukParameters) -> None:
    """明确第一版使用的量化定义与数据边界。"""
    with st.expander("规则与数据口径", expanded=False):
        st.markdown(f"""
**{RULE_VERSION}：工程量化定义，未回测认证。** EMA9/21/50、先位置后触发来自资料；下面的窗口与阈值是本程序研究假设。

- **趋势**：EMA9＞EMA21＞EMA50，三线均高于5根前，收盘≥EMA21。
- **强势回调**：趋势成立，收盘低于此前5日最高收盘，或日低下移且收盘低于昨日高；日低距 EMA9 或21 不超过 {parameters.support_atr:g} ATR，且收盘守在线上。
- **收紧**：近3日平均高低区间≤此前10日的 {parameters.contraction_ratio:.0%}，近3日均量≤此前10日均量。此前3日收紧后收盘突破前10日高，标为“收紧突破”。
- **U&R**：日低跌破此前20日最低价，收盘重新站回；弱趋势下独立提示风险。
- **延伸**：收盘高于 EMA21 超过 {parameters.extension_atr:g} ATR；普通回调与收紧候选暂不纳入。
- **指标**：ATR14 为14日真实区间简单均值；ADR20%=20日平均 `(high/low−1)×100`；均线用 `adjust=False` EMA。
- **参考位**：回调以本日高／近5日低；收紧以此前10日高／整理3日低；U&R以本日高／本日低。同股多标签时，参考位优先 U&R、再收紧、再回调。
- **相对强度**：同一组21个个股交易日期的20日收益减基准收益，单位为百分点；不对齐时留空。它是证据列，不是硬筛选条件。
- **数据**：扫描只读本地库，复权口径沿用云端 `qfq`；云端美股沿用现有数据源自动调整。上传 CSV 应保持同股价格与量的口径一致。至少 {MIN_HISTORY} 根日线，不接受重复日期、缺失量、零量最新快照。

只识别日线形态，不验证事件催化、板块主题、AVWAP或1/5分钟触发。A股执行需另考虑T+1、涨跌停；失效参考不是亏损上限。
""")


def render_martin_luk() -> None:
    """渲染自选股输入、扫描状态、结果列表和观察卡。"""
    st.title("Martin 辅助扫描")
    st.write("从强股里找靠近支撑的位置，让每一个候选都有可检查的依据。")
    st.caption("日线研究工作台 · 回调 / 收紧突破 / U&R · 支持 A 股与美股")
    st.caption("Actions → 云端数据库 → 显式增量同步 → 本机日线分析；扫描不在线拉行情。")
    market_label = st.radio("市场", ["A 股", "美股"], horizontal=True, key="luk_market")
    market: Literal["cn", "us"] = "cn" if market_label == "A 股" else "us"
    mode = st.radio("行情输入", INPUT_MODES, horizontal=True, key="luk_mode")
    completed_day = resolve_end_calendar_day_for_market(market)
    with st.form("luk_scan_form"):
        left, right = st.columns([3, 1])
        with left:
            symbols_text = st.text_area(
                "股票代码" if mode == INPUT_MODES[0] else "单股 CSV 对应代码（有 symbol 列时忽略）",
                value=LUK_DEFAULT_SYMBOLS[market], key=f"luk_symbols_{market}_{mode}",
                help=f"用逗号或换行分隔；每次最多 {MAX_SYMBOLS} 只。示例代码用于输入格式说明。",
                height=90, max_chars=500,
            )
        with right:
            as_of = st.date_input("分析截止日", value=completed_day, max_value=completed_day, key=f"luk_date_{market}")
            st.caption("按市场时区排除当日未完成日线。节假日会使用数据中的最近交易日。")
        uploaded = None
        if mode == INPUT_MODES[1]:
            uploaded = st.file_uploader("UTF-8 CSV · 最大 5 MB", type=["csv"])
            st.caption("必需列：date, open, high, low, close, volume；多股增加 symbol 列。也支持对应中文列名。")
        use_benchmark = st.checkbox(f"读取本地基准 {'沪深300' if market == 'cn' else 'SPY'}，计算相对强度", value=mode == INPUT_MODES[0])
        with st.expander("研究参数"):
            a, b, c = st.columns(3)
            support = a.number_input("支撑带宽 / ATR", 0.1, 2.0, 0.75, 0.05)
            extension = b.number_input("延伸预警 / ATR", 2.5, 6.0, 3.0, 0.25)
            contraction = c.slider("区间收缩比例", 0.3, 0.95, 0.70, 0.05)
        submitted = st.form_submit_button("开始扫描", type="primary", width="stretch")
    parameters = LukParameters(support, extension, contraction)
    revision: tuple[object, ...] = ()
    if mode == INPUT_MODES[0]:
        revision = render_local_sync(market, symbols_text)
    elif use_benchmark:
        try:
            summary = _local_summary(market)
            revision = (str(local_market_path()), summary["last_sync"])
        except (RuntimeError, ValueError, OSError):
            revision = (str(local_market_path()), "unavailable")
    render_rules(parameters)
    if uploaded is not None and uploaded.size > MAX_CSV_BYTES:
        st.error("CSV 大小不能超过 5 MB")
        return
    content = uploaded.getvalue() if uploaded is not None else b""
    signature = (market, mode, symbols_text, as_of, use_benchmark, parameters, sha256(content).hexdigest(), revision)
    if submitted:
        st.session_state.pop(STATE_KEY, None)
        try:
            frames = read_daily_csv(content, market, symbols_text) if mode == INPUT_MODES[1] else None
            symbols = list(frames) if frames is not None else parse_symbols(symbols_text, market)
        except ValueError as exc:
            st.error(str(exc))
            return
        observations: list[LukObservation] = []
        failures: list[dict[str, str]] = []
        messages: list[str] = []
        benchmark = None
        with st.status("正在读取与检查本地日线…", expanded=True) as status:
            if use_benchmark:
                try:
                    benchmark = load_daily(BENCHMARKS[market], market, as_of, benchmark=True)
                except (RuntimeError, ValueError, OSError) as exc:
                    messages.append(f"基准加载失败：{exc}。先完成云端基准数据维护并同步到本机；个股仍可分析，相对强度留空。")
            progress = st.progress(0.0)
            for index, symbol in enumerate(symbols):
                try:
                    daily = frames[symbol] if frames is not None else load_daily(symbol, market, as_of)
                    observations.append(analyze_daily(daily, symbol=symbol, market=market, as_of=as_of, parameters=parameters, benchmark=benchmark))
                except (RuntimeError, ValueError, OSError) as exc:
                    failures.append({"代码": symbol, "问题": str(exc)})
                progress.progress((index + 1) / len(symbols), text=f"已检查 {index + 1}/{len(symbols)}：{symbol}")
            status.update(label=f"扫描结束 · 可分析 {len(observations)} 只 · 失败 {len(failures)} 只", state="complete" if observations else "error", expanded=False)
        if any(observation.data_date < str(as_of) for observation in observations):
            messages.append("部分股票的数据日期早于请求截止日，结果仅代表表中日期的历史状态；请检查停牌、休市或云端更新情况。")
        st.session_state[STATE_KEY] = {"signature": signature, "observations": observations, "failures": failures, "messages": messages}
    state = st.session_state.get(STATE_KEY)
    if state is None:
        st.info("选择自选股后可开始扫描；本地缺失的股票请先展开“本地行情库与云端同步”同步，也可上传日线CSV。")
        return
    if state["signature"] != signature:
        st.info("输入条件已变化，请重新扫描以查看对应结果。")
        return
    for message in state["messages"]:
        st.warning(message)
    if state["failures"]:
        with st.expander(f"数据问题 · {len(state['failures'])} 只", expanded=True):
            st.dataframe(pd.DataFrame(state["failures"]), hide_index=True, width="stretch")
    observations = state["observations"]
    if not observations:
        st.error("没有可分析的数据，请先同步本地日线，或检查代码与CSV内容。")
        return
    candidates = sum(any(tag in {"强势回调", "收紧待突破", "收紧突破", "U&R 收复"} for tag in o.tags) for o in observations)
    a, b, c = st.columns(3)
    a.metric("已分析", len(observations))
    b.metric("形态候选", candidates)
    c.metric("延伸 / 偏弱", sum(any(t in {"延伸预警", "结构偏弱"} for t in o.tags) for o in observations))
    st.caption(f"本次市场：{market_label} · 请求截止：{as_of} · 各股实际数据日期见表；停牌或行情延迟时可能不同。")
    filter_tag = st.selectbox("按形态查看", ["全部"] + list(dict.fromkeys(t for o in observations for t in o.tags)))
    visible = [o for o in observations if filter_tag == "全部" or filter_tag in o.tags]
    table = pd.DataFrame(o.summary() for o in visible)
    st.dataframe(table, hide_index=True, width="stretch")
    st.download_button("导出当前结果 CSV", table.to_csv(index=False).encode("utf-8-sig"), file_name=f"martin_luk_{market}_{as_of}.csv", mime="text/csv")
    if not candidates:
        st.info("本批股票未命中三类候选；普通观察与风险预警仍可查看。")
    selected = st.selectbox("打开单股观察卡", [o.symbol for o in visible])
    render_observation(next(o for o in visible if o.symbol == selected))
