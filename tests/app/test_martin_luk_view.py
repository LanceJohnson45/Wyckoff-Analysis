"""真实 Streamlit 组件执行与 CSV 输入边界验证。"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.martin_luk_view import parse_symbols, read_daily_csv

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def isolated_local_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """UI测试不读取正在同步的真实本地文件。"""
    monkeypatch.setenv("LOCAL_MARKET_DB_PATH", str(tmp_path / "ui-market.sqlite3"))


def test_normal_csv_preserves_a_share_leading_zero() -> None:
    """【正常值】中文上传列正确映射，A股代码前导零不丢失。"""
    content = "代码,日期,开盘,最高,最低,收盘,成交量\n000001,2025-01-02,10,11,9,10,1000\n".encode()

    frames = read_daily_csv(content, "cn", "")

    assert list(frames) == ["000001"]
    assert frames["000001"]["close"].tolist() == ["10"]


@pytest.mark.parametrize("text,market", [("600519,AAPL", "cn"), ("<script>", "us"), ("", "cn"), (",".join(f"{i:06}" for i in range(21)), "cn")], ids=["mixed-market", "bad-symbol", "empty", "too-many"])
def test_param_symbol_input_rejects_invalid_or_excessive_lists(text: str, market: str) -> None:
    """【参数校验】输入错误应显示为校验失败，不能截断或误扫市场。"""
    with pytest.raises(ValueError):
        parse_symbols(text, market)


@pytest.mark.parametrize("content", [b"", b"x" * (5 * 1024 * 1024 + 1), b"symbol,close\n,10\n"], ids=["empty", "oversized", "missing-symbol"])
def test_error_bad_csv_is_explicit(content: bytes) -> None:
    """【异常值】空文件、超限文件和缺失代码不能静默通过。"""
    with pytest.raises(ValueError):
        read_daily_csv(content, "cn", "000001")


def test_normal_web_scan_filter_and_card_use_real_analysis(monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】网页提交完整走过分析器、结果表、过滤器和观察卡。"""
    close = pd.Series([40 + i * 0.1 for i in range(120)])
    frame = pd.DataFrame({"date": pd.bdate_range("2025-01-01", periods=120), "open": close, "high": close + 0.6, "low": close - 0.6, "close": close, "volume": 1000})
    frame.attrs["source"] = "测试日线"

    def daily(symbol: str, market: str, as_of: date, *, benchmark: bool = False) -> pd.DataFrame:
        return frame.copy()

    # 仅替换外部行情 I/O；组件、业务计算与状态保存均真实执行。
    monkeypatch.setattr("app.martin_luk_view.load_daily", daily)
    app = AppTest.from_file(str(ROOT / "martin_luk_app.py")).run(timeout=20)
    app.radio[0].set_value("美股").run()
    app.text_area[0].set_value("AAPL,MSFT")
    app.button[0].click().run(timeout=20)

    assert not app.exception
    assert any(metric.label == "已分析" and metric.value == "2" for metric in app.metric)
    assert {"AAPL", "MSFT"} == set(app.selectbox[1].options)
    assert len(app.dataframe[0].value) == 2
    app.selectbox[1].set_value("MSFT").run()
    assert any("MSFT" in heading.value for heading in app.subheader)
    assert not app.exception
    app.radio[0].set_value("A 股").run()
    assert any("输入条件已变化" in item.value for item in app.info)
    assert not app.dataframe


def test_error_web_invalid_input_shows_error_without_results() -> None:
    """【异常值】代码校验失败时展示错误，不留上次结果假装成功。"""
    app = AppTest.from_file(str(ROOT / "martin_luk_app.py")).run(timeout=20)
    app.text_area[0].set_value("AAPL")
    app.button[0].click().run(timeout=20)

    assert not app.exception
    assert any("代码格式不符" in error.value for error in app.error)
    assert not app.dataframe


def test_normal_web_partial_failure_keeps_good_observation(monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】基准和个别股票失败时，可用股票仍出卡，失败有明确提示。"""
    close = pd.Series([40 + i * 0.1 for i in range(120)])
    frame = pd.DataFrame({"date": pd.bdate_range("2025-01-01", periods=120), "open": close, "high": close + 0.6, "low": close - 0.6, "close": close, "volume": 1000})

    def daily(symbol: str, market: str, as_of: date, *, benchmark: bool = False) -> pd.DataFrame:
        if benchmark or symbol == "MSFT":
            raise RuntimeError("测试行情源不可用")
        return frame.copy()

    monkeypatch.setattr("app.martin_luk_view.load_daily", daily)
    app = AppTest.from_file(str(ROOT / "martin_luk_app.py")).run(timeout=20)
    app.radio[0].set_value("美股").run()
    app.text_area[0].set_value("AAPL,MSFT")
    app.button[0].click().run(timeout=20)

    assert not app.exception
    assert any("基准加载失败" in item.value for item in app.warning)
    assert app.dataframe[0].value.iloc[0]["代码"] == "MSFT"
    assert any(metric.label == "已分析" and metric.value == "1" for metric in app.metric)
    assert any("AAPL" in heading.value for heading in app.subheader)


def test_normal_csv_data_reaches_analysis_and_export() -> None:
    """【正常值】上传的字符串价格量能实际进入分析并导出研究参数。"""
    from core.martin_luk import LukParameters, analyze_daily

    closes = [40 + i * 0.1 for i in range(120)]
    frame = pd.DataFrame({"symbol": "000001", "date": pd.bdate_range("2025-01-01", periods=120), "open": closes, "high": [c + 0.6 for c in closes], "low": [c - 0.6 for c in closes], "close": closes, "volume": 1000})
    content = frame.to_csv(index=False).encode("utf-8-sig")

    uploaded = read_daily_csv(content, "cn", "")
    result = analyze_daily(uploaded["000001"], symbol="000001", market="cn", as_of=date(2025, 6, 17), parameters=LukParameters(support_atr=0.5))
    exported = result.summary()

    assert exported["代码"] == "000001"
    assert exported["支撑带宽/ATR"] == 0.5
    assert "CSV" in exported["数据源"]
    assert exported["数据日期"] == "2025-06-17"


def test_normal_web_reads_actual_local_sqlite_offline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】禁网时真实SQLite日线仍能经网页生成观察卡，无行情I/O替身。"""
    from datetime import datetime
    from integrations.local_market_store import DailyBar, LocalMarketWriter, local_engine

    path = tmp_path / "offline.sqlite3"
    monkeypatch.setenv("LOCAL_MARKET_DB_PATH", str(path))
    updated = datetime.fromisoformat("2026-10-10T12:00:00+08:00")
    dates = pd.bdate_range("2026-01-01", periods=120)
    bars = [DailyBar("cn", "stock", "000001", day.date(), 40 + i * 0.1, 41 + i * 0.1, 39 + i * 0.1, 40 + i * 0.1, 1000, updated) for i, day in enumerate(dates)]
    with local_engine(path, writable=True) as engine:
        writer = LocalMarketWriter(engine, "offline-test")
        writer.write(bars)
        writer.complete("cn:all", "cn", updated)

    app = AppTest.from_file(str(ROOT / "martin_luk_app.py")).run(timeout=20)
    app.text_area[0].set_value("000001")
    app.checkbox[0].uncheck()
    app.button[0].click().run(timeout=20)

    assert not app.exception
    assert any("000001" in heading.value for heading in app.subheader)
    assert "本地SQLite" in app.dataframe[0].value.iloc[0]["数据源"]
    assert any("早于请求截止日" in warning.value for warning in app.warning)


def test_error_web_missing_local_store_does_not_fetch_online(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】缺本地库时只提示同步，禁网环境下不尝试在线行情兜底。"""
    path = tmp_path / "missing.sqlite3"
    monkeypatch.setenv("LOCAL_MARKET_DB_PATH", str(path))
    app = AppTest.from_file(str(ROOT / "martin_luk_app.py")).run(timeout=20)
    app.button[0].click().run(timeout=20)

    assert not app.exception
    assert any("先同步本地日线" in error.value for error in app.error)
    assert not path.exists()
