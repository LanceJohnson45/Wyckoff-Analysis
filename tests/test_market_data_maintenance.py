"""上市边界、日线覆盖和人工审核的行为回归。"""

from datetime import date
import json
from pathlib import Path

import pandas as pd
import pytest

from integrations.fetch_a_share_csv import TradingWindow
from integrations.market_data_policy import BeforeListingRange, exclusion_applies, load_security_overrides
from integrations.market_data_review import write_review
from scripts import funnel_prewarm as prewarm


@pytest.fixture
def maintenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    """【正常值】隔离行情I/O，真实执行缺口处理、主入口和文件写出。"""
    dates = list(pd.bdate_range("2026-03-02", "2026-10-15").date)
    cache: dict[str, list[date]] = {}
    requests = []
    monkeypatch.setattr(prewarm, "_PREWARM_RUN_STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(prewarm, "_RECENT_GAP_MAX_AGE_DAYS", 0)
    monkeypatch.setattr(prewarm, "_job_end_calendar_day", lambda: dates[-1])
    monkeypatch.setattr(prewarm, "_resolve_trading_window", lambda **kwargs: TradingWindow(dates[0], kwargs["end_calendar_day"]))
    monkeypatch.setattr(prewarm, "_resolve_us_window", lambda **kwargs: TradingWindow(dates[0], kwargs["end_calendar_day"]))
    monkeypatch.setattr(prewarm, "_expected_trade_dates", lambda window, market: [d for d in dates if d <= window.end_trade_date])
    monkeypatch.setattr(prewarm, "load_cached_dates", lambda symbol, *args, **kwargs: cache.get(symbol, []).copy())
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)

    def fetch(*, symbol: str, market: str, start: date, end: date, **kwargs: object) -> pd.DataFrame:
        requests.append((symbol, start, end))
        return pd.DataFrame({"date": [str(d) for d in dates if start <= d <= end], "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1000})

    def write(*, symbol: str, df: pd.DataFrame, **kwargs: object) -> bool:
        cache.setdefault(symbol, []).extend(pd.to_datetime(df["date"]).dt.date)
        return True

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    monkeypatch.setattr(prewarm, "upsert_cache_data", write)
    return {"dates": dates, "cache": cache, "requests": requests, "fetch": fetch, "path": tmp_path}


def test_normal_known_ipo_does_not_request_prelisting_data(maintenance: dict) -> None:
    """【正常值】001237不足320根是新上市，不请求上市前区间且近期补齐。"""
    ipo = date(2026, 5, 22)
    maintenance["cache"]["001237"] = [d for d in maintenance["dates"] if ipo <= d <= date(2026, 10, 8)]

    result = prewarm._prefetch_one("001237", "cn", 320, listing_date=ipo)

    assert result[1] == "gap_repaired"
    assert all(start >= ipo for _, start, _ in maintenance["requests"])
    assert result[8]["latest_date"] == "2026-10-15"
    assert result[8]["listing_date"] == "2026-05-22"


@pytest.mark.parametrize("market,symbol", [("cn", "001257"), ("us", "NEWIPO")], ids=["cn-ipo", "us-ipo"])
def test_normal_provider_listing_boundary_is_not_delisting(maintenance: dict, monkeypatch: pytest.MonkeyPatch, market: str, symbol: str) -> None:
    """【正常值】供应商确认上市边界时保留股票，先更新近期再排除上市前空档。"""
    ipo = date(2026, 3, 31)
    key = symbol if market == "cn" else f"US:{symbol}"
    maintenance["cache"][key] = [d for d in maintenance["dates"] if ipo <= d <= date(2026, 10, 8)]
    order = []

    def fetch(**kwargs: object) -> pd.DataFrame:
        order.append(kwargs["end"])
        if kwargs["end"] < ipo:
            raise BeforeListingRange(ipo)
        return maintenance["fetch"](**kwargs)

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    result = prewarm._prefetch_one(symbol, market, 320)

    assert result[1] == "gap_repaired"
    assert order[0] == date(2026, 10, 15)
    assert result[8]["latest_date"] == "2026-10-15"
    assert result[8]["listing_date"] == str(ipo)
    assert not result[8]["errors"]


def test_error_old_gap_failure_keeps_latest_repair_and_requires_review(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】未知旧空档不擅自当上市前，近期仍更新，但任务保留失败供审核。"""
    maintenance["cache"]["001257"] = [d for d in maintenance["dates"] if date(2026, 3, 31) <= d <= date(2026, 10, 8)]

    def fetch(**kwargs: object) -> pd.DataFrame:
        if kwargs["end"] < date(2026, 3, 31):
            raise RuntimeError("possibly delisted; no timezone found")
        return maintenance["fetch"](**kwargs)

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    result = prewarm._prefetch_one("001257", "cn", 320)

    assert result[1] == "gap_partial"
    assert result[8]["latest_date"] == "2026-10-15"
    assert result[8]["listing_date"] == ""
    assert "possibly delisted" in result[8]["errors"][0]


def test_normal_cold_start_uses_explicit_provider_listing_metadata(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】新股首次完整拉取按元数据截断预期交易日，不补造上市前K线。"""
    ipo = date(2026, 5, 22)

    def fetch(**kwargs: object) -> pd.DataFrame:
        frame = maintenance["fetch"](**kwargs)
        frame = frame.loc[frame["date"] >= str(ipo)].copy()
        frame.attrs["listing_date"] = str(ipo)
        return frame

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    result = prewarm._prefetch_one("001237", "cn", 320)

    assert result[1] == "gap_repaired"
    assert min(maintenance["cache"]["001237"]) == ipo
    assert result[4] == len([d for d in maintenance["dates"] if d >= ipo])


def test_normal_main_freezes_target_when_run_crosses_day_switch(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【边界值】模拟跨过16点，整批仍以启动时10月15日核验和写报表。"""
    original_fetch = maintenance["fetch"]

    def fetch(**kwargs: object) -> pd.DataFrame:
        monkeypatch.setattr(prewarm, "_job_end_calendar_day", lambda: date(2026, 10, 16))
        return original_fetch(**kwargs)

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    monkeypatch.setattr("sys.argv", ["prewarm", "--market", "cn", "--symbols", "000001,000002", "--max-workers", "1", "--review-dir", str(maintenance["path"])])

    code = prewarm.main()
    coverage = pd.read_csv(maintenance["path"] / "prewarm_cn_2026-10-15_coverage.csv", dtype=str)

    assert code == 0
    assert set(coverage["target_date"]) == {"2026-10-15"}
    assert set(coverage["latest_date"]) == {"2026-10-15"}
    assert all(end == date(2026, 10, 15) for _, _, end in maintenance["requests"])


@pytest.mark.parametrize("kind", ["partial", "failed"], ids=["remaining-gap", "source-error"])
def test_error_main_partial_success_returns_nonzero_and_persists_review(maintenance: dict, monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    """【异常值】一只成功不能掩盖另一只失败/缺口；报表与检查点真实保存。"""
    def fetch(**kwargs: object) -> pd.DataFrame:
        if kwargs["symbol"] == "000002":
            if kind == "failed":
                raise RuntimeError("Connection reset by peer; possibly delisted")
            return maintenance["fetch"](**kwargs).iloc[:-1]
        return maintenance["fetch"](**kwargs)

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    monkeypatch.setattr("sys.argv", ["prewarm", "--market", "cn", "--symbols", "000001,000002", "--review-dir", str(maintenance["path"])])

    code = prewarm.main()
    report = pd.read_csv(maintenance["path"] / "prewarm_cn_2026-10-15_review.csv", dtype=str)
    state = json.loads((maintenance["path"] / "state.json").read_text())["cn"]

    assert code == 2
    assert report["symbol"].tolist() == ["000002"]
    assert report.iloc[0]["status"] == ("failed" if kind == "failed" else "gap_partial")
    assert state["status"] == "partial"
    assert set(state["ready_symbols"]) == {"000001"}
    if kind == "failed":
        assert report.iloc[0]["reason"] == "source_or_network_error"


def test_normal_all_cn_pool_keeps_st_star_and_bse_but_skips_approved_delisting(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】数据维护池不漏ST/科创/北交所；已批准000004只退出拉取。"""
    items = [{"code": "000004", "name": "国华退"}, {"code": "000010", "name": "*ST美丽"}, {"code": "688001", "name": "科创样本"}, {"code": "920001", "name": "北交样本"}]
    monkeypatch.setattr(prewarm, "get_all_stocks", lambda **kwargs: items)
    monkeypatch.setattr("sys.argv", ["prewarm", "--market", "cn", "--universe", "all", "--review-dir", str(maintenance["path"])])

    code = prewarm.main()
    coverage = pd.read_csv(maintenance["path"] / "prewarm_cn_2026-10-15_coverage.csv", dtype=str).set_index("symbol")

    assert code == 0
    assert set(maintenance["cache"]) == {"000010", "688001", "920001"}
    assert coverage.loc["000004", "status"] == "excluded"
    assert coverage.loc["000004", "reason"] == "confirmed_delisted"
    assert len(coverage) == 4


def test_boundary_old_manifest_is_revalidated(maintenance: dict) -> None:
    """【边界值】旧版可能误报成功的检查点不再直接跳过整轮。"""
    prewarm._save_prewarm_run_state({"cn": {"status": "ok", "end_trade_date": "2026-10-15", "trading_days": 320,
                                              "symbol_count": 1, "symbol_digest": prewarm._symbol_digest(["000001"])}})

    skip, _ = prewarm._should_skip_prewarm_run(market="cn", trading_days=320, end_trade_date=date(2026, 10, 15), symbols=["000001"])

    assert not skip
    assert not prewarm._symbol_manifest_record_is_ready({"status": "ready", "end_trade_date": "2026-10-15", "trading_days": 320}, end_trade_date=date(2026, 10, 15), trading_days=320)


def test_normal_review_files_include_pending_and_excluded_and_redact_keys(tmp_path: Path) -> None:
    """【正常值】检查点含未完成项，审核CSV保护密钥与表格公式，排除记录仍可审计。"""
    records = {"AAPL": {"symbol": "AAPL", "status": "pending", "detail": "token=secret"},
               "CTRA": {"symbol": "CTRA", "status": "excluded"},
               "MSFT": {"symbol": "MSFT", "status": "failed", "detail": "=HYPERLINK(unsafe)"}}

    summary = write_review(tmp_path, "us", "2026-10-15", records, pool_mode="sp500", status="partial")
    review = pd.read_csv(tmp_path / "prewarm_us_2026-10-15_review.csv", dtype=str)
    coverage = pd.read_csv(tmp_path / "prewarm_us_2026-10-15_coverage.csv", dtype=str)

    assert summary["counts"] == {"pending": 1, "excluded": 1, "failed": 1}
    assert len(coverage) == 3 and set(review["symbol"]) == {"AAPL", "MSFT"}
    assert "secret" not in (tmp_path / "prewarm_us_2026-10-15_review.csv").read_text()
    assert review.loc[review["symbol"] == "MSFT", "detail"].iloc[0].startswith("'=")


def test_normal_manual_exclusion_has_evidence_and_effective_date() -> None:
    """【正常值】退市排除只对确认后的目标日期生效，历史行情不会删除。"""
    records = load_security_overrides()

    assert not exclusion_applies(records[("cn", "000004")], date(2026, 7, 13))
    assert exclusion_applies(records[("cn", "000004")], date(2026, 7, 14))
    assert exclusion_applies(records[("us", "CTRA")], date(2026, 10, 15))


def test_boundary_yunchuang_exclusion_starts_on_delisting_date() -> None:
    """【边界值】云创退仅从摘牌日排除，最后交易日的历史维护仍保留。"""
    record = load_security_overrides().get(("cn", "920305"), {})

    assert not exclusion_applies(record, date(2026, 7, 29))
    assert exclusion_applies(record, date(2026, 7, 30))
    assert record["reason"] == "confirmed_delisted"
    assert record["evidence"].startswith("https://www.bse.cn/disclosure/")


def test_normal_main_excludes_delisted_yunchuang_and_preserves_history(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】名单残留云创退时不补退市后行情，保留缓存及排除审计。"""
    history = [date(2026, 7, 29)]
    maintenance["cache"]["920305"] = history.copy()
    items = [{"code": "920305", "name": "*ST云创"}, {"code": "920001", "name": "北交样本"}]

    def fetch(**kwargs: object) -> pd.DataFrame:
        if kwargs["symbol"] == "920305":
            raise RuntimeError("退市后无行情")
        return maintenance["fetch"](**kwargs)

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    monkeypatch.setattr(prewarm, "get_all_stocks", lambda **kwargs: items)
    monkeypatch.setattr("sys.argv", ["prewarm", "--market", "cn", "--universe", "all", "--review-dir", str(maintenance["path"])])

    code = prewarm.main()
    coverage = pd.read_csv(maintenance["path"] / "prewarm_cn_2026-10-15_coverage.csv", dtype=str).set_index("symbol")
    review = pd.read_csv(maintenance["path"] / "prewarm_cn_2026-10-15_review.csv", dtype=str)

    assert code == 0
    assert maintenance["cache"]["920305"] == history
    assert all(symbol != "920305" for symbol, _, _ in maintenance["requests"])
    assert coverage.loc["920305", "status"] == "excluded"
    assert coverage.loc["920305", "reason"] == "confirmed_delisted"
    assert coverage.loc["920305", "evidence"] == load_security_overrides()[("cn", "920305")]["evidence"]
    assert review.empty


def test_error_forced_failure_invalidates_previous_ready_checkpoint(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【边界值】强制重跑失败后，不得沿用旧ready令下一次跳过该股票。"""
    prewarm._save_prewarm_progress(run_state={}, market="cn", status="ok", end_trade_date=date(2026, 10, 15), trading_days=320,
        symbols=["000002"], pool_mode="cli_symbols", cache_ready=1, fast_skipped=0, repaired_symbols=0, repaired_ranges=0, repaired_rows=0,
        ready_updates={"000002": prewarm._ready_symbol_manifest_entry(end_trade_date=date(2026, 10, 15), trading_days=320, expected_count=1, cached_count=1)})
    monkeypatch.setattr(prewarm, "fetch_stock_hist", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("Connection reset")))
    monkeypatch.setattr("sys.argv", ["prewarm", "--market", "cn", "--symbols", "000002", "--force", "--review-dir", str(maintenance["path"])])

    code = prewarm.main()
    state = json.loads((maintenance["path"] / "state.json").read_text())["cn"]

    assert code == 2
    assert state["status"] == "partial"
    assert state["ready_symbols"] == {}


def test_error_stale_universe_fallback_cannot_report_complete_success(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】名单刷新失败时仍更新已知股票，但不能声称全市场覆盖成功。"""
    def universe(**kwargs: object) -> list[dict[str, str]]:
        kwargs["on_warning"]("名单刷新失败，沿用旧缓存")
        return [{"code": "000001", "name": "示例"}]

    monkeypatch.setattr(prewarm, "get_all_stocks", universe)
    monkeypatch.setattr("sys.argv", ["prewarm", "--market", "cn", "--universe", "all", "--review-dir", str(maintenance["path"])])

    code = prewarm.main()
    summary = json.loads((maintenance["path"] / "prewarm_cn_2026-10-15_summary.json").read_text())

    assert code == 2
    assert summary["status"] == "partial"
    assert "名单刷新失败" in summary["universe_warning"]
    assert maintenance["cache"]["000001"][-1] == date(2026, 10, 15)


def test_error_failed_symbols_stay_invalid_after_multiple_checkpoints(maintenance: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """【边界值】检查点替换内存字典后，后续失败仍清除该股票的旧ready。"""
    symbols = ["000001", "000002"]
    entries = {symbol: prewarm._ready_symbol_manifest_entry(end_trade_date=date(2026, 10, 15), trading_days=320, expected_count=1, cached_count=1) for symbol in symbols}
    prewarm._save_prewarm_progress(run_state={}, market="cn", status="ok", end_trade_date=date(2026, 10, 15), trading_days=320,
        symbols=symbols, pool_mode="cli_symbols", cache_ready=2, fast_skipped=0, repaired_symbols=0, repaired_ranges=0, repaired_rows=0, ready_updates=entries)
    monkeypatch.setattr(prewarm, "_PREWARM_CHECKPOINT_EVERY", 1)
    monkeypatch.setattr(prewarm, "as_completed", lambda futures: iter(futures))
    monkeypatch.setattr(prewarm, "fetch_stock_hist", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("Connection reset")))
    monkeypatch.setattr("sys.argv", ["prewarm", "--market", "cn", "--symbols", ",".join(symbols), "--force", "--review-dir", str(maintenance["path"])])

    assert prewarm.main() == 2
    state = json.loads((maintenance["path"] / "state.json").read_text())["cn"]
    assert state["ready_symbols"] == {}


def test_normal_force_refresh_bypasses_checkout_timestamp_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】Git checkout刷新mtime不能让每日任务永久使用仓库旧股票名单。"""
    from integrations import fetch_a_share_csv as source

    cache = tmp_path / "data" / "stock_list_cache.json"
    cache.parent.mkdir()
    cache.write_text(json.dumps([{"code": "000001", "name": "旧名单"}]))
    monkeypatch.setattr(source, "__file__", str(tmp_path / "integrations" / "source.py"))
    monkeypatch.setattr(source.ak, "stock_info_a_code_name", lambda: pd.DataFrame([{"code": "688001", "name": "新名单"}]))

    old = source.get_all_stocks()
    fresh = source.get_all_stocks(force_refresh=True)

    assert old[0]["code"] == "000001"
    assert fresh[0]["code"] == "688001"
    assert json.loads(cache.read_text())[0]["code"] == "688001"


def test_error_empty_universe_provider_preserves_last_good_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】空上游名单不覆盖有效缓存，并把未确认覆盖原因交给任务摘要。"""
    from integrations import fetch_a_share_csv as source

    cache = tmp_path / "data" / "stock_list_cache.json"
    cache.parent.mkdir()
    cache.write_text(json.dumps([{"code": "000001", "name": "有效旧名单"}]))
    monkeypatch.setattr(source, "__file__", str(tmp_path / "integrations" / "source.py"))
    monkeypatch.setattr(source.ak, "stock_info_a_code_name", lambda: pd.DataFrame())
    warnings = []

    result = source.get_all_stocks(force_refresh=True, on_warning=warnings.append)

    assert result[0]["code"] == "000001"
    assert len(json.loads(cache.read_text())) == 1
    assert warnings and "旧缓存" in warnings[0]


def test_normal_confirmed_listing_boundary_survives_daily_checkpoint() -> None:
    """【正常值】已确认首次交易日跨日复用，人工修订仍具有更高优先级。"""
    ready = {"US:NEWIPO": prewarm._ready_symbol_manifest_entry(end_trade_date=date(2026, 10, 15), trading_days=320, expected_count=50, cached_count=50, listing_date="2026-05-22")}

    inferred = prewarm._confirmed_listing_date("us", "NEWIPO", {}, ready)
    manual = prewarm._confirmed_listing_date("us", "NEWIPO", {("us", "NEWIPO"): {"listing_date": "2026-05-20"}}, ready)

    assert inferred == date(2026, 5, 22)
    assert manual == date(2026, 5, 20)


@pytest.mark.parametrize("representation", ["epoch", "timestamp", "date"], ids=["raw-yahoo-epoch", "sdk-datetime", "sdk-date"])
def test_normal_yahoo_listing_metadata_accepts_sdk_date_formats(monkeypatch: pytest.MonkeyPatch, representation: str) -> None:
    """【正常值】真实SDK已将firstTradeDate转换为日期时仍能正确识别上市边界。"""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import yfinance as yf
    from integrations.market_data_policy import yahoo_first_trade_date

    instant = datetime(2026, 3, 31, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    value = instant.timestamp() if representation == "epoch" else instant if representation == "timestamp" else instant.date()

    class Ticker:
        def history(self, **kwargs: object) -> pd.DataFrame:
            return pd.DataFrame()

        def get_history_metadata(self) -> dict:
            return {"firstTradeDate": value, "exchangeTimezoneName": "Asia/Shanghai"}

    monkeypatch.setattr(yf, "Ticker", lambda symbol: Ticker())

    result = yahoo_first_trade_date("001257.SZ")

    assert result == date(2026, 3, 31)


def test_normal_job_log_survives_vendor_stdout_redirection(capfd: pytest.CaptureFixture[str]) -> None:
    """【正常值】SDK临时重定向stdout时，主任务状态日志仍输出且带UTC+8时区。"""
    from contextlib import redirect_stdout
    from io import StringIO

    hidden = StringIO()

    with redirect_stdout(hidden):
        prewarm._log("prewarm test status")
    line = capfd.readouterr().out

    event = json.loads(line)
    assert event["event"] == "prewarm test status"
    assert event["timestamp"].endswith("+08:00")
    assert hidden.getvalue() == ""
