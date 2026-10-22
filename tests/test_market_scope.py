"""排除板块在维护、缓存和独立分析入口中的一致性。"""

from datetime import date

import pandas as pd
import pytest

from core.market_scope import excluded_cn_board, require_supported_stock
from integrations.fetch_a_share_csv import _normalize_symbols


@pytest.mark.parametrize("code", ["920002","430047","830001","870001","880001","688981","689009","sh688001","688001.SH","688001.XSHG","BJ.920002"])
def test_excluded_boards_and_common_code_formats(code: str) -> None:
    """证券前后缀不能绕过板块限制。"""
    assert excluded_cn_board(code)
    with pytest.raises(ValueError, match="不更新、不分析"):
        require_supported_stock(code)


@pytest.mark.parametrize("market,code", [("cn","600519"),("cn","000001"),("cn","300750"),("cn","301569"),("cn","000300"),("us","AAPL"),("us","688001"),("hk","00688.HK")])
def test_main_chinext_benchmarks_and_other_markets_unchanged(market: str, code: str) -> None:
    """不误拦主板、创业板、基准及其他市场。"""
    assert not excluded_cn_board(code,market)
    require_supported_stock(code,market)


def test_batch_pool_excludes_before_fetching() -> None:
    """全池及手工股票池公用同一个过滤入口。"""
    assert _normalize_symbols(["688001","920002","600519","000001","300750","689009","430047"]) == ["600519","000001","300750"]


@pytest.mark.parametrize("symbol", ["688001","920002"])
def test_cached_history_cannot_bypass_scope(monkeypatch: pytest.MonkeyPatch, symbol: str) -> None:
    """即便缓存已有行情，也不读取或拉取排除板块。"""
    from integrations import stock_hist_repository as repo

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("excluded stock reached storage")

    monkeypatch.setattr(repo,"_load_from_md_tables",forbidden)
    monkeypatch.setattr(repo,"get_cache_meta",forbidden)
    with pytest.raises(ValueError,match="不更新、不分析"):
        repo.get_stock_hist(symbol,date(2026,1,1),date(2026,10,22),cache_only=True)


@pytest.mark.parametrize("symbol", ["688001","920002"])
def test_direct_analysis_of_existing_or_uploaded_frames_is_rejected(symbol: str) -> None:
    """直接传入已缓存或导入的数据同样不能分析。"""
    from core.holding_diagnostic import diagnose_one_stock, diagnose_holdings
    from core.martin_luk import analyze_daily

    with pytest.raises(ValueError,match="不更新、不分析"):
        diagnose_one_stock(symbol,"sample",0,pd.DataFrame())
    with pytest.raises(ValueError,match="不更新、不分析"):
        analyze_daily(pd.DataFrame(),symbol=symbol,market="cn",as_of=date(2026,10,22))
    assert diagnose_holdings([(symbol,"sample",0)],{symbol:pd.DataFrame()}) == []


def test_manual_page_input_is_rejected_without_optional_streamlit() -> None:
    """直接验证页面解析函数，Streamlit组件另由可选页面测试覆盖。"""
    import ast
    from pathlib import Path
    import re

    source=Path('app/martin_luk_view.py').read_text()
    function=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='parse_symbols')
    namespace={'re':re,'LUK_DEFAULT_SYMBOLS':{'cn':[], 'us':[]},'MAX_SYMBOLS':20,'require_supported_stock':require_supported_stock}
    exec(compile(ast.Module(body=[function],type_ignores=[]),'app/martin_luk_view.py:parse_symbols','exec'),namespace)
    with pytest.raises(ValueError,match='不更新、不分析'):
        namespace['parse_symbols']('600519,688001','cn')
    assert namespace['parse_symbols']('600519,300750','cn')==['600519','300750']


def test_batch_ai_report_skips_excluded_stocks_before_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """手工研报输入也必须在取数或调用模型之前过滤。"""
    from scripts import step3_batch_report as step3

    monkeypatch.setattr(step3,"_job_end_calendar_day",lambda:pytest.fail("excluded batch reached I/O preparation"))
    assert step3.run(['688001','920002'],'','','',notify=False)==(True,'skipped_excluded_cn_boards','')


def test_portfolio_analysis_refuses_excluded_holdings_without_altering_account(monkeypatch: pytest.MonkeyPatch) -> None:
    """不通过删除持仓或重算资产来绕过排除范围。"""
    from types import SimpleNamespace
    from scripts import step4_rebalancer as step4

    portfolio=SimpleNamespace(positions=[SimpleNamespace(code='688001',market='cn')])
    monkeypatch.setattr(step4,'load_portfolio_from_supabase',lambda _: (portfolio,'fixture','unchanged'))
    monkeypatch.setattr(step4,'call_llm',lambda *args,**kwargs:pytest.fail('excluded portfolio reached model'))
    result=step4.run('',None,'test-key','test-model',portfolio_id='test',tg_bot_token='',tg_chat_id='')
    assert result[0] is False and result[1].startswith('excluded_cn_board')
    assert portfolio.positions[0].code=='688001'
