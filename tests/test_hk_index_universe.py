from __future__ import annotations

from integrations.hk_index_universe import (
    _extract_symbol_map_from_html,
    _normalize_hk_symbol,
    _normalize_name,
)


def test_normalize_hk_symbol_zero_pads_and_suffixes():
    assert _normalize_hk_symbol("700") == "0700.HK"
    assert _normalize_hk_symbol("0700.HK") == "0700.HK"
    assert _normalize_hk_symbol("9988") == "9988.HK"


def test_extract_symbol_map_from_html_reads_embedded_json():
    html = '{"flag":{"name":"Tencent Holdings","code":"HK"},"name":{"label":"Tencent Holdings","title":"Tencent Holdings Ltd","derived":false,"url":"/equities/tencent-holdings-hk"},"symbol":"0700","ticker":"","exchangeId":"21"}'
    mapping = _extract_symbol_map_from_html(html)
    assert mapping[_normalize_name("Tencent Holdings")] == "0700.HK"
