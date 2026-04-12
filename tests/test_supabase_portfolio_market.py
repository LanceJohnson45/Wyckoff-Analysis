# -*- coding: utf-8 -*-
from __future__ import annotations

from integrations import supabase_portfolio as portfolio


class TestComputePortfolioStateSignature:
    def test_signature_distinguishes_cn_and_us_positions(self):
        cn_sig = portfolio.compute_portfolio_state_signature(
            1000,
            [
                {
                    "market": "cn",
                    "code": "000001",
                    "shares": 100,
                    "cost_price": 10.0,
                    "buy_dt": "20240101",
                    "strategy": "x",
                }
            ],
        )
        us_sig = portfolio.compute_portfolio_state_signature(
            1000,
            [
                {
                    "market": "us",
                    "code": "AAPL",
                    "shares": 100,
                    "cost_price": 10.0,
                    "buy_dt": "20240101",
                    "strategy": "x",
                }
            ],
        )

        assert cn_sig != us_sig

    def test_signature_accepts_us_symbols(self):
        sig = portfolio.compute_portfolio_state_signature(
            1000,
            [
                {
                    "market": "us",
                    "code": "MSFT",
                    "shares": 50,
                    "cost_price": 300.0,
                    "buy_dt": "20240101",
                    "strategy": "trend",
                }
            ],
        )

        assert isinstance(sig, str)
        assert len(sig) == 16


class _FakeExecuteResult:
    def __init__(self, data=None):
        self.data = data or []


class _FakeTable:
    def __init__(self, name: str):
        self.name = name
        self.inserted_payload = None

    def insert(self, payload):
        self.inserted_payload = payload
        return self

    def execute(self):
        return _FakeExecuteResult()


class _FakeClient:
    def __init__(self):
        self.tables: dict[str, _FakeTable] = {}

    def table(self, name: str) -> _FakeTable:
        table = self.tables.get(name)
        if table is None:
            table = _FakeTable(name)
            self.tables[name] = table
        return table


class TestSaveAiTradeOrders:
    def test_save_ai_trade_orders_persists_explicit_market(self, monkeypatch):
        client = _FakeClient()
        monkeypatch.setattr(portfolio, "is_supabase_configured", lambda: True)
        monkeypatch.setattr(portfolio, "_get_supabase_admin_client", lambda: client)

        ok = portfolio.save_ai_trade_orders(
            run_id="run-1",
            portfolio_id="USER_LIVE:1",
            model="gpt",
            trade_date="2026-04-11",
            market_view="mixed",
            orders=[
                {
                    "market": "us",
                    "code": "AAPL",
                    "name": "Apple",
                    "action": "PROBE",
                    "status": "APPROVED",
                    "shares": 100,
                    "amount": 1000,
                }
            ],
        )

        assert ok is True
        payload = client.tables[portfolio.TABLE_TRADE_ORDERS].inserted_payload
        assert payload[0]["market"] == "us"
        assert payload[0]["code"] == "AAPL"

    def test_save_ai_trade_orders_infers_market_from_symbol(self, monkeypatch):
        client = _FakeClient()
        monkeypatch.setattr(portfolio, "is_supabase_configured", lambda: True)
        monkeypatch.setattr(portfolio, "_get_supabase_admin_client", lambda: client)

        ok = portfolio.save_ai_trade_orders(
            run_id="run-2",
            portfolio_id="USER_LIVE:1",
            model="gpt",
            trade_date="2026-04-11",
            market_view="mixed",
            orders=[
                {
                    "code": "MSFT",
                    "name": "Microsoft",
                    "action": "PROBE",
                    "status": "APPROVED",
                    "shares": 100,
                    "amount": 1000,
                },
                {
                    "code": "600519",
                    "name": "贵州茅台",
                    "action": "HOLD",
                    "status": "APPROVED",
                    "shares": 100,
                    "amount": 0,
                },
            ],
        )

        assert ok is True
        payload = client.tables[portfolio.TABLE_TRADE_ORDERS].inserted_payload
        assert [row["market"] for row in payload] == ["us", "cn"]
