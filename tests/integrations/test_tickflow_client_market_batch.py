from __future__ import annotations

import pytest

from integrations.tickflow_client import TickFlowClient


class FakeResponse:
    def __init__(self, status_code, text="", payload=None, headers=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self):
        return self._payload


def test_request_honors_tickflow_rate_limit_wait(monkeypatch):
    client = TickFlowClient(api_key="test-key", max_retries=2)
    sleeps = []
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append((url, headers, params, timeout))
        if len(calls) == 1:
            return FakeResponse(429, '{"code":"RATE_LIMITED","message":"实时行情限流 (60/min)，请 1234ms 后重试"}')
        return FakeResponse(200, payload={"data": []})

    monkeypatch.setattr("integrations.tickflow_client.requests.get", fake_get)
    monkeypatch.setattr("integrations.tickflow_client.time.sleep", sleeps.append)

    payload = client._request("/v1/quotes", params={"symbols": "AAPL.US"})

    assert payload == {"data": []}
    assert len(calls) == 2
    assert sleeps == [pytest.approx(1.734)]


def test_get_quotes_accepts_universe(monkeypatch):
    client = TickFlowClient(api_key="test-key")
    calls = []

    def fake_request(path, *, params=None):
        calls.append((path, params))
        return {"data": [{"symbol": "AAPL.US", "last_price": 205.0}]}

    monkeypatch.setattr(client, "_request", fake_request)

    quotes = client.get_quotes(universes=["US_Equity"])

    assert calls == [("/v1/quotes", {"universes": "US_Equity"})]
    assert quotes["AAPL.US"]["last_price"] == 205.0


def test_get_klines_batch_parses_payload(monkeypatch):
    client = TickFlowClient(api_key="test-key")
    calls = []

    def fake_request(path, *, params=None):
        calls.append((path, params))
        return {
            "data": {
                "AAPL.US": {
                    "timestamp": [1704067200000, 1704153600000],
                    "open": [100.0, 101.0],
                    "high": [102.0, 103.0],
                    "low": [99.0, 100.0],
                    "close": [101.0, 102.0],
                    "prev_close": [99.0, 101.0],
                    "volume": [1000, 1200],
                    "amount": [101000.0, 122400.0],
                }
            }
        }

    monkeypatch.setattr(client, "_request", fake_request)

    result = client.get_klines_batch(["AAPL.US"], period="1d", count=2, adjust="forward")

    assert calls == [
        (
            "/v1/klines/batch",
            {"symbols": "AAPL.US", "period": "1d", "count": 2, "adjust": "forward"},
        )
    ]
    assert list(result) == ["AAPL.US"]
    assert result["AAPL.US"]["close"].tolist() == [101.0, 102.0]
