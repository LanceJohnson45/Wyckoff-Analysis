# -*- coding: utf-8 -*-
from __future__ import annotations

from postgrest.exceptions import APIError

from core import stock_cache


class _FailingQuery:
    def eq(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        raise APIError(
            {
                "message": "JSON could not be generated",
                "code": 402,
                "details": "Service restricted: exceed_egress_quota",
                "hint": "upgrade plan",
            }
        )


class _FailingClient:
    def table(self, _name: str):
        return self

    def select(self, _fields: str):
        return _FailingQuery()


def test_get_cache_meta_returns_none_on_supabase_api_error(monkeypatch):
    monkeypatch.setattr(stock_cache, "postgres_enabled", lambda: False)
    monkeypatch.setattr(
        stock_cache,
        "_get_stock_cache_client",
        lambda **_kwargs: _FailingClient(),
    )

    meta = stock_cache.get_cache_meta("CN:000001", "qfq", context="background")

    assert meta is None
