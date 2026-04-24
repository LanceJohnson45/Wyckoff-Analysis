# -*- coding: utf-8 -*-
from __future__ import annotations

from scripts import db_maintenance


class _DeleteQuery:
    def __init__(self, error: Exception | None):
        self._error = error

    def lt(self, _date_col: str, _cutoff):
        return self

    def execute(self):
        if self._error is not None:
            raise self._error
        return object()


class _TableClient:
    def __init__(self, error: Exception | None):
        self._error = error

    def delete(self):
        return _DeleteQuery(self._error)


class _Client:
    def __init__(self, error: Exception | None):
        self._error = error

    def table(self, _name: str):
        return _TableClient(self._error)


def test_cleanup_table_skips_missing_supabase_table():
    err = Exception(
        "{'message': \"Could not find the table 'public.signal_pending' in the schema cache\", 'code': 'PGRST205'}"
    )

    status, count = db_maintenance.cleanup_table(
        _Client(err),
        "signal_pending",
        "signal_date",
        15,
        "iso_date",
    )

    assert status == "skip_missing_table"
    assert count is None


def test_cleanup_table_preserves_real_errors():
    status, count = db_maintenance.cleanup_table(
        _Client(Exception("boom")),
        "signal_pending",
        "signal_date",
        15,
        "iso_date",
    )

    assert status == "error: boom"
    assert count is None
