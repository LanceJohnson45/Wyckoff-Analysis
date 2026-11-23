from scripts import quality_gate
from scripts.quality_gate import DEFAULT_FUNC_LIMIT, SOFT_FUNC_TARGET_LINES, function_line_limit


def test_function_line_limits_are_layer_specific() -> None:
    assert SOFT_FUNC_TARGET_LINES == 50
    assert function_line_limit("core/wyckoff_engine.py") == DEFAULT_FUNC_LIMIT
    assert function_line_limit("integrations/data_source.py") == DEFAULT_FUNC_LIMIT
    assert function_line_limit("workflows/wyckoff_funnel.py") == DEFAULT_FUNC_LIMIT
    assert function_line_limit("cli/tui.py") == 100
    assert function_line_limit("stock_research/cli.py") == 100
    assert function_line_limit("stock_research/pipeline.py") == DEFAULT_FUNC_LIMIT
    assert function_line_limit("web/apps/web/src/routes/chat.tsx") == 120
    assert function_line_limit("web/apps/web/src/components/market-bar.tsx") == 90
    assert function_line_limit("web/packages/shared/src/chat-tools.ts") == DEFAULT_FUNC_LIMIT


def test_whitelisted_function_growth_fails(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_whitelist", lambda: {"core/foo.py::legacy": 75})
    monkeypatch.setattr(quality_gate, "scan_py_functions", lambda _dirs: [("core/foo.py", "legacy", 80, 70)])
    monkeypatch.setattr(quality_gate, "scan_ts_functions", lambda _dirs: [])

    assert quality_gate.cmd_check_functions() == 1


def test_new_layer_violation_fails(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_whitelist", lambda: {})
    monkeypatch.setattr(quality_gate, "scan_py_functions", lambda _dirs: [("core/foo.py", "new_long_fn", 71, 70)])
    monkeypatch.setattr(quality_gate, "scan_ts_functions", lambda _dirs: [])

    assert quality_gate.cmd_check_functions() == 1


def test_stale_whitelist_entries_do_not_fail(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_whitelist", lambda: {"core/old.py::legacy": 90})
    monkeypatch.setattr(quality_gate, "scan_py_functions", lambda _dirs: [])
    monkeypatch.setattr(quality_gate, "scan_ts_functions", lambda _dirs: [])

    assert quality_gate.cmd_check_functions() == 0


def test_ci_scan_rejects_long_function_in_research_package(tmp_path, monkeypatch, capsys) -> None:
    package = tmp_path / "stock_research"
    package.mkdir()
    (package / "example.py").write_text(
        "def oversized():\n" + "".join(f"    value_{index} = {index}\n" for index in range(72))
    )
    monkeypatch.setattr(quality_gate, "ROOT", tmp_path)

    assert quality_gate.cmd_check_functions() == 1
    output = capsys.readouterr().out
    assert "stock_research/example.py" in output
    assert "oversized()" in output
