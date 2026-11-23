"""独立供应商补充的时区、完整性、对拍和不可变读取验证。"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from stock_research.benchmarks import BenchmarkBundle, frame_bars, merge_benchmarks
from stock_research.cli import snapshot_command
from stock_research.config import Settings
from stock_research.identity import digest, now
from stock_research.models import Bar, ClassificationVersion, UniverseVersion

AS_OF = date(2026, 11, 20)


@pytest.fixture
def bundle(bars: tuple[Bar, ...]) -> BenchmarkBundle:
    """三基准都有320根，全部明确标记为合成供应商补充。"""
    source = "synthetic-benchmark-supplier"
    selected = tuple(bar.model_copy(update={"source": source}) for bar in bars if bar.kind == "benchmark")
    payload = dict(
        as_of=AS_OF.isoformat(),
        window_start=min(b.session_date for b in selected).isoformat(),
        history_rows=320,
        obtained_time=now().isoformat(),
        source=source,
        responses={name: "{}" for name in ("SPY", "QQQ", "IWM")},
        bars=[b.model_dump(mode="json") for b in selected],
        limitations=("synthetic supplement",),
    )
    return BenchmarkBundle(**payload, content_hash=digest(payload))


def test_normal_frame_retains_new_york_session_and_volume() -> None:
    """【正常值】纽约午夜转换不偏移日期，成交量不作金额或单位换算。"""
    frame = pd.DataFrame(
        {"Open": [10], "High": [12], "Low": [9], "Close": [11], "Volume": [12345]},
        index=pd.DatetimeIndex(["2026-11-20"], tz="America/New_York"),
    )

    result = frame_bars("SPY", frame, AS_OF, AS_OF, "synthetic")

    assert result[0].session_date == AS_OF
    assert result[0].volume == 12345
    assert result[0].kind == "benchmark"


@pytest.mark.parametrize("mode", ["naive", "duplicate", "future", "nan", "bad_ohlc"])
def test_error_invalid_supplier_frame_is_rejected(mode: str) -> None:
    """【异常值】时区、重复、未来数据、NaN和坏OHLC不能生成补充。"""
    frame = pd.DataFrame(
        {"Open": [10.0], "High": [12.0], "Low": [9.0], "Close": [11.0], "Volume": [1000.0]},
        index=pd.DatetimeIndex(["2026-11-20"], tz="America/New_York"),
    )
    if mode == "naive":
        frame.index = frame.index.tz_localize(None)
    elif mode == "duplicate":
        frame = pd.concat([frame, frame])
    elif mode == "future":
        frame.index = pd.DatetimeIndex(["2026-11-23"], tz="America/New_York")
    elif mode == "nan":
        frame.loc[:, "Volume"] = float("nan")
    else:
        frame.loc[:, "High"] = 5

    with pytest.raises(ValueError):
        frame_bars("SPY", frame, AS_OF, AS_OF, "synthetic")


def test_normal_explicit_merge_uses_supplier_for_all_benchmarks(bundle: BenchmarkBundle, bars: tuple[Bar, ...]) -> None:
    """【正常值】只有显式启用的文件参与替换，三个基准保持同一供应商。"""
    merged = merge_benchmarks(bars, bundle, AS_OF, 320, 0.001)

    assert len(merged) == len(bars)
    assert {bar.source for bar in merged if bar.kind == "benchmark"} == {bundle.source}
    assert {bar.source for bar in merged if bar.kind == "stock"} == {"synthetic-fixture"}


@pytest.mark.parametrize(
    "field,value",
    [pytest.param("close", 11.1, id="error-price-adjustment"), pytest.param("volume", 100, id="error-volume-unit")],
)
def test_error_incompatible_spy_blocks_merge(
    field: str, value: float, bundle: BenchmarkBundle, bars: tuple[Bar, ...]
) -> None:
    """【异常值】复权价格或成交量单位冲突不能放宽门槛或静默替换。"""
    changed = tuple(bar.model_copy(update={field: value}) if bar.symbol == "SPY" else bar for bar in bundle.bars)
    invalid = bundle.model_copy(update={"bars": changed})

    with pytest.raises(ValueError, match="冲突"):
        merge_benchmarks(bars, invalid, AS_OF, 320, 0.001)


def test_error_missing_reference_cannot_confirm_compatibility(bundle: BenchmarkBundle, bars: tuple[Bar, ...]) -> None:
    """【异常值】没有云端SPY重叠样本时明确拒绝，不能自证口径相同。"""
    stocks_only = tuple(bar for bar in bars if bar.kind == "stock")

    with pytest.raises(ValueError, match="重叠不足"):
        merge_benchmarks(stocks_only, bundle, AS_OF, 320, 0.001)


def test_validation_bundle_rejects_incomplete_dates_even_with_new_hash(bundle: BenchmarkBundle) -> None:
    """【参数校验】缺一根日线后重算摘要仍不能通过完整性校验。"""
    payload = bundle.model_dump(mode="json")
    payload["bars"].pop()
    payload["content_hash"] = digest({key: value for key, value in payload.items() if key != "content_hash"})

    with pytest.raises(ValidationError, match="覆盖不完整"):
        BenchmarkBundle.model_validate(payload)


def test_validation_bundle_rejects_file_tampering(bundle: BenchmarkBundle) -> None:
    """【参数校验】归一化价格被修改后，重新读取摘要失败。"""
    payload = bundle.model_dump(mode="json")
    payload["bars"][0]["close"] = 10.5

    with pytest.raises(ValidationError, match="摘要"):
        BenchmarkBundle.model_validate(payload)


def test_normal_snapshot_reads_explicit_bundle_and_freezes_provenance(
    tmp_path: Path,
    market_path: Path,
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    bundle: BenchmarkBundle,
) -> None:
    """【正常值】CLI快照路径真实读取独立文件，事实及限制均参与摘要。"""
    upath, cpath, bpath = (tmp_path / name for name in ("universe.json", "classifications.json", "benchmarks.json"))
    upath.write_text(universe.model_dump_json())
    cpath.write_text(classifications.model_dump_json())
    bpath.write_text(bundle.model_dump_json())
    settings = Settings(
        _env_file=None,
        universe_path=upath,
        classification_path=cpath,
        benchmark_path=bpath,
        local_market_path=market_path,
        artifacts_path=tmp_path / "artifacts",
    )

    result, status = snapshot_command(settings, AS_OF, offline=True)
    from stock_research.models import Snapshot

    materialized = Snapshot.model_validate_json(Path(result["path"]).read_text())

    assert status == 0
    assert materialized.source_identity.startswith("composite:")
    assert "synthetic supplement" in materialized.quality.limitations
    assert {bar.source for bar in materialized.bars if bar.kind == "benchmark"} == {bundle.source}


def test_validation_empty_benchmark_configuration_disables_supplement() -> None:
    """【参数校验】空env键不会被解释成当前目录，也不会自动回退取网。"""
    settings = Settings(_env_file=None, benchmark_path="")

    assert settings.benchmark_path is None
