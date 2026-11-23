"""同一快照的ATR/ADR独立进程对拍，测量冷/热耗时及进程峰值内存。"""

import argparse
import json
import resource
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

import numpy as np
import pandas as pd

from stock_research.artifacts import write_once
from stock_research.factors import volatility_factors
from stock_research.identity import digest
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters


def pandas_values(rows: list[dict[str, object]], parameters: MartinParameters) -> tuple[np.ndarray, str]:
    """直接调用生产ATR/ADR参考组件，避免另写公式作为评估代理。"""
    data = pd.DataFrame(rows).sort_values(["symbol", "date"])
    values, keys = [], []
    for symbol, frame in data.groupby("symbol", sort=True):
        frame = frame.reset_index(drop=True)
        volatility_factors(frame, parameters)
        values.extend(frame[["atr", "adr"]].to_numpy().tolist())
        keys.extend(f"{symbol}:{day}" for day in frame["date"])
    return np.asarray(values, dtype=float), digest(keys)


def akquant_values(
    rows: list[dict[str, object]], parameters: MartinParameters, directory: Path
) -> tuple[np.ndarray, str]:
    """AKQuant只比较可对齐的SMA算子；TR用独立Polars行级定义。"""
    import polars as pl
    from akquant.data import ParquetDataCatalog
    from akquant.factor import FactorEngine

    data = pl.DataFrame(rows).sort(["symbol", "date"])
    data = data.with_columns(pl.col("close").shift(1).over("symbol").alias("previous"))
    data = data.with_columns(
        pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("previous")).abs(),
            (pl.col("low") - pl.col("previous")).abs(),
        ).alias("true_range")
    )
    engine = FactorEngine(ParquetDataCatalog(str(directory)))
    atr = engine.run_on_data(data, f"Ts_Mean(true_range,{parameters.atr_window})")
    adr = engine.run_on_data(data, f"Ts_Mean((high/low-1)*100,{parameters.adr_window})")
    keys = [":".join((symbol, day)) for symbol, day in atr.select("symbol", "date").iter_rows()]
    return np.column_stack((atr["factor_value"].to_numpy(), adr["factor_value"].to_numpy())), digest(keys)


def worker(snapshot_path: Path, parameters_path: Path, backend: str, output: Path) -> None:
    """每个后端在独立进程中读同一输入，峰值包含解析和两次计算。"""
    snapshot = Snapshot.model_validate_json(snapshot_path.read_text(encoding="utf-8"))
    parameters = MartinParameters.model_validate_json(parameters_path.read_text(encoding="utf-8"))
    eligible = set(snapshot.quality.eligible["martin"])
    rows = [
        {
            "symbol": bar.symbol,
            "date": bar.session_date.isoformat(),
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
        }
        for bar in snapshot.bars
        if bar.kind == "stock" and bar.symbol in eligible
    ]
    times = []
    values, keys = None, None
    for _ in range(2):
        start = perf_counter()
        values, keys = (
            pandas_values(rows, parameters)
            if backend == "pandas"
            else akquant_values(rows, parameters, output.parent / "catalog")
        )
        times.append(perf_counter() - start)
    np.savez(output, values=values)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    metrics = {
        "backend": backend,
        "cold_seconds": times[0],
        "hot_seconds": times[1],
        "rows": len(rows),
        "keys_hash": keys,
        "peak_rss_mib": rss / (1024 * 1024 if sys.platform == "darwin" else 1024),
    }
    output.with_suffix(".json").write_text(json.dumps(metrics), encoding="utf-8")


def benchmark(
    snapshot_path: Path, parameters: MartinParameters, destination: Path, *, timeout_seconds: int = 180
) -> dict[str, object]:
    """父进程逐值核对位置、预热缺失与数值，子进程超时明确失败。"""
    snapshot = Snapshot.model_validate_json(snapshot_path.read_text(encoding="utf-8"))
    if not snapshot.quality.passed or not snapshot.quality.eligible["martin"]:
        raise ValueError("对拍需要质量通过且非空的Martin有效股票池")
    with TemporaryDirectory(prefix="stock-factor-benchmark-") as directory:
        root = Path(directory)
        parameter_path = root / "parameters.json"
        parameter_path.write_text(parameters.model_dump_json(), encoding="utf-8")
        metrics, arrays = {}, {}
        for backend in ("pandas", "akquant"):
            output = root / f"{backend}.npz"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "stock_research.factor_benchmark",
                    "--worker",
                    backend,
                    "--snapshot",
                    str(snapshot_path.resolve()),
                    "--parameters",
                    str(parameter_path),
                    "--output",
                    str(output),
                ],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
            if result.returncode:
                raise ValueError(f"{backend}因子对拍进程失败，未生成通过结论")
            metrics[backend] = json.loads(output.with_suffix(".json").read_text(encoding="utf-8"))
            with np.load(output, allow_pickle=False) as data:
                arrays[backend] = data["values"].copy()
    reference, candidate = arrays["pandas"], arrays["akquant"]
    positions_match = (
        metrics["pandas"]["keys_hash"] == metrics["akquant"]["keys_hash"] and reference.shape == candidate.shape
    )
    nan_match = positions_match and np.array_equal(np.isnan(reference), np.isnan(candidate))
    errors = (
        np.abs(reference[np.isfinite(reference)] - candidate[np.isfinite(reference)])
        if nan_match
        else np.asarray([np.inf])
    )
    maximum = float(errors.max()) if len(errors) else 0.0
    result = {
        "snapshot_hash": snapshot.content_hash,
        "parameter_hash": digest(parameters.model_dump(mode="json")),
        "compared": ["atr_sma", "adr"],
        "ema_compared": False,
        "tolerance": 1e-10,
        "positions_match": positions_match,
        "warmup_missing_match": nan_match,
        "max_absolute_error": maximum if np.isfinite(maximum) else None,
        "passed": bool(nan_match and maximum <= 1e-10),
        "measurements": metrics,
        "scope": "相同ATR/ADR输出；独立进程峰值含输入解析、冷/热两次计算及运行时，不代表完整策略性能",
    }
    path = destination / f"factor-benchmark-{digest(result)}.json"
    write_once(path, json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return {**result, "path": str(path.resolve())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", choices=["pandas", "akquant"], required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--parameters", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    worker(arguments.snapshot, arguments.parameters, arguments.worker, arguments.output)
