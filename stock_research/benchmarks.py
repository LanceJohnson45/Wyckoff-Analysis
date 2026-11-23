"""供应商基准的不可变补充文件，不写入或冒充云端行情。"""

import importlib.metadata
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pydantic import AwareDatetime, model_validator

from stock_research.artifacts import write_once
from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.identity import digest, now
from stock_research.models import Bar, FrozenModel
from stock_research.quality import BENCHMARKS


class BenchmarkBundle(FrozenModel):
    """取得时的完整供应商响应及归一化结果，可独立核验与重读。"""

    as_of: date
    window_start: date
    history_rows: int
    obtained_time: AwareDatetime
    source: str
    responses: dict[str, str]
    bars: tuple[Bar, ...]
    limitations: tuple[str, ...]
    content_hash: str

    def content(self) -> dict[str, object]:
        """摘要涵盖响应、参数及全部价格，不含自身摘要。"""
        return self.model_dump(mode="json", exclude={"content_hash"})

    @model_validator(mode="after")
    def validate_bundle(self) -> "BenchmarkBundle":
        """文件损坏、错日期、错kind、重复和不完整基准均拒收。"""
        if digest(self.content()) != self.content_hash:
            raise ValueError("基准补充内容摘要不匹配")
        if self.history_rows < 320:
            raise ValueError("基准补充至少需要320个交易日")
        sessions = sessions_ending(self.as_of, self.history_rows)
        expected = {(symbol, day) for symbol in BENCHMARKS for day in sessions}
        actual = {(bar.symbol, bar.session_date) for bar in self.bars if bar.kind == "benchmark"}
        if self.window_start != sessions[0] or actual != expected or len(self.bars) != len(expected):
            raise ValueError("基准补充交易日、种类或覆盖不完整")
        if set(self.responses) != set(BENCHMARKS) or any(bar.source != self.source for bar in self.bars):
            raise ValueError("基准补充响应或来源不一致")
        return self


def frame_bars(symbol: str, frame: pd.DataFrame, start: date, as_of: date, source: str) -> tuple[Bar, ...]:
    """保留原成交量；仅接受带时区的日线及完整有限OHLCV。"""
    if frame.empty or not isinstance(frame.index, pd.DatetimeIndex) or frame.index.tz is None:
        raise ValueError(f"{symbol}供应商没有带时区的完整日线")
    if not {"Open", "High", "Low", "Close", "Volume"} <= set(frame.columns) or frame.index.has_duplicates:
        raise ValueError(f"{symbol}供应商日线字段缺失或日期重复")
    stamp = now()
    bars = []
    for index, row in frame.iterrows():
        day = index.tz_convert("America/New_York").date()
        if day < start or day > as_of:
            raise ValueError(f"{symbol}供应商返回请求范围之外的日线")
        bars.append(
            Bar(
                symbol=symbol,
                kind="benchmark",
                session_date=day,
                **{key: float(row[key.capitalize()]) for key in ("open", "high", "low", "close", "volume")},
                source=source,
                source_revision=stamp,
                observed_time=stamp,
            )
        )
    return tuple(bars)


def fetch_benchmarks(as_of: date, history_rows: int, destination: Path) -> dict[str, object]:
    """沿用yfinance auto_adjust日线定义，全量验证成功后才发布补充文件。"""
    import yfinance as yf

    if as_of > latest_complete_session():
        raise ValueError("基准截止日尚未完成收盘")
    sessions = sessions_ending(as_of, history_rows)
    source = f"yfinance:{importlib.metadata.version('yfinance')}:daily:auto_adjust=True:repair=False"
    bars, responses = [], {}
    for symbol in BENCHMARKS:
        ticker = yf.Ticker(symbol)
        frame = ticker.history(
            start=sessions[0].isoformat(),
            end=(as_of + timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=True,
            actions=True,
            repair=False,
            timeout=20,
        )
        metadata = ticker.history_metadata
        if metadata.get("currency") != "USD" or metadata.get("instrumentType") != "ETF":
            raise ValueError(f"{symbol}供应商币种或证券类别未确认，拒绝补充")
        bars.extend(frame_bars(symbol, frame, sessions[0], as_of, source))
        responses[symbol] = json.dumps(
            {
                "frame": json.loads(frame.to_json(orient="table", date_format="iso", double_precision=15)),
                "metadata": {key: metadata.get(key) for key in ("currency", "instrumentType", "exchangeTimezoneName")},
            },
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
    content = dict(
        as_of=as_of.isoformat(),
        window_start=sessions[0].isoformat(),
        history_rows=history_rows,
        obtained_time=now().isoformat(),
        source=source,
        responses=responses,
        bars=[bar.model_dump(mode="json") for bar in bars],
        limitations=(
            "基准来自明确启用的yfinance补充，未写入云端同步库",
            "使用取得时最新复权历史，非严格时点复权版本；source_revision是适配器取得版本时间，非供应商发布时间",
        ),
    )
    bundle = BenchmarkBundle(**content, content_hash=digest(content))
    path = destination / f"{bundle.content_hash}.json"
    write_once(path, bundle.model_dump_json(indent=2) + "\n")
    return {
        "path": str(path.resolve()),
        "content_hash": bundle.content_hash,
        "rows": len(bundle.bars),
        "as_of": as_of.isoformat(),
        "source": source,
        "limitations": bundle.limitations,
    }


def merge_benchmarks(
    bars: tuple[Bar, ...], bundle: BenchmarkBundle, as_of: date, history_rows: int, price_tolerance: float
) -> tuple[Bar, ...]:
    """显式选用同一供应商的三基准，先与云端SPY对拍，冲突则阻断。"""
    if bundle.as_of != as_of or bundle.history_rows != history_rows:
        raise ValueError("基准补充与快照截止日或窗口不一致")
    if not 0 < price_tolerance <= 0.01:
        raise ValueError("基准价格对拍容忍度无效")
    reference = {bar.session_date: bar for bar in bars if bar.kind == "benchmark" and bar.symbol == "SPY"}
    compared = 0
    for bar in bundle.bars:
        if bar.symbol != "SPY" or bar.session_date not in reference:
            continue
        other = reference[bar.session_date]
        if any(
            abs(getattr(bar, name) / getattr(other, name) - 1) > price_tolerance
            for name in ("open", "high", "low", "close")
        ):
            raise ValueError("供应商基准与云端SPY复权价格冲突，拒绝替换")
        if (other.volume == 0 and bar.volume != 0) or (other.volume and abs(bar.volume / other.volume - 1) > 0.01):
            raise ValueError("供应商基准与云端SPY成交量单位或修订冲突")
        compared += 1
    if compared < 100:
        raise ValueError("与云端SPY重叠不足100根，无法确认补充口径")
    return tuple(bar for bar in bars if bar.kind == "stock") + bundle.bars
