from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _preview_df(df: pd.DataFrame | None, *, rows: int = 3) -> dict[str, Any]:
    if df is None:
        return {"rows": 0, "columns": [], "sample": []}
    sample = df.head(rows).copy()
    for col in sample.columns:
        sample[col] = sample[col].map(_jsonable)
    return {
        "rows": int(len(df)),
        "columns": [str(c) for c in df.columns.tolist()],
        "sample": sample.to_dict("records"),
    }


def _pick_focus_columns(df: pd.DataFrame, keywords: list[str]) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        text = str(col).lower()
        if any(k.lower() in text for k in keywords):
            cols.append(str(col))
    return cols


def _safe_probe(fn):
    try:
        data = fn()
        return {"ok": True, "result": _jsonable(data)}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run_probe(output_path: str) -> dict[str, Any]:
    import akshare as ak

    summary: dict[str, Any] = {
        "akshare_version": getattr(ak, "__version__", "unknown"),
        "tests": {},
    }

    def _a_hist():
        df = ak.stock_zh_a_hist(
            symbol="000001",
            period="daily",
            start_date="20260301",
            end_date="20260415",
            adjust="qfq",
        )
        return {
            "focus_columns": _pick_focus_columns(
                df,
                [
                    "日期",
                    "开盘",
                    "收盘",
                    "成交量",
                    "成交额",
                    "换手",
                    "量比",
                    "振幅",
                    "涨跌幅",
                ],
            ),
            "preview": _preview_df(df),
        }

    def _a_index_hist():
        df = ak.index_zh_a_hist(
            symbol="000001",
            period="daily",
            start_date="20260301",
            end_date="20260415",
        )
        return {
            "focus_columns": _pick_focus_columns(
                df,
                [
                    "日期",
                    "开盘",
                    "收盘",
                    "成交量",
                    "成交额",
                    "换手",
                    "量比",
                    "振幅",
                    "涨跌幅",
                ],
            ),
            "preview": _preview_df(df),
        }

    def _a_spot():
        df = ak.stock_zh_a_spot_em()
        picked = df[df["代码"].astype(str) == "000001"].copy()
        return {
            "focus_columns": _pick_focus_columns(
                df,
                [
                    "代码",
                    "名称",
                    "最新",
                    "成交量",
                    "成交额",
                    "换手",
                    "量比",
                    "市盈率",
                    "总市值",
                ],
            ),
            "preview": _preview_df(picked if not picked.empty else df),
        }

    def _hk_hist():
        df = ak.stock_hk_hist(
            symbol="00700",
            period="daily",
            start_date="20260301",
            end_date="20260415",
            adjust="qfq",
        )
        return {
            "focus_columns": _pick_focus_columns(
                df,
                [
                    "日期",
                    "开盘",
                    "收盘",
                    "成交量",
                    "成交额",
                    "换手",
                    "量比",
                    "振幅",
                    "涨跌幅",
                ],
            ),
            "preview": _preview_df(df),
        }

    def _hk_spot():
        df = ak.stock_hk_spot_em()
        picked = df[df["代码"].astype(str).str.zfill(5) == "00700"].copy()
        return {
            "focus_columns": _pick_focus_columns(
                df,
                [
                    "代码",
                    "名称",
                    "最新",
                    "成交量",
                    "成交额",
                    "换手",
                    "量比",
                    "市盈率",
                    "总市值",
                ],
            ),
            "preview": _preview_df(picked if not picked.empty else df),
        }

    def _us_spot_and_hist():
        spot = ak.stock_us_spot_em()
        code_col = "代码" if "代码" in spot.columns else str(spot.columns[0])
        name_col = "名称" if "名称" in spot.columns else None
        picked = spot[
            spot[code_col].astype(str).str.contains("AAPL", case=False, na=False)
        ].copy()
        if picked.empty and name_col:
            picked = spot[
                spot[name_col]
                .astype(str)
                .str.contains("苹果|Apple", case=False, na=False)
            ].copy()
        if picked.empty:
            raise RuntimeError("AAPL not found in stock_us_spot_em")
        us_symbol = str(picked.iloc[0][code_col])
        hist = ak.stock_us_hist(
            symbol=us_symbol,
            period="daily",
            start_date="20260301",
            end_date="20260415",
            adjust="qfq",
        )
        return {
            "resolved_symbol": us_symbol,
            "spot_focus_columns": _pick_focus_columns(
                spot,
                [
                    "代码",
                    "名称",
                    "最新",
                    "成交量",
                    "成交额",
                    "换手",
                    "量比",
                    "市盈率",
                    "总市值",
                ],
            ),
            "spot_preview": _preview_df(picked),
            "hist_focus_columns": _pick_focus_columns(
                hist,
                [
                    "日期",
                    "开盘",
                    "收盘",
                    "成交量",
                    "成交额",
                    "换手",
                    "量比",
                    "振幅",
                    "涨跌幅",
                ],
            ),
            "hist_preview": _preview_df(hist),
        }

    def _cn_board():
        names = ak.stock_board_industry_name_em()
        board_name = str(names.iloc[0]["板块名称"])
        hist = ak.stock_board_industry_hist_em(
            symbol=board_name,
            start_date="20260301",
            end_date="20260415",
            period="日k",
            adjust="",
        )
        return {
            "board_name": board_name,
            "name_preview": _preview_df(names),
            "hist_focus_columns": _pick_focus_columns(
                hist,
                [
                    "日期",
                    "开盘",
                    "收盘",
                    "成交量",
                    "成交额",
                    "振幅",
                    "涨跌幅",
                    "换手",
                    "量比",
                ],
            ),
            "hist_preview": _preview_df(hist),
        }

    summary["tests"]["a_stock_hist"] = _safe_probe(_a_hist)
    summary["tests"]["a_index_hist"] = _safe_probe(_a_index_hist)
    summary["tests"]["a_spot"] = _safe_probe(_a_spot)
    summary["tests"]["hk_hist"] = _safe_probe(_hk_hist)
    summary["tests"]["hk_spot"] = _safe_probe(_hk_spot)
    summary["tests"]["us_spot_and_hist"] = _safe_probe(_us_spot_and_hist)
    summary["tests"]["cn_board_industry"] = _safe_probe(_cn_board)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="AKShare 本地数据探针")
    parser.add_argument(
        "--output",
        default="data/akshare_probe_results.json",
        help="结果 JSON 输出路径",
    )
    args = parser.parse_args()
    result = run_probe(args.output)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
