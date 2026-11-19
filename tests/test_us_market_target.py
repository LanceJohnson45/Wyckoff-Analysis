"""目标日校验须拒绝 Yahoo 的无收盘价占位行。"""

from datetime import date

import pandas as pd
import pytest

from scripts import us_market_target as target


def test_empty_ohlc_is_rejected_even_when_volume_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    """本次真实失败样本只有成交量，不能据此启动全池。"""
    monkeypatch.setattr(target, "_fetch_stock_yfinance", lambda *args: pd.DataFrame({
        "开盘": [None], "最高": [None], "最低": [None], "收盘": [None], "成交量": [9297240],
    }))
    with pytest.raises(RuntimeError, match="上游日线未就绪"):
        target.verify_target(date(2026, 11, 18))
