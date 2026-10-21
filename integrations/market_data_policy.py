"""人工确认的证券状态及上市边界；不根据空行情自动判定退市。"""

from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

POLICY_PATH = Path(__file__).resolve().parents[1] / "config" / "market_data_overrides.json"


class BeforeListingRange(RuntimeError):
    """行情源元数据证实整个请求区间早于该代码首次交易日。"""

    def __init__(self, listing_date: date) -> None:
        self.listing_date = listing_date
        super().__init__(f"请求区间早于行情源首次交易日 {listing_date}")


def load_security_overrides(path: Path = POLICY_PATH) -> dict[tuple[str, str], dict]:
    """读取有证据的人工覆写；格式错误直接拒绝而非静默扩大排除范围。"""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != 1 or not isinstance(payload.get("securities"), list):
        raise ValueError("证券维护清单格式无效")
    result = {}
    for row in payload["securities"]:
        key = (row["market"], row["symbol"])
        if key in result or key[0] not in {"cn", "us", "hk"} or not key[1]:
            raise ValueError("证券维护清单代码重复或市场无效")
        if not row.get("evidence") or not isinstance(row.get("exclude", False), bool):
            raise ValueError("证券维护清单必须提供证据及有效排除标志")
        for field in ("listing_date", "effective_from"):
            if row.get(field):
                date.fromisoformat(row[field])
        if row.get("exclude") and not row.get("reason"):
            raise ValueError("排除证券必须写明人工确认原因")
        result[key] = dict(row)
    return result


def exclusion_applies(record: dict, target: date) -> bool:
    """仅排除已批准且在本轮目标日期已生效的证券。"""
    return bool(record.get("exclude") and (not record.get("effective_from") or date.fromisoformat(record["effective_from"]) <= target))


def fetch_cn_suspensions(target: date) -> dict[str, str]:
    """读取指定交易日全天停牌证据；不把预期复牌日外推为永久排除。"""
    import akshare as ak
    import pandas as pd

    frame = ak.stock_tfp_em(date=target.strftime("%Y%m%d"))
    required = {"代码", "停牌时间", "停牌截止时间", "预计复牌时间", "停牌期限"}
    if frame is None or not required.issubset(frame.columns):
        raise RuntimeError("停复牌接口缺少核验字段")
    result = {}
    for row in frame.to_dict("records"):
        start = pd.to_datetime(row["停牌时间"], errors="coerce")
        end = pd.to_datetime(row["停牌截止时间"], errors="coerce")
        resume = pd.to_datetime(row["预计复牌时间"], errors="coerce")
        if row["停牌期限"] not in {"连续停牌", "全天", "全天停牌", "1天"}:
            continue
        if pd.isna(start) or start.date() > target:
            continue
        if (not pd.isna(end) and end.date() < target) or (not pd.isna(resume) and resume.date() <= target):
            continue
        code = str(row["代码"]).zfill(6)
        if len(code) == 6 and code.isdigit():
            result[code] = f"https://data.eastmoney.com/tfpxx/ (query_date={target.isoformat()})"
    return result


def fetch_cn_listing_dates(exchange: str) -> dict[str, dict[str, str]]:
    """批量读取交易所明确上市日期，不用行情首行推断上市日。"""
    import akshare as ak
    import pandas as pd

    if exchange == "bj":
        frame = ak.stock_info_bj_name_code()
        code_column, date_column = "证券代码", "上市日期"
        evidence = "https://www.bse.cn/nq/listedcompany.html"
    elif exchange == "sz":
        frame = ak.stock_info_sz_name_code(symbol="A股列表")
        code_column, date_column = "A股代码", "A股上市日期"
        evidence = "https://www.szse.cn/market/product/stock/list/index.html"
    else:
        raise ValueError(f"unsupported listing-date exchange: {exchange}")
    if frame is None or not {code_column, date_column}.issubset(frame.columns):
        raise RuntimeError("交易所上市日期接口缺少字段")
    result = {}
    for row in frame.to_dict("records"):
        code = str(row[code_column]).zfill(6)
        listing = pd.to_datetime(row[date_column], errors="coerce")
        if len(code) == 6 and code.isdigit() and not pd.isna(listing):
            if code in result:
                raise ValueError(f"交易所上市日期代码重复: {code}")
            result[code] = {"listing_date": listing.date().isoformat(), "evidence": evidence}
    if not result:
        raise RuntimeError("交易所上市日期接口无有效记录")
    return result


def yahoo_first_trade_date(symbol: str) -> date | None:
    """读取Yahoo明确给出的firstTradeDate，不把最早缓存行推断为上市日。"""
    import yfinance as yf

    try:
        ticker = yf.Ticker(symbol)
        # 显式设置网络超时；之后元数据从同一次history响应读取。
        ticker.history(period="1mo", timeout=8)
        metadata = ticker.get_history_metadata()
        stamp = metadata.get("firstTradeDate")
        if stamp is None:
            return None
        default_tz = "Asia/Shanghai" if symbol.endswith((".SZ", ".SS", ".BJ")) else "Asia/Hong_Kong" if symbol.endswith(".HK") else "America/New_York"
        tz = ZoneInfo(metadata.get("exchangeTimezoneName") or default_tz)
        # SDK有的版本把epoch格式化成Timestamp，二者都要保留交易所日期。
        if isinstance(stamp, datetime):
            return stamp.astimezone(tz).date() if stamp.tzinfo else stamp.replace(tzinfo=tz).date()
        if isinstance(stamp, date):
            return stamp
        return datetime.fromtimestamp(float(stamp), tz=tz).date()
    except Exception:
        # 元数据不可用不能证明已退市，也不能批准忽略历史缺口。
        return None
