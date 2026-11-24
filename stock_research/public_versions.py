"""按Git提交固定公开聚合成分，保留数据更新时间及回溯限制。"""

import base64
import csv
import hashlib
import json
import re
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import httpx
from pydantic import AwareDatetime

from stock_research.artifacts import write_once
from stock_research.config import PUBLIC_GITHUB_API, Settings
from stock_research.identity import now
from stock_research.models import Classification, ClassificationVersion, FrozenModel, UniverseMember, UniverseVersion
from stock_research.versions import validate_universe


class ConstituentSource(FrozenModel):
    """完整响应、不可变Git版本与本次取得时间。"""

    index: Literal["sp500", "nasdaq100"]
    repository: str
    path: str
    commit: str
    revision_time: AwareDatetime
    obtained_time: AwareDatetime
    content_sha256: str
    body: str

    def reference(self) -> str:
        """来源定位到已取得的提交和全文摘要。"""
        return f"github:{self.repository}@{self.commit}:{self.path}#sha256:{self.content_sha256}"


def fetch_constituents(
    index: Literal["sp500", "nasdaq100"], repository: str, path: str, max_age_days: int
) -> ConstituentSource:
    """只从固定GitHub API取得公开文件，不执行仓库内容或回退到旧缓存。"""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or not path or ".." in path.split("/"):
        raise ValueError("公开成分仓库或文件路径无效")
    with httpx.Client(
        base_url=PUBLIC_GITHUB_API, timeout=30, headers={"Accept": "application/vnd.github+json"}
    ) as client:
        commits = client.get(f"/repos/{repository}/commits", params={"path": path, "per_page": 1})
        commits.raise_for_status()
        history = commits.json()
        if not isinstance(history, list) or not history:
            raise ValueError("公开成分没有可核验的文件提交记录")
        try:
            commit = history[0]["sha"]
            revision = datetime.fromisoformat(history[0]["commit"]["committer"]["date"].replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ValueError("公开成分提交元数据无效") from exc
        if not isinstance(commit, str) or not re.fullmatch(r"[a-f0-9]{40}", commit) or revision.tzinfo is None:
            raise ValueError("公开成分提交标识无效")
        obtained = now()
        if not 0 <= (obtained - revision).total_seconds() <= max_age_days * 86400:
            raise ValueError(f"{index}成分文件更新时间超过允许范围，拒绝安装")
        response = client.get(f"/repos/{repository}/contents/{quote(path, safe='/')}", params={"ref": commit})
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("encoding") != "base64"
            or not isinstance(payload.get("size"), int)
            or not 0 < payload["size"] <= 2_000_000
            or not isinstance(payload.get("content"), str)
        ):
            raise ValueError("公开成分响应格式或大小无效")
        body = base64.b64decode(payload["content"], validate=False).decode("utf-8-sig")
    return ConstituentSource(
        index=index,
        repository=repository,
        path=path,
        commit=commit,
        revision_time=revision,
        obtained_time=obtained,
        content_sha256=hashlib.sha256(body.encode()).hexdigest(),
        body=body,
    )


def parse_constituents(source: ConstituentSource) -> list[dict[str, str]]:
    """聚合源字段变化即拒收；不采用Nasdaq聚合文件中已知含混的分类列。"""
    if hashlib.sha256(source.body.encode()).hexdigest() != source.content_sha256:
        raise ValueError("公开成分原始响应摘要不符")
    if source.index == "sp500":
        rows = list(csv.DictReader(StringIO(source.body.strip())))
        names = ("Symbol", "Security", "GICS Sub-Industry")
    else:
        rows = json.loads(source.body)
        names = ("Ticker", "Company")
    if not isinstance(rows, list) or not rows:
        raise ValueError("公开成分为空或不是数组")
    if any(not isinstance(row, dict) or any(not str(row.get(name) or "").strip() for name in names) for row in rows):
        raise ValueError("公开成分缺少必需字段，不接受部分清单")
    return rows


def build_public_versions(
    sources: tuple[ConstituentSource, ...], version: str, as_of: date
) -> tuple[UniverseVersion, ClassificationVersion]:
    """研究适用日不冒充指数真实调整日；两个指数必须来自各自独立响应。"""
    if {source.index for source in sources} != {"sp500", "nasdaq100"} or len(sources) != 2:
        raise ValueError("必须同时提供两指数各一份成分来源")
    members, classifications = [], []
    for source in sources:
        for row in parse_constituents(source):
            symbol_key, name_key = ("Symbol", "Security") if source.index == "sp500" else ("Ticker", "Company")
            members.append(
                UniverseMember(
                    symbol=row[symbol_key],
                    name=row[name_key],
                    index=source.index,
                    source=source.reference(),
                    effective_date=as_of,
                    obtained_time=source.obtained_time,
                    symbol_aliases=(row[symbol_key],),
                )
            )
            if source.index == "sp500":
                classifications.append(
                    Classification(
                        symbol=row[symbol_key],
                        company_name=row[name_key],
                        industry="GICS:" + row["GICS Sub-Industry"].strip(),
                        source=source.reference(),
                        effective_date=as_of,
                        obtained_time=source.obtained_time,
                    )
                )
    limitations = (
        "公开聚合成分，不是指数提供方认证；以各文件提交时间为资料版本，不证明之后没有成分变更",
        "当前取得清单用于指定研究截止日，effective_date仅为研究适用日，非指数真实调整日；含存活偏差",
        "行业使用标普源GICS细分行业；仅纳指成员缺分类时排除，不混入另一个分类体系；主题仅保留已有版本，不自动分配",
    )
    universe = UniverseVersion(version=version, members=tuple(members), limitations=limitations)
    validate_universe(universe, as_of)
    return universe, ClassificationVersion(version=version, records=tuple(classifications), limitations=limitations)


def refresh_versions(settings: Settings, version: str, as_of: date, destination: Path) -> dict[str, object]:
    """先取得、校验全部版本，再发布完成清单；失败不更改生效配置。"""
    sources = tuple(
        fetch_constituents(index, repository, path, settings.public_source_max_age_days)
        for index, repository, path in (
            ("sp500", settings.public_sp500_repository, settings.public_sp500_file),
            ("nasdaq100", settings.public_nasdaq100_repository, settings.public_nasdaq100_file),
        )
    )
    universe, classifications = build_public_versions(sources, version, as_of)
    universe, classifications = preserve_manual_versions(settings, universe, classifications, as_of)
    for source in sources:
        write_once(destination / f"{source.index}-source.json", source.model_dump_json(indent=2) + "\n")
    for name, value in (("universe", universe), ("classifications", classifications)):
        write_once(destination / f"{name}.json", value.model_dump_json(indent=2) + "\n")
    result = {
        "version": version,
        "research_as_of": as_of.isoformat(),
        "universe_count": len(universe.active(as_of)),
        "classification_count": len(classifications.records),
        "directory": str(destination.resolve()),
        "sources": [
            {
                "index": s.index,
                "commit": s.commit,
                "revision_time": s.revision_time.isoformat(),
                "content_sha256": s.content_sha256,
            }
            for s in sources
        ],
        "limitations": universe.limitations,
    }
    write_once(destination / "complete.json", json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return result


def preserve_manual_versions(
    settings: Settings, universe: UniverseVersion, classifications: ClassificationVersion, as_of: date
) -> tuple[UniverseVersion, ClassificationVersion]:
    """保留已生效自选、主题与缺项的GICS补充，来源和资料限制随版本沿用。"""
    manual = ()
    if settings.universe_path.is_file():
        previous = UniverseVersion.model_validate_json(settings.universe_path.read_text(encoding="utf-8"))
        manual = tuple(
            member for members in previous.active(as_of).values() for member in members if member.index == "manual"
        )
        universe = UniverseVersion.model_validate({**universe.model_dump(), "members": (*universe.members, *manual)})
    if settings.classification_path.is_file():
        previous = ClassificationVersion.model_validate_json(settings.classification_path.read_text(encoding="utf-8"))
        themes = {
            record.symbol: record for record in previous.records if record.effective_date <= as_of and record.themes
        }
        records = tuple(
            record.model_copy(
                update={
                    "themes": themes[record.symbol].themes,
                    "source": record.source + "; retained_themes:" + themes[record.symbol].source,
                }
            )
            if record.symbol in themes
            else record
            for record in classifications.records
        )
        known = {record.symbol for record in records}
        manual_symbols = {member.symbol for member in manual}
        active_symbols = set(universe.active(as_of))
        records += tuple(
            record
            for record in previous.records
            if record.symbol not in known
            and record.effective_date <= as_of
            and (
                record.symbol in manual_symbols
                or (record.symbol in active_symbols and (record.industry or "").startswith("GICS:"))
            )
        )
        classifications = ClassificationVersion.model_validate(
            {
                **classifications.model_dump(),
                "records": records,
                "limitations": tuple(sorted(set(classifications.limitations + previous.limitations))),
            }
        )
    return universe, classifications
