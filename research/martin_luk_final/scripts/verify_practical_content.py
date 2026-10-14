"""核验教材导出完整性、导航、图片及教学算例。"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import zipfile
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]


def normalize(value: str) -> str:
    """忽略导出格式产生的空白，保留正文字符。"""
    return re.sub(r"\s+", "", value)


def verify() -> None:
    """检查新版内容实际进入网页和Word，以及算例的预算约束。"""
    source = ROOT / "Martin_Luk_回调买入法与其他交易方法_V2.0_最终整合版.md"
    base = BeautifulSoup((ROOT / "Martin_Luk_详细实操教程_V4.0_图文底稿.html").read_text(), "html.parser")
    final = BeautifulSoup((ROOT / "Martin_Luk_详细实操教程_V3.0.html").read_text(), "html.parser")
    ids = [node["id"] for node in final.select("[id]")]
    assert len(ids) == len(set(ids)), "重复章节ID"
    for link in final.select('a[href^="#"]'):
        assert link["href"][1:] in ids, link["href"]
    assert "V4.0" in final.title.get_text()
    with zipfile.ZipFile(ROOT / "Martin_Luk_详细实操教程_V3.0.docx") as archive:
        assert archive.testzip() is None
        xml = ElementTree.fromstring(archive.read("word/document.xml"))
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        word_text = normalize("".join(node.text or "" for node in xml.findall(".//w:t", ns)))
        word_images = len(xml.findall(".//{http://schemas.openxmlformats.org/drawingml/2006/picture}pic"))
    blocks = base.select("main > h1, main > h2, main > h3, main > p, main > blockquote, main > ol, main > ul, main > pre, main > .table-scroll")
    checked = 0
    for node in blocks:
        if "notice" in node.get("class", []):
            continue
        value = normalize(node.get_text())
        if value:
            assert value in word_text, f"Word遗漏正文: {value[:60]}"
            assert value in normalize(final.main.get_text()), f"视频网页遗漏正文: {value[:60]}"
            checked += 1
    pictures = final.select("main img")
    assert len(pictures) == word_images == 21
    for picture in pictures:
        assert int(picture["width"]) > 0 and int(picture["height"]) > 0
        data = base64.b64decode(picture["src"].split(",", 1)[1], validate=True)
        assert data.startswith(b"\xff\xd8\xff") or data.startswith(b"\x89PNG")
    # 与正文独立计算预算和向下取整，避免有预算约束却按错误数量演练。
    examples = [
        ("20.12", "19.50", "50", 80, "49.60"),
        ("20.12", "19.90", "50", 227, "49.94"),
        ("20.25", "19.98", "50", 185, "49.95"),
        ("20.50", "19.98", "50", 96, "49.92"),
        ("30.22", "29.80", "50", 119, "49.98"),
        ("30.52", "29.80", "50", 69, "49.68"),
        ("20.12", "19.78", "50", 147, "49.98"),
        ("15.80", "15.98", "40", 222, "39.96"),
        ("15.80", "16.24", "40", 90, "39.60"),
        ("20.12", "19.90", "40", 181, "39.82"),
        ("20.18", "19.90", "40", 142, "39.76"),
        ("20.03", "19.81", "50", 227, "49.94"),
    ]
    for entry, stop, budget, quantity, loss in examples:
        risk = abs(Decimal(entry) - Decimal(stop))
        assert int(Decimal(budget) / risk) == quantity
        assert risk * quantity == Decimal(loss)
        assert risk * quantity <= Decimal(budget) < risk * (quantity + 1)
    assert Decimal("181") * Decimal("0.28") == Decimal("50.68")
    assert Decimal("20.03") * 227 == Decimal("4546.81")
    profit = 100 * (Decimal("20.69") - Decimal("20.03")) + 127 * (Decimal("20.45") - Decimal("20.03"))
    assert profit == Decimal("119.34")
    assert round(profit / Decimal("49.94"), 2) == Decimal("2.39")
    report = {"version": "V4.0", "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "characters": len(source.read_text()), "lessons": 15, "subsections": len(base.select("main h3")), "embedded_images_html_and_word": word_images, "body_blocks_checked": checked, "arithmetic_cases": len(examples), "unique_anchors_and_links": "passed", "docx_zip": "passed", "limits": ["未逐秒人工听校四视频", "未逐页在Word应用检查分页", "字幕沿用原机器译文及已有修订", "未做策略回测或独立核实成交"]}
    (ROOT / "V4.0_教材核验记录.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    verify()
