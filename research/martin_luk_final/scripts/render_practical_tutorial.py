"""从同一Markdown生成含嵌入图片的网页底稿及Word教材。"""

from __future__ import annotations

import base64
import json
import subprocess
import re
from pathlib import Path

import markdown
from bs4 import BeautifulSoup
from docx import Document
from docx.oxml.ns import qn
from docx.shared import Inches, Pt

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Martin_Luk_回调买入法与其他交易方法_V2.0_最终整合版.md"
BASE = ROOT / "Martin_Luk_详细实操教程_V4.0_图文底稿.html"
WORD = ROOT / "Martin_Luk_详细实操教程_V3.0.docx"
HISTORY = ROOT / "Martin_Luk_详细实操教程_V3.0_纯图文历史备份.html"


def image_path(value: str) -> Path:
    """解析正文图片路径并确保图片实体存在。"""
    path = Path(value)
    result = path if path.is_absolute() else ROOT / path
    if not result.is_file():
        raise FileNotFoundError(result)
    return result


def render() -> None:
    """保留原章节锚点，导出同内容的网页及Word。"""
    source = SOURCE.read_text(encoding="utf-8")
    body = BeautifulSoup(markdown.markdown(source, extensions=["tables", "fenced_code", "toc"]), "html.parser")
    old = BeautifulSoup(HISTORY.read_text(encoding="utf-8"), "html.parser")
    old_ids = {h.get_text(): h.get("id") for h in old.find_all(["h1", "h2", "h3"])}
    for heading in body.find_all(["h1", "h2", "h3"]):
        if old_ids.get(heading.get_text()):
            heading["id"] = old_ids[heading.get_text()]
        else:
            heading["id"] = "v4-" + heading["id"]
    doc = Document()
    normal = doc.styles["Normal"]
    normal.font.name = "Arial"
    normal.font.size = Pt(10.5)
    normal.element.rPr.rFonts.set(qn("w:eastAsia"), "宋体")
    doc.sections[0].header.paragraphs[0].text = "Martin Luk · V4.1 操作细节与解说核对"
    for node in list(body.children):
        if not getattr(node, "name", None):
            continue
        if node.name in {"h1", "h2", "h3", "h4"}:
            doc.add_heading(node.get_text(), level=int(node.name[1]) - 1)
        elif node.name == "table":
            rows = node.find_all("tr")
            count = max(len(row.find_all(["th", "td"], recursive=False)) for row in rows)
            table = doc.add_table(rows=0, cols=count)
            table.style = "Table Grid"
            for row in rows:
                cells = table.add_row().cells
                for i, cell in enumerate(row.find_all(["th", "td"], recursive=False)):
                    cells[i].text = cell.get_text(" ", strip=True)
            doc.add_paragraph()
        elif node.name in {"ol", "ul"}:
            for item in node.find_all("li", recursive=False):
                doc.add_paragraph(item.get_text(" ", strip=True), style="List Number" if node.name == "ol" else "List Bullet")
        else:
            text = node.get_text(" ", strip=True)
            if text:
                doc.add_paragraph(text)
        for img in node.find_all("img"):
            doc.add_picture(str(image_path(img["src"])), width=Inches(6))
    doc.save(WORD)
    # Word保留本地图片；网页使用内嵌数据，搬动文件也能显示。
    for img in body.find_all("img"):
        path = image_path(img["src"])
        mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
        img["src"] = f"data:{mime};base64," + base64.b64encode(path.read_bytes()).decode()
        size = json.loads(subprocess.check_output(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "json", str(path)]))["streams"][0]
        img["width"] = str(size["width"])
        img["height"] = str(size["height"])
        img["loading"] = "lazy"
    soup = BeautifulSoup("<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Martin Luk 实操教程 V4.1</title></head><body><main></main></body></html>", "html.parser")
    style = soup.new_tag("style")
    style.string = old.style.string + "\nfigure{margin:20px 0}td{overflow-wrap:anywhere}.table-scroll{overflow-x:auto}.contents details{margin:8px 0}.contents summary{cursor:pointer}.contents a{display:block}h4{color:#265878}"
    soup.head.append(style)
    main = soup.main
    notice = soup.new_tag("p", attrs={"class": "notice"})
    notice.string = "V4.1 · 操作细节、解说核对与逐根演练 · 图片内嵌 · 视频1080p原始流"
    main.append(notice)
    nav = soup.new_tag("nav", attrs={"class": "contents", "aria-label": "课程目录"})
    title = soup.new_tag("strong");title.string = "课程目录（展开查看操作小节）";nav.append(title)
    current = nav
    for heading in body.find_all(["h2", "h3"]):
        if heading.name == "h2":
            current = soup.new_tag("details")
            summary = soup.new_tag("summary");summary.string = heading.get_text();current.append(summary)
            nav.append(current)
        link = soup.new_tag("a", href="#" + heading["id"])
        link.string = heading.get_text();current.append(link)
    main.append(nav)
    for node in list(body.contents):
        main.append(node.extract())
    for table in soup.select("main > table"):
        wrapper = soup.new_tag("div", attrs={"class": "table-scroll"})
        table.wrap(wrapper)
    BASE.write_text(str(soup), encoding="utf-8")
    assert len(doc.inline_shapes) == len(soup.find_all("img"))
    assert len(re.findall(r"^## 第\d+课", source, re.M)) == 15


if __name__ == "__main__":
    render()
