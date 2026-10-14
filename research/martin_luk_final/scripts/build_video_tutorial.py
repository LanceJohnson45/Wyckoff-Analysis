"""Embed cut teaching clips and timed subtitles in the existing tutorial."""

from __future__ import annotations

import html
import json
from pathlib import Path

from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "Martin_Luk_详细实操教程_V3.0.html"
ANCHORS = {
    "第三方解说：三类扫描器": "2.4 ",
    "第三方解说：风控与退出（需对照修订）": "11.7 ",
    "第三方解说：GME案例": "13.8 ",
    "第三方解说：COIN关联交易与退出": "13.9 ",
    "第三方解说：名单与执行流程": "2.5 ",
    "市场和自身反馈": "1.3 ",
    "名单、板块强弱与扫描": "2.3 ",
    "AVWAP、回调与首次回测": "3.3 ",
    "回调小周期触发": "4.4 ",
    "USO确认后回踩": "5.4 ",
    "平台、内包、开盘区间": "6.4 ",
    "震荡早盘过滤": "1.3 ",
    "U&R": "第8课",
    "理想反弹空与空间反例": "8.3 ",
    "QS完整失败讲述": "8.5 ",
    "SNDK转向及激进加仓": "8.6 ",
    "急涨后上推失败": "8.7 ",
    "EP与提前入场": "第11课",
    "风险与仓位": "11.4 ",
    "分批、趋势跟踪、时区": "12.5 ",
    "保本和资金曲线": "12.4 ",
    "空头持仓阶段": "12.6 ",
    "过早、错过与FOMO": "第14课",
    "回撤、过度交易与减少参与": "第14课",
}


def clock(seconds: float) -> str:
    """Format a source position for the visible clip caption."""
    value = int(seconds)
    return f"{value // 3600:02d}:{value // 60 % 60:02d}:{value % 60:02d}"


def target_prefix(clip: dict) -> str:
    """Choose the lesson subsection corresponding to each source excerpt."""
    topic = clip["topic"]
    if topic == "极端超跌做多、极端上涨反转":
        return "9.3 " if clip["start"] < 10000 else "10.2 "
    if topic == "QBTS、PLTR、SK hynix":
        return "13.7 " if clip["start"] >= 5595 else "13.6 "
    return ANCHORS[topic]


def build() -> None:
    """Render relative MP4 sources and self-contained subtitle cue data."""
    base = ROOT / "Martin_Luk_详细实操教程_V4.0_图文底稿.html"
    soup = BeautifulSoup(base.read_text(encoding="utf-8"), "html.parser")
    manifest = json.loads((ROOT / "clips_manifest.json").read_text(encoding="utf-8"))
    cue_data = json.loads((ROOT / "subtitles" / "clip_cues.json").read_text(encoding="utf-8"))
    for clip in manifest:
        media = ROOT / "clips" / f"{clip['id']}.mp4"
        if not media.exists() or clip["id"] not in cue_data:
            raise FileNotFoundError(f"Missing completed clip/subtitle: {clip['id']}")
        prefix = target_prefix(clip)
        anchor = next((h for h in soup.find_all(["h2", "h3"]) if h.get_text().startswith(prefix)), None)
        if anchor is None:
            raise ValueError(f"Missing lesson anchor: {prefix}")
        cid = html.escape(clip["id"], quote=True)
        title = html.escape(clip["topic"])
        label = f"{clip['source']} {clock(clip['start'])}～{clock(clip['end'])}"
        source_label = "第三方解说（请对照正文，非本人新增亲述）" if clip["source"] == "E" else "本人访谈/直播"
        markup = f'''<details class="teaching-clip" id="clip-{cid}">
<summary>视频讲解 · {title} · {clip['end'] - clip['start']}秒</summary>
<p class="clip-meta">来源：{source_label} · {label}。1080p原画质，音视频流直接复制；片内从00:00开始。</p>
<video controls playsinline preload="none" aria-label="{title} {label}" data-clip="{cid}">
<source src="clips/{cid}.mp4" type="video/mp4">浏览器不支持视频播放。</video>
<div class="clip-controls"><label>字幕：<select aria-label="{title}字幕" data-for="{cid}">
<option value="zh">中文字幕</option><option value="bi">中英对照</option><option value="en">英文原文</option><option value="off">关闭</option></select></label>
<a href="subtitles/{cid}.zh.srt" download>中文字幕SRT</a>
<a href="subtitles/{cid}.bi.srt" download>双语SRT</a>
<a href="clips/{cid}.mp4" download>视频文件</a></div>
<p class="clip-status" role="status">展开后点击播放。字幕为本地机器翻译，交易术语和数字请结合原文；字幕时间来自机器转写，未逐句人工校时。</p>
</details>'''
        # E的新课文先解释二手证据，再在该小节末尾提供核对片段。
        target = anchor.find_next(["h2", "h3"]) if clip["source"] == "E" else anchor
        if target is None:
            raise ValueError(f"Missing following heading: {prefix}")
        target.insert_before(BeautifulSoup(markup, "html.parser"))
    note = soup.new_tag("p", attrs={"class": "video-edition-note"})
    duration = sum(clip["end"] - clip["start"] for clip in manifest)
    note.string = f"实操教程 V4.1：新增三类扫描、名单迁移、仓位漂移及GME/COIN收益核算；A～D为本人访谈/直播，E为第三方解说，分歧见正文。附{len(manifest)}段独立教学视频，共{int(duration // 60)}分{int(duration % 60)}秒。展开‘视频讲解’播放，可切换中文、双语、英文或关闭字幕。视频位于同目录clips文件夹，仅保留本地，未随Git上传；移动教程请保留clips和subtitles相对目录。图片和网页字幕已内嵌。"
    soup.find("h1").insert_after(note)
    style = soup.new_tag("style")
    style.string = """
.video-edition-note{padding:18px;background:#eaf4f7;border-left:4px solid #26708a}
.teaching-clip{margin:24px 0;border:1px solid #bfd3df;border-radius:8px;padding:14px 18px;background:#f6fafc}
.teaching-clip summary{cursor:pointer;font-weight:600;color:#153a5b}
.teaching-clip video{width:100%;aspect-ratio:16/9;background:#101820;display:block;margin:12px 0}
.teaching-clip video::cue{font-size:18px;color:white;background:rgba(0,0,0,.8)}
.clip-controls{display:flex;gap:14px;flex-wrap:wrap;align-items:center;font-size:14px}
.clip-controls select{padding:7px;border:1px solid #b5cad6;border-radius:4px;font-size:15px}
.clip-meta,.clip-status{font-size:13px;line-height:1.6;color:#526174}
@media print{.teaching-clip{display:none}}
"""
    soup.head.append(style)
    data_tag = soup.new_tag("script", attrs={"id": "subtitle-cues", "type": "application/json"})
    data_tag.string = json.dumps(cue_data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    soup.body.append(data_tag)
    script = soup.new_tag("script")
    script.string = """
(() => {
  const cues = JSON.parse(document.getElementById('subtitle-cues').textContent);
  const videos = [...document.querySelectorAll('video[data-clip]')];
  function initialize(video) {
    if (video.dataset.subtitlesReady) return;
    const rows = cues[video.dataset.clip];
    if (!Array.isArray(rows) || !rows.length) throw new Error('缺少片段字幕');
    for (const [key, label, lang] of [['zh','简体中文（机器）','zh-CN'],['bi','中英对照（机器）','zh-CN'],['en','英文原文（机器）','en']]) {
      const track = video.addTextTrack('subtitles', label, lang);
      track.mode = 'hidden';
      for (const row of rows) {
        const text = key === 'bi' ? row.zh + '\\n' + row.en : row[key];
        const cue = new VTTCue(row.start, row.end, text.replace(/</g, '&lt;').replace(/>/g, '&gt;'));
        cue.line = -3; track.addCue(cue);
      }
    }
    video.dataset.subtitlesReady = 'true';
  }
  function setMode(video) {
    initialize(video);
    const select = document.querySelector('select[data-for="' + video.dataset.clip + '"]');
    const index = {zh:0, bi:1, en:2, off:-1}[select.value];
    [...video.textTracks].forEach((track, i) => {track.mode = i === index ? 'showing' : 'disabled';});
  }
  for (const video of videos) {
    video.parentElement.addEventListener('toggle', () => {
      if (!video.parentElement.open) video.pause();
    });
    video.addEventListener('play', () => {
      videos.filter(other => other !== video).forEach(other => other.pause());
      setMode(video);
    });
    video.addEventListener('error', () => {
      video.parentElement.querySelector('.clip-status').textContent = '视频加载失败：请确认clips文件夹与HTML放在一起，文件名未改。';
    });
    document.querySelector('select[data-for="' + video.dataset.clip + '"]').addEventListener('change', () => setMode(video));
  }
})();
"""
    soup.body.append(script)
    PAGE.write_text(str(soup), encoding="utf-8")
    assert len(soup.find_all("video")) == len(manifest)


if __name__ == "__main__":
    build()
