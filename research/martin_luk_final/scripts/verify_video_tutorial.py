"""Check real encoded media, rebased subtitle bounds and tutorial references."""

from __future__ import annotations

import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]


def check_media(clip: dict) -> dict:
    """Probe duration/start time and decode frames at both ends of a real clip."""
    path = ROOT / "clips" / f"{clip['id']}.mp4"
    probe = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration,start_time:stream=codec_name,codec_type,start_time,width,height", "-of", "json", str(path)], check=True, text=True, capture_output=True)
    info = json.loads(probe.stdout)
    duration = float(info["format"]["duration"])
    assert abs(duration - (clip["end"] - clip["start"])) < .15, clip["id"]
    assert abs(float(info["format"]["start_time"])) < .05, clip["id"]
    video = next(s for s in info["streams"] if s["codec_type"] == "video")
    audio = next(s for s in info["streams"] if s["codec_type"] == "audio")
    assert video["codec_name"] == "h264" and audio["codec_name"] == "aac"
    assert video["width"] == 1920 and video["height"] == 1080
    for seek in [0, max(0, duration - .5)]:
        subprocess.run(["ffmpeg", "-hide_banner", "-v", "error", "-ss", str(seek), "-i", str(path), "-frames:v", "1", "-an", "-f", "null", "-"], check=True, capture_output=True)
    return {"id": clip["id"], "duration": duration, "bytes": path.stat().st_size, "start_time": info["format"]["start_time"], "width": video["width"], "height": video["height"], "decode": "first/last passed"}


def main() -> None:
    """Validate finished output without accepting missing files or blank captions."""
    manifest = json.loads((ROOT / "clips_manifest.json").read_text(encoding="utf-8"))
    cues = json.loads((ROOT / "subtitles" / "clip_cues.json").read_text(encoding="utf-8"))
    source = json.loads((ROOT / "subtitles" / "source_segments.json").read_text(encoding="utf-8"))
    translations = json.loads((ROOT / "subtitles" / "translations_zh.json").read_text(encoding="utf-8"))
    assert all(s["key"] in translations for s in source)
    with ThreadPoolExecutor(max_workers=4) as executor:
        media = list(executor.map(check_media, manifest))
    for clip in manifest:
        end_limit = clip["end"] - clip["start"]
        assert clip["id"] in cues and cues[clip["id"]]
        last_end = 0
        for cue in cues[clip["id"]]:
            assert last_end <= cue["start"] + .002
            assert 0 <= cue["start"] < cue["end"] <= end_limit + .001
            assert cue["zh"].strip() and cue["en"].strip()
            last_end = cue["end"]
        for lang in ["zh", "bi", "en"]:
            for ext in ["vtt", "srt"]:
                tx = (ROOT / "subtitles" / f"{clip['id']}.{lang}.{ext}").read_text(encoding="utf-8")
                assert tx.count(" --> ") == len(cues[clip["id"]])
    html = BeautifulSoup((ROOT / "Martin_Luk_详细实操教程_V3.0.html").read_text(encoding="utf-8"), "html.parser")
    assert len(html.select("video[data-clip]")) == len(manifest)
    assert len(html.select("details.teaching-clip")) == len(manifest)
    assert len(html.select("img")) == 6
    assert len([h for h in html.find_all("h2") if re.match(r"第\d+课", h.get_text())]) == 15
    for node in html.select("video source, .clip-controls a"):
        value = node.get("src") or node.get("href")
        assert not value.startswith(("/", "http:", "https:"))
        assert (ROOT / value).exists(), value
    embedded = json.loads(html.select_one("#subtitle-cues").string)
    assert embedded == cues
    report = {"clips": len(media), "duration_seconds": sum(x["duration"] for x in media), "bytes": sum(x["bytes"] for x in media), "source_segments": len(source), "translated_segments": len(translations), "caption_cues": sum(map(len, cues.values())), "media": media}
    (ROOT / "视频课程核验记录.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
