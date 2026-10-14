"""用现有本地MLX Whisper生成第三方解说视频的可追溯转写。"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import mlx_whisper
import structlog

ROOT = Path(__file__).resolve().parents[1]
AUDIO = Path("/tmp/martin-luk-commentary")
LOGGER = structlog.get_logger()
MODEL = "mlx-community/whisper-small-mlx"


def main() -> None:
    """分两段转写并保留原始机器输出和全片时间。"""
    video = Path(sys.argv[1])
    if not video.is_file():
        raise FileNotFoundError(video)
    AUDIO.mkdir(parents=True, exist_ok=True)
    (ROOT / "evidence").mkdir(parents=True, exist_ok=True)
    info = json.loads(subprocess.check_output(["ffprobe", "-v", "error", "-show_entries", "format=duration,size:stream=codec_name,codec_type,width,height", "-of", "json", str(video)]))
    info["source_file"] = str(video)
    (ROOT / "媒体信息.json").write_text(json.dumps(info, ensure_ascii=False, indent=2))
    duration = float(info["format"]["duration"])
    all_segments = []
    for index, start in enumerate([0, 600]):
        end = min(start + 600, duration)
        dest = ROOT / "evidence" / f"whisper_{index:02d}.json"
        if not dest.exists():
            wav = AUDIO / f"E_{index:02d}.wav"
            subprocess.run(["ffmpeg", "-v", "error", "-y", "-ss", str(start), "-i", str(video), "-t", str(end - start), "-vn", "-ac", "1", "-ar", "16000", str(wav)], check=True)
            LOGGER.info("transcribe_start", index=index, start=start, end=end)
            result = mlx_whisper.transcribe(str(wav), path_or_hf_repo=MODEL, verbose=None, condition_on_previous_text=False, temperature=0)
            result["start_offset_seconds"] = start
            dest.write_text(json.dumps(result, ensure_ascii=False, indent=2))
            LOGGER.info("transcribe_done", index=index, language=result["language"], segments=len(result["segments"]))
        result = json.loads(dest.read_text())
        for segment in result["segments"]:
            all_segments.append({"start": start + segment["start"], "end": min(duration, start + segment["end"]), "text": segment["text"].strip()})
    (ROOT / "全片转写段.json").write_text(json.dumps(all_segments, ensure_ascii=False, indent=2))
    lines = ["# E：第三方解说视频全片机器转写", "", "未经逐字人工听校；该视频为博主整理，不能自动当作Martin本人原话。", ""]
    for segment in all_segments:
        sec = int(segment["start"])
        lines.append(f"[{sec // 3600:02d}:{sec // 60 % 60:02d}:{sec % 60:02d}] {segment['text']}")
    (ROOT / "英文机器转写_带时间.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
