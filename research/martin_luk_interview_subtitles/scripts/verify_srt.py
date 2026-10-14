"""独立解析最终SRT，检查编号、边界、空字幕和输出一致性。"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import structlog

ROOT = Path(__file__).resolve().parents[1]
LOGGER = structlog.get_logger()


def seconds(value: str) -> float:
    """把SRT时间字符串转为秒。"""
    h, m, s, ms = map(int, re.split(r'[:,]', value))
    assert 0 <= m < 60 and 0 <= s < 60 and 0 <= ms < 1000
    return h * 3600 + m * 60 + s + ms / 1000


def main() -> None:
    """从实际磁盘SRT重新读取，避免仅验证内存中的字幕结构。"""
    cues = json.loads((ROOT / 'subtitle_cues.json').read_text())
    report = json.loads((ROOT / '字幕核验记录.json').read_text())
    outputs = []
    for path in sorted(ROOT.glob('*.srt')):
        blocks = path.read_text(encoding='utf-8').strip().split('\n\n')
        assert len(blocks) == len(cues), path.name
        previous = 0.0
        for index, block in enumerate(blocks, 1):
            lines = block.splitlines()
            assert int(lines[0]) == index and len(lines) >= 3
            match = re.fullmatch(r'(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})', lines[1])
            assert match, (path.name, index)
            start, end = map(seconds, match.groups())
            assert 3930 <= start < end <= 14950.609
            assert start >= previous and abs(start - cues[index - 1]['start']) < .0001
            assert abs(end - cues[index - 1]['end']) < .0001
            assert ''.join(lines[2:]).strip() and '译文待核对' not in block
            previous = end
        outputs.append({'file': path.name, 'cues': len(blocks), 'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    assert len(outputs) == 3
    source = json.loads((ROOT / 'source_segments.json').read_text())
    cache = json.loads((ROOT / 'translations_zh.json').read_text())
    assert all(row['key'] in cache for row in source)
    assert all(not re.search(r'[ぁ-ゖァ-ヺ]', cache[row['key']]) for row in source)
    report.update({'srt_disk_parse': 'passed', 'outputs': outputs, 'manual_translation_corrections': len(json.loads((ROOT / 'manual_corrections.json').read_text())), 'source_correction_events': len(json.loads((ROOT / 'source_corrections.json').read_text())), 'maximum_internal_gap_seconds': round(max(cues[i]['start'] - cues[i - 1]['end'] for i in range(1,len(cues))), 3), 'explicit_uncertainty_cues': sum('存疑' in cue['zh'] for cue in cues)})
    (ROOT / '字幕核验记录.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    LOGGER.info('srt_disk_verification_passed', files=len(outputs), cues=len(cues))


if __name__ == '__main__':
    main()
