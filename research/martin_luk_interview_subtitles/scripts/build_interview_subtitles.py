"""校核并翻译访谈后半段，导出保持原视频时间的字幕。"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
import re
import sys
import textwrap
from pathlib import Path

import structlog

ROOT = Path(__file__).resolve().parents[1]
RESEARCH = ROOT.parent
SOURCE = RESEARCH / 'martin_luk_v1_source_audit/evidence'
COURSE = RESEARCH / 'martin_luk_final'
START = 3930.0
DURATION = 14950.609
CHUNK_SECONDS = 600
STEM = 'Martin_Luk_VKNEJA5r8zw_从01h05m30s至片尾_原视频时间轴'
LOGGER = structlog.get_logger()
# 仅修正本访谈语境中可识别的同音误识；不猜股票代码和含混数字。
TERMS = [
 (r'\b(?:Angular V-?WAP|Angular (?:Free Web|VWAP)|angle of view (?:app|up|at|web)|anchor (?:free web|through up|view app|view up|view web|viewap)|angle (?:free web|freeway|view app|view up|view at|feel well|for you up|for it up))\b','anchored VWAP'),
 (r'\bAnchorView app\b','anchored VWAP'),
 (r'\bBrian Shen\b','Brian Shannon'),
 (r'\bslip page\b','slippage'),
 (r'\bdraw dance\b','drawdowns'),
 (r'\b(?:9|21|50|150)[- ](?:year[- ]may|year[- ]ma|yerman|yermay)\b',None),
 (r'\bQs\b(?= and IWM)','QQQ'),
]


def normalize(text: str) -> str:
    """根据上下文已核对的交易术语，保留原始数字。"""
    for pattern, replacement in TERMS:
        if replacement is None:
            text = re.sub(pattern, lambda m: re.match(r'\d+', m.group()).group() + ' EMA', text, flags=re.I)
        else:
            text = re.sub(pattern, replacement, text, flags=re.I)
    return text


def prepare() -> list[dict]:
    """合并25段既有转写，用起点附近的新转写替换局部时轴。"""
    rows = []
    for path in sorted(SOURCE.glob('whisper_*.json')):
        offset = int(path.stem.split('_')[1]) * CHUNK_SECONDS
        for seg in json.loads(path.read_text())['segments']:
            start, end = round(offset + seg['start'], 2), min(DURATION, round(offset + seg['end'], 2))
            if end > START:
                rows.append({'key': f'A_{start}', 'start': max(START, start), 'end': end, 'raw_en': seg['text'].strip()})
    focus = json.loads((ROOT / 'evidence/whisper_verify_010530.json').read_text())
    boundary = 4049.48
    rows = [row for row in rows if row['start'] >= boundary]
    for seg in focus['segments']:
        start = round(focus['start_offset_seconds'] + seg['start'], 2)
        end = min(boundary, round(focus['start_offset_seconds'] + seg['end'], 2))
        if end > START and START <= start < boundary:
            rows.append({'key': f'focus_{start}', 'start': max(START, start), 'end': end, 'raw_en': seg['text'].strip()})
    rows.sort(key=lambda row: (row['start'], row['end']))
    edits = []
    for row in rows:
        row['en'] = normalize(row['raw_en'])
        if row['raw_en'] != row['en']:
            edits.append({'key': row['key'], 'time': row['start'], 'before': row['raw_en'], 'after': row['en'], 'reason': '已识别的交易术语同音误识；数字未推定改写'})
    overrides = json.loads((ROOT / 'source_overrides.json').read_text()) if (ROOT / 'source_overrides.json').exists() else {}
    for row in rows:
        if row['key'] in overrides:
            row['en'] = overrides[row['key']]['en']
    (ROOT / 'source_segments.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    (ROOT / 'source_corrections.json').write_text(json.dumps(edits, ensure_ascii=False, indent=2))
    return rows


def translate(rows: list[dict], model_path: Path) -> None:
    """复用有相同来源文本的缓存，剩余对白使用现有本地模型。"""
    sys.path.insert(0, str(COURSE / 'scripts'))
    import translate_clip_subtitles as translator
    import mlx.core as mx
    from mlx_lm import load

    translator.OUTPUT = ROOT
    cache_path = ROOT / 'translations_zh.json'
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    old_rows = {r['key']: r for r in json.loads((COURSE / 'subtitles/source_segments.json').read_text())}
    old_zh = json.loads((COURSE / 'subtitles/translations_zh.json').read_text())
    for row in rows:
        previous = old_rows.get(row['key'])
        if row['key'] not in cache and previous and previous['en'] == row['raw_en'] and row['raw_en'] == row['en']:
            cache[row['key']] = old_zh[row['key']]
    remaining = [row for row in rows if row['key'] not in cache]
    LOGGER.info('translation_start', segments=len(rows), reused=len(cache), remaining=len(remaining))
    if remaining:
        model, tokenizer = load(str(model_path))
        for index in range(0, len(remaining), translator.BATCH_SIZE):
            batch = remaining[index:index + translator.BATCH_SIZE]
            answers = translator.translate_batch(model, tokenizer, batch)
            cache.update({row['key']: zh for row, zh in zip(batch, answers, strict=True)})
            tmp = cache_path.with_suffix('.partial.json')
            tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
            tmp.replace(cache_path)
            LOGGER.info('translation_progress', completed=len(cache), total=len(rows))
            mx.clear_cache()
    # 无新增翻译时也保存复用缓存。
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2))


def timestamp(seconds: float) -> str:
    """返回SRT毫秒格式。"""
    value = round(seconds * 1000)
    whole, ms = divmod(value, 1000)
    return f'{whole // 3600:02d}:{whole // 60 % 60:02d}:{whole % 60:02d},{ms:03d}'


def export(rows: list[dict]) -> None:
    """按机器段落比例拆分长字幕，验证时轴及数字后原子导出。"""
    cache = json.loads((ROOT / 'translations_zh.json').read_text())
    manual_path = ROOT / 'manual_corrections.json'
    if manual_path.exists():
        cache.update({key: item['zh'] for key, item in json.loads(manual_path.read_text()).items()})
    cues, flags = [], []
    for index, row in enumerate(rows):
        zh = cache[row['key']].strip()
        for old, new in {'蜡烛':'K线','烛台':'K线','烛线':'K线','股权曲线':'资金曲线','停止损失':'止损','短头寸':'空头头寸','长头寸':'多头头寸'}.items():
            zh = zh.replace(old, new)
        zh = re.sub(r'^(?:译文|简体中文译文)[：:]\s*', '', zh)
        cache[row['key']] = zh
        assert zh and '译文待核对' not in zh, row['key']
        start, end = row['start'], row['end']
        if index + 1 < len(rows):
            end = min(end, rows[index + 1]['start'])
        if end <= start:
            flags.append({'key':row['key'], 'kind':'non_positive_interval','en':row['en'],'zh':zh})
            continue
        # shortcut: 比例拆分不是逐词强制对齐；需要精确打轴时用人工/强制对齐替换。
        words = row['en'].split()
        parts = max(1, (len(zh) + 31) // 32, int((end - start) / 6.5) + 1)
        parts = min(parts, max(1, int((end - start) / .8)), max(1,len(words)))
        for part in range(parts):
            cn = zh[round(len(zh)*part/parts):round(len(zh)*(part+1)/parts)].strip()
            en = ' '.join(words[round(len(words)*part/parts):round(len(words)*(part+1)/parts)])
            cues.append({'start':round(start+(end-start)*part/parts,3),'end':round(start+(end-start)*(part+1)/parts,3),'zh':cn,'en':en})
        source_numbers = set(re.findall(r'\d+(?:\.\d+)?', row['en']))
        target_numbers = set(re.findall(r'\d+(?:\.\d+)?', zh))
        if source_numbers != target_numbers:
            flags.append({'key':row['key'],'time':start,'kind':'number_difference','en':row['en'],'zh':zh,'source':sorted(source_numbers),'target':sorted(target_numbers)})
    for language in ['zh','bi','en']:
        blocks=[]
        for index,cue in enumerate(cues,1):
            value = cue['zh']+'\n'+cue['en'] if language=='bi' else cue[language]
            value=value.replace('-->','→')
            if language=='zh':value='\n'.join(textwrap.wrap(value,width=32))
            blocks.append(f"{index}\n{timestamp(cue['start'])} --> {timestamp(cue['end'])}\n{value}\n")
        path=ROOT/f'{STEM}.{language}.srt';tmp=path.with_suffix('.partial.srt');tmp.write_text('\n'.join(blocks));tmp.replace(path)
    for index,cue in enumerate(cues):
        assert START <= cue['start'] < cue['end'] <= DURATION, cue
        if index:assert cue['start'] >= cues[index-1]['end']
    (ROOT/'translations_zh.json').write_text(json.dumps(cache,ensure_ascii=False,indent=2))
    (ROOT/'subtitle_cues.json').write_text(json.dumps(cues,ensure_ascii=False,indent=2))
    (ROOT/'review_flags.json').write_text(json.dumps(flags,ensure_ascii=False,indent=2))
    lines=['# 访谈01:05:30至片尾：中文字幕阅读稿','','时间为完整视频原时间轴；机器译文及重点修订，未逐句人工听校。','']
    for row in rows:lines.extend([f"**[{timestamp(row['start'])}]** {cache[row['key']]}",''])
    (ROOT/'中文字幕阅读稿.md').write_text('\n'.join(lines))
    report={'source':'VKNEJA5r8zw','range_start_seconds':START,'video_duration_seconds':DURATION,'segments':len(rows),'cues':len(cues),'first_cue':cues[0]['start'],'last_cue':cues[-1]['end'],'timing':'原视频绝对时间；范围内有静音/停顿，未用假字幕填空','timeline_checks':'passed','number_flags':len(flags),'sha256_zh_srt':hashlib.sha256((ROOT/f'{STEM}.zh.srt').read_bytes()).hexdigest(),'limits':['机器转写/翻译，未逐句人工听校','长句比例拆分，并非逐词强制对齐','含混数字保留原转写并在重点处标注，不按策略常识猜改']}
    (ROOT/'字幕核验记录.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    LOGGER.info('export_complete',**report)


def main() -> None:
    """可断点续译；--export-only用于重点修订后的重新导出。"""
    parser=argparse.ArgumentParser()
    parser.add_argument('--model',type=Path,default=Path('/tmp/martin-subtitles/qwen'))
    parser.add_argument('--export-only',action='store_true')
    args=parser.parse_args()
    rows=json.loads((ROOT/'source_segments.json').read_text()) if args.export_only else prepare()
    if not args.export_only:translate(rows,args.model)
    export(rows)


if __name__ == '__main__':
    main()
