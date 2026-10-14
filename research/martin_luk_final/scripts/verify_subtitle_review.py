"""核对所有实际字幕文件、审阅覆盖及关键语义回归。"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import re
from pathlib import Path

import structlog

ROOT = Path(__file__).resolve().parents[1]
REVIEW = ROOT / 'subtitle_review_2026-10-14'
LOGGER = structlog.get_logger()


def main() -> None:
    """全量检查磁盘文本并将修订前后对应到56个片段。"""
    rows = json.loads((ROOT/'subtitles/source_segments.json').read_text())
    final = json.loads((ROOT/'subtitles/translations_zh.json').read_text())
    manual = json.loads((ROOT/'subtitles/manual_corrections.json').read_text())
    before = {row['key']:row for row in json.loads((REVIEW/'before_review.json').read_text())}
    models = json.loads((REVIEW/'model_review.json').read_text())
    clips = json.loads((ROOT/'clips_manifest.json').read_text())
    cues = json.loads((ROOT/'subtitles/clip_cues.json').read_text())
    assert len(rows)==len(before)==len(models)==1703
    assert len(clips)==len(cues)==56
    assert {row['key'] for row in rows}==set(models)
    assert all(row['key'] in final for row in rows)
    for key,value in manual.items():assert final[key]==value['zh']
    for text in final.values():
        assert text.strip() and '译文待核对' not in text
        assert not re.search(r'[ぁ-ゖァ-ヺ]',text)
    # 已确认的实质错误必须维持修正，不将模型提议直接写回字幕。
    checks={
        'A_11036.02':['0.5%','50美元'],
        'A_7099.62':['60%～70%','时候'],
        'A_10889.44':['约2.3%','低于2.5%'],
        'B_422.48':['USO','盘前'],
        'C_1407.7':['现金','仓位'],
        'C_3401.02':['止损'],
        'C_2976.84':['0.3%','0.4%'],
        'D_4098.46':['弱收盘'],
        'D_3271.88':['低点不断抬高'],
        'D_4695.2':['低于'],
        'D_4708.92':['1.5%'],
        'D_5774.68':['阻力下方'],
        'E_642.96':['可能'],
        'E_556.52':['讲者称','40R'],
    }
    for key,expected in checks.items():assert all(value in final[key] for value in expected),key
    files=0
    for clip in clips:
        values=cues[clip['id']]
        for language in ['zh','bi','en']:
            for ext in ['srt','vtt']:
                path=ROOT/'subtitles'/f"{clip['id']}.{language}.{ext}"
                blocks=path.read_text().strip().split('\n\n')
                if ext=='vtt':assert blocks.pop(0)=='WEBVTT'
                assert len(blocks)==len(values),path.name
                previous=0
                for index,(block,cue) in enumerate(zip(blocks,values,strict=True),1):
                    lines=block.splitlines();assert int(lines[0])==index
                    separator=',' if ext=='srt' else '.'
                    pattern=rf'\d{{2}}:\d{{2}}:\d{{2}}{re.escape(separator)}\d{{3}} --> \d{{2}}:\d{{2}}:\d{{2}}{re.escape(separator)}\d{{3}}'
                    assert re.fullmatch(pattern,lines[1]),path.name
                    expected=cue['zh']+'\n'+cue['en'] if language=='bi' else cue[language]
                    expected=expected.replace('-->','→').replace('<','&lt;').replace('>','&gt;')
                    assert '\n'.join(lines[2:])==expected,path.name
                    assert previous<=cue['start']<cue['end']<=clip['end']-clip['start']+.001
                    previous=cue['end']
                files+=1
    log=[]
    for row in rows:
        key=row['key'];old=before[key]
        log.append({'key':key,'source':row['source'],'start':row['start'],'raw_en':old['en'],'reviewed_en':row['en'],'before_zh':old['zh'],'final_zh':final[key],'changed':old['zh']!=final[key],'method':'重点对照修订' if key in manual else '全量机器对照与规则检查；未作重点改写','model_proposal_automatically_applied':False})
    (REVIEW/'review_log.json').write_text(json.dumps(log,ensure_ascii=False,indent=2))
    coverage=[]
    for clip in clips:
        selected=[r for r in log if r['source']==clip['source'] and r['start']<clip['end'] and before[r['key']]['end']>clip['start']]
        assert selected,clip['id']
        coverage.append({'clip':clip['id'],'source':clip['source'],'segments_checked':len(selected),'changed_segments':sum(r['changed'] for r in selected),'cues':len(cues[clip['id']])})
    report={'date':'2026-10-14','clips':len(clips),'source_segments_checked':len(rows),'source_counts':dict(Counter(row['source'] for row in rows)),'changed_unique_translation_segments':sum(r['changed'] for r in log),'curated_records_total':len(manual),'caption_cues':sum(map(len,cues.values())),'subtitle_files_checked':files,'critical_semantic_checks':len(checks),'all_srt_vtt_text_and_timeline':'passed','model_review_coverage':len(models),'automatic_model_proposals_applied':0,'explanation':'模型全量对照用于定位问题；其提议有方向/数字及串句错误，未自动覆盖原字幕。采用对照原文/已有重点听辨和图表后的修订。','uncertain_unique_segments':sum(bool(re.search('存疑|听辨不清|转写.*遗漏|转写.*误',final[row['key']])) for row in rows),'limits':['未逐秒人工听校全部约175分钟片段','字幕时间沿用机器段落并比例拆分；保护数字/EMA/存疑标记，非逐词强制对齐','E第三方原话中的收益/规则分歧保留，正文另作核对','仍可能存在细碎残句、专名或听辨错误'],'coverage':coverage,'translation_sha256':hashlib.sha256((ROOT/'subtitles/translations_zh.json').read_bytes()).hexdigest()}
    (ROOT/'字幕校验记录_2026-10-14.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    LOGGER.info('subtitle_review_verified',**{k:report[k] for k in ['clips','source_segments_checked','changed_unique_translation_segments','curated_records_total','caption_cues','subtitle_files_checked','critical_semantic_checks']})


if __name__=='__main__':
    main()
