from pathlib import Path
import json,subprocess,structlog
r=Path('/Volumes/E/github/Wyckoff-Analysis/research/martin_luk_final')
sources={'A':'/Users/wxjer/Downloads/YTDown.com_YouTube_Media_VKNEJA5r8zw_969-Return-in-1-Year-The-Pullback-Strategy-of-a-Trading-Champion_001_1080p.mp4','B':'/Volumes/E/Download/YTDown.com_YouTube_Media_WjkWNi-ASn8_EP104-13-Oct-2026_001_1080p.mp4','C':'/Volumes/E/Download/YTDown.com_YouTube_Media_7NKlKaRIW9g_My-first-stream-12-Nov-2025_001_1080p.mp4','D':'/Volumes/E/Download/YTDown.com_YouTube_Media_z_IxO8qskgs_Short-updates-12-Oct-2026_001_1080p.mp4'}
log=structlog.get_logger()
rows=json.loads((r/'clips_manifest.json').read_text())
for i,c in enumerate(rows):
 dest=r/'clips'/f'{c["id"]}.mp4'
 if dest.exists():continue
 tmp=dest.with_suffix('.partial.mp4')
 log.info('cut_start',index=i+1,total=len(rows),clip=c['id'])
 subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-y','-ss',str(c['start']),'-i',sources[c['source']],'-t',str(c['end']-c['start']),'-map','0:v:0','-map','0:a:0','-vf','scale=1280:-2','-c:v','h264_videotoolbox','-b:v','1400k','-c:a','aac','-b:a','96k','-movflags','+faststart',str(tmp)],check=True)
 tmp.replace(dest)
 log.info('cut_done',index=i+1,total=len(rows),clip=c['id'],bytes=dest.stat().st_size)
