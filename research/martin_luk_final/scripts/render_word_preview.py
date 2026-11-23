"""使用打包运行时及其字体配置渲染Word教材预览。"""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORD = ROOT / "Martin_Luk_详细实操教程_V3.0.docx"


def main() -> None:
    """显式加载Fontconfig配置，避免中文字体回退为缺字字形。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", type=Path, required=True, help="依赖加载工具返回的dependencies目录")
    parser.add_argument("--renderer", type=Path, required=True, help="documents技能的render_docx.py路径")
    parser.add_argument("--output-dir", type=Path, required=True, help="预览图片和PDF的输出目录")
    args = parser.parse_args()
    runtime = args.runtime_root.resolve()
    python = runtime / "python/bin/python3"
    fontconfig = runtime / "native/libreoffice-headless/libreoffice/LibreOfficeDev.app/Contents/Resources/fontconfig/fonts.conf"
    renderer = args.renderer.resolve()
    for path in (python, fontconfig, renderer, WORD):
        if not path.is_file():
            raise FileNotFoundError(path)
    environment = os.environ.copy()
    environment["FONTCONFIG_FILE"] = str(fontconfig)
    subprocess.run(
        [str(python), str(renderer), str(WORD), "--output_dir", str(args.output_dir.resolve()), "--emit_pdf"],
        env=environment,
        check=True,
    )


if __name__ == "__main__":
    main()
