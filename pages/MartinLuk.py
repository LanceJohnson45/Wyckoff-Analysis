"""项目内 Martin Luk 工作台，沿用现有登录和导航。"""

from app.layout import setup_page
from app.martin_luk_view import render_martin_luk
from app.navigation import show_right_nav

setup_page(page_title="Martin 辅助扫描", page_icon="🔎")
with show_right_nav():
    render_martin_luk()
