"""仅分析公开行情与当前会话上传文件的独立网页入口。"""

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from app.martin_luk_view import render_martin_luk

st.set_page_config(page_title="Martin 辅助扫描", page_icon="🔎", layout="wide")
render_martin_luk()
