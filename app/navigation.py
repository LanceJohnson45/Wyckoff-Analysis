import streamlit as st


def show_right_nav():
    """
    Keep backward-compatible API name, but render navigation in the left sidebar
    using Streamlit-native page links for consistent styling and behavior.
    """
    with st.sidebar:
        st.markdown("### 导航")
        st.page_link("streamlit_app.py", label="读盘室", icon="💬")
        st.page_link("pages/Export.py", label="数据导出", icon="📁")
        st.page_link("pages/WyckoffScreeners.py", label="沙里淘金", icon="🧭")
        st.page_link("pages/AIAnalysis.py", label="大师模式", icon="🤖")
        st.page_link("pages/Portfolio.py", label="持仓管理", icon="💼")
        st.page_link("pages/RecommendationTracking.py", label="推荐跟踪", icon="🎯")
        st.page_link("pages/Settings.py", label="设置", icon="⚙️")
        st.page_link("pages/Changelog.py", label="更新日志", icon="📢")
        st.link_button(
            "⭐ GitHub",
            "https://github.com/YoungCan-Wang/Wyckoff-Analysis",
            use_container_width=True,
        )
        st.divider()
    return st.container()
