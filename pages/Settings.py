import streamlit as st

from app.layout import setup_page
from app.navigation import show_right_nav
from integrations.supabase_client import save_user_settings
from integrations.llm_client import OPENAI_COMPATIBLE_BASE_URLS
from app.ui_helpers import show_page_loading

setup_page(page_title="设置", page_icon="⚙️")

# Show Navigation
content_col = show_right_nav()
with content_col:

    st.title("⚙️ 设置 (Settings)")
    st.markdown("配置您的 API Key 和通知服务，让 Akshare 更加智能。")

    # 获取当前用户 ID
    user = st.session_state.get("user") or {}
    user_id = user.get("id") if isinstance(user, dict) else None
    if not user_id:
        st.error("无法识别当前用户，设置页已拒绝展示。请重新登录。")
        st.stop()

    # 兼容旧会话：新增字段可能尚未初始化，先补默认值，避免 AttributeError。
    st.session_state.setdefault("openai_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("openai", ""))
    st.session_state.setdefault("gemini_base_url", "")
    st.session_state.setdefault("zhipu_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("zhipu", ""))
    st.session_state.setdefault("minimax_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("minimax", ""))
    st.session_state.setdefault("deepseek_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("deepseek", ""))
    st.session_state.setdefault("qwen_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("qwen", ""))
    st.session_state.setdefault("kimi_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("kimi", ""))
    st.session_state.setdefault("volcengine_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("volcengine", ""))

    for key in (
        "zhipu_api_key", "zhipu_model",
        "minimax_api_key", "minimax_model",
        "qwen_api_key", "qwen_model",
        "kimi_api_key", "kimi_model",
        "volcengine_api_key", "volcengine_model",
    ):
        st.session_state.setdefault(key, "")

    # 顶部展示 user_id，方便复制
    with st.expander("🔑 账户信息", expanded=True):
        st.info(f"当前用户 ID (SUPABASE_USER_ID): `{user_id}`")
        st.caption("请复制此 ID 并配置到 GitHub Secrets 的 SUPABASE_USER_ID 中，以便定时任务能识别您的账户。")


    def on_save_settings():
        """保存配置到云端"""
        if not user_id:
            st.error("用户未登录，无法保存配置")
            return

        custom_providers = {
            "zhipu": {
                "apikey": st.session_state.zhipu_api_key,
                "baseurl": st.session_state.zhipu_base_url,
                "model": st.session_state.zhipu_model,
            },
            "minimax": {
                "apikey": st.session_state.minimax_api_key,
                "baseurl": st.session_state.minimax_base_url,
                "model": st.session_state.minimax_model,
            },
            "qwen": {
                "apikey": st.session_state.qwen_api_key,
                "baseurl": st.session_state.qwen_base_url,
                "model": st.session_state.qwen_model,
            },
            "kimi": {
                "apikey": st.session_state.kimi_api_key,
                "baseurl": st.session_state.kimi_base_url,
                "model": st.session_state.kimi_model,
            },
            "volcengine": {
                "apikey": st.session_state.volcengine_api_key,
                "baseurl": st.session_state.volcengine_base_url,
                "model": st.session_state.volcengine_model,
            },
        }
        settings = {
            # 通知
            "feishu_webhook": st.session_state.feishu_webhook,
            "wecom_webhook": st.session_state.wecom_webhook,
            "dingtalk_webhook": st.session_state.dingtalk_webhook,
            # 大模型
            "gemini_api_key": st.session_state.gemini_api_key,
            "gemini_model": st.session_state.gemini_model,
            "gemini_base_url": st.session_state.gemini_base_url,
            "openai_api_key": st.session_state.openai_api_key,
            "openai_model": st.session_state.openai_model,
            "openai_base_url": st.session_state.openai_base_url,
            "deepseek_api_key": st.session_state.deepseek_api_key,
            "deepseek_model": st.session_state.deepseek_model,
            "deepseek_base_url": st.session_state.deepseek_base_url,
            "custom_providers": custom_providers,
            # 其它
            "tushare_token": st.session_state.tushare_token,
            "tg_bot_token": st.session_state.tg_bot_token,
            "tg_chat_id": st.session_state.tg_chat_id,
        }

        loading = show_page_loading(title="加载中...", subtitle="正在保存到云端")
        try:
            if save_user_settings(user_id, settings):
                st.toast("✅ 配置已保存到云端", icon="☁️")
            else:
                st.toast("❌ 保存失败，请检查网络", icon="⚠️")
        finally:
            loading.empty()


    col1, col2 = st.columns([2, 1])

    with col1:
        # 1. 通知配置：飞书 / 企微 / 钉钉
        st.subheader("🔔 通知配置")
        with st.container(border=True):
            st.markdown(
                "配置群机器人的 **Webhook**，定时任务与批量操作完成后可自动推送到对应群。"
            )

            new_feishu_webhook = st.text_input(
                "飞书 Webhook URL",
                value=st.session_state.feishu_webhook,
                type="password",
                placeholder="https://open.feishu.cn/open-apis/bot/v2/hook/...",
                help="飞书自定义机器人 Webhook，详见飞书官方文档。",
            )

            new_wecom_webhook = st.text_input(
                "企业微信 Webhook URL",
                value=st.session_state.wecom_webhook,
                type="password",
                placeholder="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...",
                help="企业微信群机器人 Webhook，可选。",
            )

            new_dingtalk_webhook = st.text_input(
                "钉钉 Webhook URL",
                value=st.session_state.dingtalk_webhook,
                type="password",
                placeholder="https://oapi.dingtalk.com/robot/send?access_token=...",
                help="钉钉群机器人 Webhook，可选。",
            )

            if st.button("💾 保存通知配置", key="save_webhook"):
                st.session_state.feishu_webhook = new_feishu_webhook
                st.session_state.wecom_webhook = new_wecom_webhook
                st.session_state.dingtalk_webhook = new_dingtalk_webhook
                on_save_settings()

        st.divider()

        # 2. 大模型配置：Gemini / OpenAI / 智谱 / Minimax / DeepSeek / Qwen
        st.subheader("🧠 AI 配置")
        with st.container(border=True):
            st.markdown("配置各家大模型的 API Key 与默认模型，后续在任务/研报中按需切换使用。")

            st.markdown("**Gemini (Google)**")
            new_gemini_key = st.text_input(
                "Gemini API Key",
                value=st.session_state.gemini_api_key,
                type="password",
                placeholder="AIzaSy...",
                help="获取 Key: Google AI Studio。",
            )
            new_gemini_model = st.text_input(
                "Gemini 默认模型",
                value=st.session_state.gemini_model,
                placeholder="gemini-3.1-flash-lite-preview",
                help="例如：gemini-3.1-flash-lite-preview、gemini-2.5-flash 等。",
            )
            new_gemini_base_url = st.text_input(
                "Gemini Base URL（可选）",
                value=st.session_state.gemini_base_url,
                placeholder="留空使用官方默认",
                help="仅用于经代理网关转发 Gemini 的场景；普通情况下保持留空。",
            )

            st.markdown("---")
            st.markdown("**OpenAI / 兼容 OpenAI 协议的厂商**")
            new_openai_key = st.text_input(
                "OpenAI API Key",
                value=st.session_state.openai_api_key,
                type="password",
                placeholder="sk-...",
            )
            new_openai_model = st.text_input(
                "OpenAI 默认模型",
                value=st.session_state.openai_model,
                placeholder="gpt-4.1-mini",
            )
            new_openai_base_url = st.text_input(
                "OpenAI Base URL",
                value=st.session_state.openai_base_url,
                placeholder="https://api.openai.com/v1",
                help="支持自定义网关地址；当前值会作为优先地址，未配置时回退到系统默认值。",
            )

            st.markdown("---")
            st.markdown("**智谱 AI (GLM)**")
            new_zhipu_key = st.text_input(
                "智谱 API Key",
                value=st.session_state.zhipu_api_key,
                type="password",
                placeholder="xxxxx",
            )
            new_zhipu_model = st.text_input(
                "智谱默认模型",
                value=st.session_state.zhipu_model,
                placeholder="glm-4-air",
            )
            new_zhipu_base_url = st.text_input(
                "智谱 Base URL",
                value=st.session_state.zhipu_base_url,
                placeholder="https://open.bigmodel.cn/api/paas/v4",
            )

            st.markdown("---")
            st.markdown("**Minimax**")
            new_minimax_key = st.text_input(
                "Minimax API Key",
                value=st.session_state.minimax_api_key,
                type="password",
                placeholder="xxxxx",
            )
            new_minimax_model = st.text_input(
                "Minimax 默认模型",
                value=st.session_state.minimax_model,
                placeholder="abab6.5-chat",
            )
            new_minimax_base_url = st.text_input(
                "Minimax Base URL",
                value=st.session_state.minimax_base_url,
                placeholder="https://api.minimax.chat/v1",
            )

            st.markdown("---")
            st.markdown("**DeepSeek**")
            new_deepseek_key = st.text_input(
                "DeepSeek API Key",
                value=st.session_state.deepseek_api_key,
                type="password",
                placeholder="sk-...",
            )
            new_deepseek_model = st.text_input(
                "DeepSeek 默认模型",
                value=st.session_state.deepseek_model,
                placeholder="deepseek-chat",
            )
            new_deepseek_base_url = st.text_input(
                "DeepSeek Base URL",
                value=st.session_state.deepseek_base_url,
                placeholder="https://api.deepseek.com/v1",
            )

            st.markdown("---")
            st.markdown("**Qwen (通义千问)**")
            new_qwen_key = st.text_input(
                "Qwen API Key",
                value=st.session_state.qwen_api_key,
                type="password",
                placeholder="sk-...",
            )
            new_qwen_model = st.text_input(
                "Qwen 默认模型",
                value=st.session_state.qwen_model,
                placeholder="qwen-max",
            )
            new_qwen_base_url = st.text_input(
                "Qwen Base URL",
                value=st.session_state.qwen_base_url,
                placeholder="https://dashscope.aliyuncs.com/compatible-mode/v1",
            )

            st.markdown("---")
            st.markdown("**Kimi (Moonshot)**")
            new_kimi_key = st.text_input(
                "Kimi API Key",
                value=st.session_state.kimi_api_key,
                type="password",
                placeholder="sk-...",
            )
            new_kimi_model = st.text_input(
                "Kimi 默认模型",
                value=st.session_state.kimi_model,
                placeholder="moonshot-v1-8k",
            )
            new_kimi_base_url = st.text_input(
                "Kimi Base URL",
                value=st.session_state.kimi_base_url,
                placeholder="https://api.moonshot.cn/v1",
            )

            st.markdown("---")
            st.markdown("**火山引擎 (Volcengine Ark)**")
            new_volc_key = st.text_input(
                "火山引擎 API Key",
                value=st.session_state.volcengine_api_key,
                type="password",
                placeholder="xxxxx",
            )
            new_volc_model = st.text_input(
                "火山引擎默认模型",
                value=st.session_state.volcengine_model,
                placeholder="ep-xxxxxx",
            )
            new_volc_base_url = st.text_input(
                "火山引擎 Base URL",
                value=st.session_state.volcengine_base_url,
                placeholder="https://ark.cn-beijing.volces.com/api/v3",
            )

            if st.button("💾 保存 AI 配置", key="save_ai"):
                st.session_state.gemini_api_key = new_gemini_key
                st.session_state.gemini_model = new_gemini_model
                st.session_state.gemini_base_url = new_gemini_base_url
                st.session_state.openai_api_key = new_openai_key
                st.session_state.openai_model = new_openai_model
                st.session_state.openai_base_url = new_openai_base_url
                st.session_state.zhipu_api_key = new_zhipu_key
                st.session_state.zhipu_model = new_zhipu_model
                st.session_state.zhipu_base_url = new_zhipu_base_url
                st.session_state.minimax_api_key = new_minimax_key
                st.session_state.minimax_model = new_minimax_model
                st.session_state.minimax_base_url = new_minimax_base_url
                st.session_state.deepseek_api_key = new_deepseek_key
                st.session_state.deepseek_model = new_deepseek_model
                st.session_state.deepseek_base_url = new_deepseek_base_url
                st.session_state.qwen_api_key = new_qwen_key
                st.session_state.qwen_model = new_qwen_model
                st.session_state.qwen_base_url = new_qwen_base_url
                st.session_state.kimi_api_key = new_kimi_key
                st.session_state.kimi_model = new_kimi_model
                st.session_state.kimi_base_url = new_kimi_base_url
                st.session_state.volcengine_api_key = new_volc_key
                st.session_state.volcengine_model = new_volc_model
                st.session_state.volcengine_base_url = new_volc_base_url
                on_save_settings()

        st.divider()

        # 3. 数据源
        st.subheader("📊 数据源配置")
        with st.container(border=True):
            st.markdown("**Tushare Token**（可选）用于行情、市值等。不配置时优先用 akshare/baostock/efinance，三者均失败时才需 Tushare。")
            new_tushare = st.text_input(
                "Tushare Token",
                value=st.session_state.tushare_token,
                type="password",
                placeholder="Tushare Pro token",
                key="tushare_input",
            )
            if st.button("💾 保存数据源配置", key="save_tushare"):
                st.session_state.tushare_token = new_tushare
                on_save_settings()

        st.divider()

        # 4. 私人决断
        st.subheader("🕶️ 私人决断")
        with st.container(border=True):
            st.markdown("可选，用于 Telegram 私密推送买卖建议。")
            new_tg_bot = st.text_input("Telegram Bot Token", value=st.session_state.tg_bot_token, type="password", key="tg_bot")
            new_tg_chat = st.text_input("Telegram Chat ID", value=st.session_state.tg_chat_id, type="password", key="tg_chat")
            if st.button("💾 保存 Step4 配置", key="save_step4"):
                st.session_state.tg_bot_token = new_tg_bot
                st.session_state.tg_chat_id = new_tg_chat
                on_save_settings()

        st.info("☁️ 您的配置已启用云端同步，将在所有登录设备间自动漫游。")
