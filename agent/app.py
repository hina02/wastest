import streamlit as st

import lance_reader as lr
from agent_core import run
from history import History

st.set_page_config(page_title="wastest agent", layout="wide")
st.title("wastest agent")

history_db = History()

# ----------------------------------------------------------------
# Sidebar
# ----------------------------------------------------------------
with st.sidebar:
    st.header("設定")

    namespaces = lr.list_namespaces()
    if not namespaces:
        st.error("Lance namespace が見つかりません")
        st.stop()
    namespace = st.selectbox("Namespace", namespaces)

    st.divider()

    existing_sessions = history_db.list_sessions()
    session_id = st.text_input("Session ID", value="default")

    if st.button("履歴クリア", use_container_width=True):
        history_db.clear(session_id)
        if "msg_history" in st.session_state:
            del st.session_state["msg_history"]
        st.rerun()

    if existing_sessions:
        st.caption("既存セッション")
        for s in existing_sessions:
            st.text(s)

# ----------------------------------------------------------------
# セッション変更時に agent の内部履歴をリセット
# ----------------------------------------------------------------
if st.session_state.get("_session_id") != session_id:
    st.session_state["_session_id"] = session_id
    st.session_state["msg_history"] = []

# ----------------------------------------------------------------
# チャット履歴を DuckDB から表示
# ----------------------------------------------------------------
for msg in history_db.load(session_id):
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

# ----------------------------------------------------------------
# 入力
# ----------------------------------------------------------------
if prompt := st.chat_input("メッセージを入力..."):
    with st.chat_message("user"):
        st.write(prompt)
    history_db.append(session_id, "user", prompt)

    with st.chat_message("assistant"):
        with st.spinner("応答中..."):
            answer, new_msgs = run(prompt, namespace, st.session_state["msg_history"])
        st.write(answer)

    st.session_state["msg_history"] = st.session_state["msg_history"] + list(new_msgs)
    history_db.append(session_id, "assistant", answer)
