from __future__ import annotations

import streamlit as st

from portfolio_assistant.assistant.ask_gpt import answer_question

st.title("Ask GPT")
st.caption("Educational analysis assistant. No auto-trading.")

question = st.text_area("Question", placeholder="Ask about your portfolio or risk...")
web_enabled = st.checkbox("Enable web mode", value=False)

if st.button("Submit", type="primary"):
    if not question.strip():
        st.warning("Enter a question first.")
    else:
        st.write(answer_question(question, web_enabled=web_enabled))
