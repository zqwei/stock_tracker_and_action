from __future__ import annotations

import streamlit as st

from portfolio_assistant.config.settings import get_settings

settings = get_settings()

st.title("Settings")
st.caption("Local runtime and data-path configuration.")

st.write("Database path")
st.code(str(settings.db_path))

st.write("Default currency")
st.code(settings.default_currency)
