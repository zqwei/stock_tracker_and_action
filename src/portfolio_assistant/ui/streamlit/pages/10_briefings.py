from __future__ import annotations

import streamlit as st

from portfolio_assistant.assistant.daily_briefing import build_daily_briefing

st.title("Briefings")
st.caption("Daily deterministic risk summary.")

briefing = build_daily_briefing(open_positions=[])

st.markdown("### Highlights")
for line in briefing["highlights"]:
    st.write(f"- {line}")

st.markdown("### Actions")
for line in briefing["actions"]:
    st.write(f"- {line}")

st.caption(briefing["disclaimer"])
