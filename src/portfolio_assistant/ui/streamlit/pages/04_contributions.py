from __future__ import annotations

import streamlit as st

from portfolio_assistant.ui.streamlit.helpers import (
    build_snapshot,
    ensure_initialized,
    render_account_scope_selector,
)


ensure_initialized()

st.title("Contributions")
st.caption("External deposits minus withdrawals, by month and account.")

account_scope = render_account_scope_selector()
snapshot = build_snapshot(account_scope)

st.metric("Net Contributions", f"${snapshot.contributions.net_total:,.2f}")

st.markdown("### Monthly Net Contributions")
if snapshot.contributions.by_month:
    st.dataframe(snapshot.contributions.by_month, use_container_width=True)
else:
    st.info("No external contribution activity found.")

st.markdown("### By Account")
if snapshot.contributions.by_account:
    st.dataframe(snapshot.contributions.by_account, use_container_width=True)
else:
    st.info("No account-level contribution totals yet.")
