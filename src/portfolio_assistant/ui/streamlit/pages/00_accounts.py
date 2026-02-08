from __future__ import annotations

from uuid import uuid4

import streamlit as st

from portfolio_assistant.db.models import Account, AccountType
from portfolio_assistant.db.repository import list_accounts, save_account
from portfolio_assistant.ui.streamlit.helpers import ensure_initialized


ensure_initialized()

st.title("Accounts")
st.caption("Create and manage brokerage accounts used for imports and reporting.")

accounts = list_accounts()
if accounts:
    st.dataframe(
        [
            {
                "account_id": account.account_id,
                "label": account.account_label,
                "broker": account.broker,
                "account_type": account.account_type.value,
            }
            for account in accounts
        ],
        use_container_width=True,
    )
else:
    st.info("No accounts yet. Add your first account below.")

with st.form("add_account"):
    st.subheader("Add or Update Account")
    account_label = st.text_input("Account label", placeholder="Taxable Main")
    broker = st.text_input("Broker", placeholder="Fidelity")
    account_type = st.selectbox("Account type", [t.value for t in AccountType])
    account_id = st.text_input("Account ID (optional)", placeholder="Leave blank to auto-generate")
    submitted = st.form_submit_button("Save Account", type="primary")

if submitted:
    if not account_label.strip() or not broker.strip():
        st.error("Account label and broker are required.")
    else:
        resolved_id = account_id.strip() or str(uuid4())
        save_account(
            Account(
                account_id=resolved_id,
                account_label=account_label.strip(),
                broker=broker.strip(),
                account_type=AccountType(account_type),
            )
        )
        st.success(f"Account saved: {resolved_id}")
        st.rerun()
