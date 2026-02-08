from __future__ import annotations

import streamlit as st

from portfolio_assistant.ui.streamlit.helpers import (
    account_name_map,
    build_snapshot,
    ensure_initialized,
    render_account_scope_selector,
)


ensure_initialized()

st.title("Wash Sale Risk")
st.caption("Cross-account replacement-buy warnings around taxable loss sales.")

account_scope = render_account_scope_selector()
snapshot = build_snapshot(account_scope)
account_names = account_name_map(snapshot.accounts)

if not snapshot.wash_sale_risks:
    st.info("No wash sale risks detected in current dataset.")
    st.stop()

st.warning(f"Detected {len(snapshot.wash_sale_risks)} wash-sale risk event(s).")
st.dataframe(
    [
        {
            "symbol": risk.symbol,
            "loss_sale_date": risk.loss_sale_date.date().isoformat(),
            "replacement_buy_date": risk.replacement_buy_date.date().isoformat(),
            "sale_account": account_names.get(risk.sale_account_id, risk.sale_account_id),
            "replacement_account": account_names.get(risk.replacement_account_id, risk.replacement_account_id),
            "sale_account_type": risk.sale_account_type.value,
            "replacement_account_type": risk.replacement_account_type.value,
            "loss_amount": risk.loss_amount,
            "notes": risk.notes,
        }
        for risk in snapshot.wash_sale_risks
    ],
    use_container_width=True,
)
