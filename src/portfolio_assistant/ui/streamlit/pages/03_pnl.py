from __future__ import annotations

import streamlit as st

from portfolio_assistant.ui.streamlit.helpers import (
    build_snapshot,
    ensure_initialized,
    render_account_scope_selector,
)


ensure_initialized()

st.title("P&L")
st.caption("Realized performance by close date, symbol, and instrument type.")

account_scope = render_account_scope_selector()
snapshot = build_snapshot(account_scope)

if snapshot.realized_rows:
    st.dataframe(
        [
            {
                "closed_at": row.closed_at.date().isoformat(),
                "symbol": row.symbol,
                "account_id": row.account_id,
                "instrument_type": row.instrument_type.value,
                "quantity": row.quantity,
                "proceeds": row.proceeds,
                "cost_basis": row.cost_basis,
                "fees": row.fees,
                "realized_pnl": row.realized_pnl,
                "holding_days": row.holding_days,
                "wash_sale": row.is_wash_sale,
            }
            for row in snapshot.realized_rows
        ],
        use_container_width=True,
    )
else:
    st.info("No realized trades yet.")
