from __future__ import annotations

import streamlit as st

from portfolio_assistant.ui.streamlit.helpers import (
    build_snapshot,
    ensure_initialized,
    render_account_scope_selector,
)


ensure_initialized()

st.title("Holdings")
st.caption("Open positions and unrealized P&L by symbol.")

account_scope = render_account_scope_selector()
snapshot = build_snapshot(account_scope)

if snapshot.pnl.open_positions:
    st.dataframe(
        [
            {
                "symbol": row.symbol,
                "account_id": row.account_id,
                "account_type": row.account_type.value,
                "instrument_type": row.instrument_type.value,
                "quantity": row.quantity,
                "average_cost": row.average_cost,
                "mark_price": row.mark_price,
                "market_value": row.market_value,
                "unrealized_pnl": row.unrealized_pnl,
            }
            for row in snapshot.pnl.open_positions
        ],
        use_container_width=True,
    )
else:
    st.info("No open holdings loaded yet.")
