from __future__ import annotations

import streamlit as st

from portfolio_assistant.ui.streamlit.helpers import (
    build_snapshot,
    ensure_initialized,
    render_account_scope_selector,
)


ensure_initialized()

st.title("Overview")
st.caption("Consolidated snapshot across taxable and IRA accounts.")

account_scope = render_account_scope_selector()
snapshot = build_snapshot(account_scope)

realized_total = sum(row.realized_pnl for row in snapshot.realized_rows)
unrealized_total = sum((row.unrealized_pnl or 0.0) for row in snapshot.pnl.open_positions)
contributions_total = snapshot.contributions.net_total

col1, col2, col3, col4 = st.columns(4)
col1.metric("Realized P&L", f"${realized_total:,.2f}")
col2.metric("Unrealized P&L", f"${unrealized_total:,.2f}")
col3.metric("Net Contributions", f"${contributions_total:,.2f}")
col4.metric("Wash Sale Risks", len(snapshot.wash_sale_risks))

symbol_totals: dict[str, float] = {}
for row in snapshot.realized_rows:
    symbol_totals[row.symbol] = symbol_totals.get(row.symbol, 0.0) + row.realized_pnl

st.markdown("### Realized P&L by Symbol")
if symbol_totals:
    st.dataframe(
        [
            {"symbol": symbol, "realized_pnl": round(value, 2)}
            for symbol, value in sorted(symbol_totals.items(), key=lambda item: item[1], reverse=True)
        ],
        use_container_width=True,
    )
else:
    st.info("No realized P&L yet. Import trade CSV files first.")

st.markdown("### Open Positions")
if snapshot.pnl.open_positions:
    st.dataframe(
        [
            {
                "symbol": row.symbol,
                "account_id": row.account_id,
                "instrument_type": row.instrument_type.value,
                "quantity": row.quantity,
                "average_cost": row.average_cost,
                "unrealized_pnl": row.unrealized_pnl,
            }
            for row in snapshot.pnl.open_positions
        ],
        use_container_width=True,
    )
else:
    st.info("No open positions detected.")
