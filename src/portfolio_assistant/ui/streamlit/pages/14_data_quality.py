from __future__ import annotations

import streamlit as st

from portfolio_assistant.db.models import InstrumentType
from portfolio_assistant.db.repository import list_trades, raw_trade_payload_sample, table_counts
from portfolio_assistant.ingest.validators import validate_trades
from portfolio_assistant.ui.streamlit.helpers import ensure_initialized


ensure_initialized()

st.title("Data Quality")
st.caption("Import diagnostics, mapping metadata, and parsing quality checks.")

counts = table_counts()
st.dataframe(
    [{"table": name, "row_count": value} for name, value in counts.items()],
    use_container_width=True,
)

trades = list_trades()
validation_issues = validate_trades(trades)
unparsed_options = [
    trade
    for trade in trades
    if trade.instrument_type == InstrumentType.OPTION
    and not trade.option_symbol_raw
    and (not trade.underlying or not trade.expiration or trade.strike is None or not trade.call_put)
]

col1, col2 = st.columns(2)
col1.metric("Trade validation issues", len(validation_issues))
col2.metric("Unparsed option contracts", len(unparsed_options))

if validation_issues:
    st.dataframe(
        [{"row_index": idx, "issue": message} for idx, message in validation_issues[:200]],
        use_container_width=True,
    )

st.markdown("### Raw Import Mapping Samples")
sample = raw_trade_payload_sample(limit=20)
if sample:
    st.dataframe(sample, use_container_width=True)
else:
    st.info("No raw trade imports recorded yet.")

if st.session_state.get("last_trade_import_unmapped"):
    st.warning("Last trade import had unmapped required fields:")
    st.write(st.session_state["last_trade_import_unmapped"])

if st.session_state.get("last_cash_import_unmapped"):
    st.warning("Last cash import had unmapped required fields:")
    st.write(st.session_state["last_cash_import_unmapped"])
