from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import streamlit as st

from portfolio_assistant.config.paths import data_dir
from portfolio_assistant.db.repository import get_account, list_accounts, save_trade_import
from portfolio_assistant.ingest.csv_import import import_trades_csv
from portfolio_assistant.ingest.validators import validate_trades
from portfolio_assistant.ui.streamlit.helpers import ensure_initialized


ensure_initialized()

st.title("Import Trades")
st.caption("Upload a trade-history CSV, map columns if needed, and persist normalized rows.")

accounts = list_accounts()
if not accounts:
    st.warning("Create at least one account in the Accounts page before importing trades.")
    st.stop()

selected_account_id = st.selectbox(
    "Account",
    [account.account_id for account in accounts],
    format_func=lambda value: next(a for a in accounts if a.account_id == value).account_label,
)
account = get_account(selected_account_id)
if account is None:
    st.error("Selected account not found.")
    st.stop()

uploaded = st.file_uploader("Trade CSV", type=["csv"])
if not uploaded:
    st.info("Upload a CSV file to preview mapping and import.")
    st.stop()

upload_dir = data_dir() / "uploads"
upload_dir.mkdir(parents=True, exist_ok=True)
temp_path = upload_dir / f"{uuid4()}_{uploaded.name}"
Path(temp_path).write_bytes(uploaded.getvalue())

base_result = import_trades_csv(temp_path, account)
active_mapping = dict(base_result.mapping)

if base_result.unmapped_required:
    st.warning("Unknown CSV headers detected. Map required fields before import.")
    for field in base_result.unmapped_required:
        choice = st.selectbox(
            f"Map `{field}` to CSV column",
            [""] + base_result.headers,
            key=f"trade_map_{field}",
        )
        if choice:
            active_mapping[field] = choice

final_result = import_trades_csv(temp_path, account, mapping_override=active_mapping)
if final_result.unmapped_required:
    st.error("Mapping incomplete. Required fields still missing.")
    st.write(final_result.unmapped_required)
    st.stop()

issues = validate_trades(final_result.trades)
if issues:
    st.error("Validation issues found. Fix mapping or source CSV before importing.")
    st.dataframe(
        [{"row_index": idx, "issue": message} for idx, message in issues],
        use_container_width=True,
    )
    st.stop()

preview = [
    {
        "executed_at": trade.executed_at.isoformat(sep=" ", timespec="seconds"),
        "symbol": trade.symbol,
        "instrument_type": trade.instrument_type.value,
        "side": trade.side.value,
        "quantity": trade.quantity,
        "price": trade.price,
        "fees": trade.fees,
        "net_amount": trade.net_amount,
    }
    for trade in final_result.trades[:100]
]
st.write(f"Ready to import {len(final_result.trades)} normalized trades.")
st.dataframe(preview, use_container_width=True)

if st.button("Import Trades", type="primary"):
    inserted = save_trade_import(
        source_file=uploaded.name,
        account=account,
        signature=final_result.signature,
        mapping=final_result.mapping,
        raw_rows=final_result.raw_rows,
        trades=final_result.trades,
    )
    st.session_state["last_trade_import_unmapped"] = []
    st.session_state["last_trade_import_issues"] = []
    st.success(f"Imported {inserted} trades from {uploaded.name}.")
