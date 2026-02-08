from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import streamlit as st

from portfolio_assistant.config.paths import data_dir
from portfolio_assistant.db.repository import get_account, list_accounts, save_cash_activity
from portfolio_assistant.ingest.cash_import import import_cash_csv
from portfolio_assistant.ui.streamlit.helpers import ensure_initialized


ensure_initialized()

st.title("Import Cash")
st.caption("Upload cash-activity CSV and tag internal transfers vs external contributions.")

accounts = list_accounts()
if not accounts:
    st.warning("Create at least one account in the Accounts page before importing cash activity.")
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

uploaded = st.file_uploader("Cash Activity CSV", type=["csv"])
if not uploaded:
    st.info("Upload a CSV file to preview mapping and tagging.")
    st.stop()

upload_dir = data_dir() / "uploads"
upload_dir.mkdir(parents=True, exist_ok=True)
temp_path = upload_dir / f"{uuid4()}_{uploaded.name}"
Path(temp_path).write_bytes(uploaded.getvalue())

base_result = import_cash_csv(temp_path, account)
active_mapping = dict(base_result.mapping)

if base_result.unmapped_required:
    st.warning("Unknown CSV headers detected. Map required fields before import.")
    for field in base_result.unmapped_required:
        choice = st.selectbox(
            f"Map `{field}` to CSV column",
            [""] + base_result.headers,
            key=f"cash_map_{field}",
        )
        if choice:
            active_mapping[field] = choice

final_result = import_cash_csv(temp_path, account, mapping_override=active_mapping)
if final_result.unmapped_required:
    st.error("Mapping incomplete. Required fields still missing.")
    st.write(final_result.unmapped_required)
    st.stop()

index_options = list(range(len(final_result.activities)))
default_internal = [idx for idx, row in enumerate(final_result.activities) if not row.is_external]
internal_indices = st.multiselect(
    "Mark rows as internal transfers (excluded from net contributions)",
    options=index_options,
    default=default_internal,
    format_func=lambda idx: (
        f"#{idx} {final_result.activities[idx].posted_at.date()} "
        f"{final_result.activities[idx].type} ${final_result.activities[idx].amount:.2f} "
        f"{final_result.activities[idx].description[:40]}"
    ),
)

for idx, item in enumerate(final_result.activities):
    item.is_external = idx not in internal_indices

preview = [
    {
        "posted_at": row.posted_at.isoformat(sep=" ", timespec="seconds"),
        "type": row.type,
        "amount": row.amount,
        "description": row.description,
        "source": row.source,
        "is_external": row.is_external,
    }
    for row in final_result.activities[:150]
]
st.write(f"Ready to import {len(final_result.activities)} cash activity rows.")
st.dataframe(preview, use_container_width=True)

if st.button("Import Cash Activity", type="primary"):
    inserted = save_cash_activity(final_result.activities)
    st.session_state["last_cash_import_unmapped"] = []
    st.success(f"Imported {inserted} cash activity rows from {uploaded.name}.")
