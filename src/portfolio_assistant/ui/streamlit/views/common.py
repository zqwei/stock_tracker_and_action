from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from portfolio_assistant.assistant.tools_db import get_engine, list_accounts
from portfolio_assistant.config.paths import ensure_data_dirs
from portfolio_assistant.db.migrate import migrate
from portfolio_assistant.db.models import Account
from portfolio_assistant.utils.money import format_money

ACCOUNT_SCOPE_SESSION_KEY = "global_account_filter_id"


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def money(value: float) -> str:
    return format_money(value, signed=True)


def account_label(account: Account) -> str:
    return f"{account.broker} | {account.account_label} | {_enum_value(account.account_type)}"


def account_lookup(accounts: list[Account]) -> dict[str, Account]:
    return {account.id: account for account in accounts}


def _sanitize_token(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in value.lower())
    cleaned = cleaned.strip("_")
    return cleaned or "account"


def scope_token(accounts: list[Account], account_filter_id: str | None) -> str:
    if account_filter_id is None:
        return "all_accounts"
    account = account_lookup(accounts).get(account_filter_id)
    if account is None:
        return "account_scope"
    return _sanitize_token(
        f"{account.broker}_{account.account_label}_{_enum_value(account.account_type)}"
    )


@st.cache_resource
def initialize_engine() -> Engine:
    ensure_data_dirs()
    migrate()
    return get_engine()


def load_accounts(engine: Engine) -> list[Account]:
    with Session(engine) as session:
        return list_accounts(session)


def render_global_account_scope(
    accounts: list[Account],
    *,
    sidebar_title: str = "Portfolio Assistant",
) -> str | None:
    options: list[str | None] = [None] + [account.id for account in accounts]
    account_by_id = account_lookup(accounts)

    current = st.session_state.get(ACCOUNT_SCOPE_SESSION_KEY)
    if current not in set(options):
        st.session_state[ACCOUNT_SCOPE_SESSION_KEY] = None

    with st.sidebar:
        st.title(sidebar_title)
        selected = st.selectbox(
            "Global account scope",
            options=options,
            format_func=lambda account_id: (
                "All accounts (consolidated)"
                if account_id is None
                else account_label(account_by_id[account_id])
            ),
            key=ACCOUNT_SCOPE_SESSION_KEY,
        )
        st.caption("This scope is shared across report pages.")
    return selected


def render_scope_caption(accounts: list[Account], account_filter_id: str | None) -> None:
    if account_filter_id is None:
        st.caption("Account scope: all accounts (consolidated).")
        return
    account = account_lookup(accounts).get(account_filter_id)
    if account is None:
        st.caption("Account scope: selected account not found.")
        return
    st.caption(f"Account scope: {account_label(account)}.")


def dataframe_to_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def csv_download(
    frame: pd.DataFrame,
    *,
    label: str,
    filename: str,
    key: str,
) -> None:
    st.download_button(
        label=label,
        data=dataframe_to_csv_bytes(frame),
        file_name=filename,
        mime="text/csv",
        key=key,
        use_container_width=True,
    )


def export_filename(prefix: str, accounts: list[Account], account_filter_id: str | None) -> str:
    scope = scope_token(accounts, account_filter_id)
    return f"{_sanitize_token(prefix)}_{scope}.csv"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[5]
