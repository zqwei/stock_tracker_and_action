from __future__ import annotations

import streamlit as st

from portfolio_assistant.ui.streamlit.app import _render_calendar
from portfolio_assistant.ui.streamlit.views.common import (
    initialize_engine,
    load_accounts,
    render_global_account_scope,
    render_scope_caption,
)


def render_page() -> None:
    st.set_page_config(page_title="Calendar", layout="wide")

    engine = initialize_engine()
    accounts = load_accounts(engine)
    account_filter_id = render_global_account_scope(accounts)
    render_scope_caption(accounts, account_filter_id)
    _render_calendar(engine, account_filter_id)
