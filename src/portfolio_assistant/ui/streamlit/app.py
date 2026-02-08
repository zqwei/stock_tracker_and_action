"""Streamlit entrypoint for Portfolio Assistant."""

from __future__ import annotations

import streamlit as st

from portfolio_assistant.config.settings import get_settings
from portfolio_assistant.db.migrate import run_migrations


def main() -> None:
    settings = get_settings()
    run_migrations(settings.db_path)

    st.set_page_config(page_title="Portfolio Assistant", layout="wide")
    st.title("Portfolio Assistant")
    st.caption("Phase 1: imports, core P&L, contributions, calendar, and wash-sale risk.")

    st.sidebar.title("Navigation")
    st.sidebar.markdown("Open pages from the list below.")

    st.subheader("Phase 1 Workflow")
    st.write("1) Create accounts")
    st.write("2) Import trade CSV files")
    st.write("3) Import cash activity CSV files")
    st.write("4) Review Overview, Calendar, and Wash Sale Risk pages")

    st.markdown("### Required Starter Pages")
    st.write("Accounts, Import Trades, Import Cash, Overview, Data Quality")

    st.markdown("### Local Data")
    st.code(str(settings.db_path))


if __name__ == "__main__":
    main()
