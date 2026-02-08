"""Shared Streamlit helpers for account scope and computed portfolio state."""

from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from portfolio_assistant.analytics.contributions import ContributionSummary, compute_contributions
from portfolio_assistant.analytics.pnl_engine import PnLComputation, compute_realized_and_open_positions
from portfolio_assistant.analytics.wash_sale import apply_wash_sale_flags, detect_wash_sale_risks
from portfolio_assistant.config.settings import get_settings
from portfolio_assistant.db.migrate import run_migrations
from portfolio_assistant.db.models import Account, RealizedPnLRow, WashSaleRiskRow
from portfolio_assistant.db.repository import (
    list_accounts,
    list_cash_activity,
    list_trades,
    replace_derived_pnl,
)


ALL_ACCOUNTS_VALUE = "__ALL__"


@dataclass(slots=True)
class PortfolioSnapshot:
    account_scope: str | None
    accounts: list[Account]
    pnl: PnLComputation
    contributions: ContributionSummary
    wash_sale_risks: list[WashSaleRiskRow]

    @property
    def realized_rows(self) -> list[RealizedPnLRow]:
        return self.pnl.realized


def ensure_initialized() -> None:
    run_migrations(get_settings().db_path)


def render_account_scope_selector(sidebar_label: str = "Account") -> str | None:
    accounts = list_accounts()
    options = [ALL_ACCOUNTS_VALUE] + [account.account_id for account in accounts]
    labels = {ALL_ACCOUNTS_VALUE: "All accounts (consolidated)"}
    labels.update({account.account_id: f"{account.account_label} ({account.account_type.value})" for account in accounts})

    selected = st.sidebar.selectbox(
        sidebar_label,
        options,
        format_func=lambda item: labels[item],
        key="global_account_scope",
    )
    if selected == ALL_ACCOUNTS_VALUE:
        return None
    return selected


def build_snapshot(account_scope: str | None) -> PortfolioSnapshot:
    accounts = list_accounts()
    trades = list_trades(account_scope)
    cash_rows = list_cash_activity(account_scope)

    pnl = compute_realized_and_open_positions(trades, latest_quotes={})
    risks = detect_wash_sale_risks(pnl.realized, trades)
    apply_wash_sale_flags(pnl.realized, risks)

    # Keep derived snapshot tables up to date for report pages.
    if account_scope is None:
        replace_derived_pnl(pnl.realized, pnl.open_positions)

    contributions = compute_contributions(cash_rows)
    return PortfolioSnapshot(
        account_scope=account_scope,
        accounts=accounts,
        pnl=pnl,
        contributions=contributions,
        wash_sale_risks=risks,
    )


def account_name_map(accounts: list[Account]) -> dict[str, str]:
    return {account.account_id: account.account_label for account in accounts}
