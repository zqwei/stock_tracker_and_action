from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.reconciliation import contributions_by_month, net_contributions
from portfolio_assistant.db.models import Account, CashActivity
from portfolio_assistant.ui.streamlit.views.common import (
    account_label,
    csv_download,
    export_filename,
    initialize_engine,
    load_accounts,
    money,
    render_global_account_scope,
    render_scope_caption,
)


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _to_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def monthly_contributions_dataframe(session: Session, account_filter_id: str | None) -> pd.DataFrame:
    rows = contributions_by_month(session, account_id=account_filter_id)
    if not rows:
        return pd.DataFrame(columns=["month", "net_contribution"])
    frame = pd.DataFrame(rows)
    frame["net_contribution"] = pd.to_numeric(
        frame["net_contribution"], errors="coerce"
    ).fillna(0.0)
    return frame.sort_values("month")


def external_cash_activity_dataframe(session: Session, account_filter_id: str | None) -> pd.DataFrame:
    stmt = select(CashActivity, Account).join(Account, Account.id == CashActivity.account_id).where(
        CashActivity.is_external.is_(True)
    )
    if account_filter_id:
        stmt = stmt.where(CashActivity.account_id == account_filter_id)
    stmt = stmt.order_by(CashActivity.posted_at.desc(), CashActivity.id.desc())

    rows: list[dict[str, Any]] = []
    for activity, account in session.execute(stmt).all():
        amount = float(activity.amount)
        direction = _enum_value(activity.activity_type)
        signed_amount = amount if direction == "DEPOSIT" else -amount
        rows.append(
            {
                "account_id": activity.account_id,
                "account": account_label(account),
                "posted_at": _to_iso(activity.posted_at),
                "activity_type": direction,
                "amount": amount,
                "signed_amount": signed_amount,
                "description": activity.description or "",
                "source": activity.source or "",
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "account_id",
                "account",
                "posted_at",
                "activity_type",
                "amount",
                "signed_amount",
                "description",
                "source",
            ]
        )
    return pd.DataFrame(rows)


def account_contributions_dataframe(activity_frame: pd.DataFrame) -> pd.DataFrame:
    if activity_frame.empty:
        return pd.DataFrame(columns=["account_id", "account", "net_contribution"])
    grouped = (
        activity_frame.groupby(["account_id", "account"], as_index=False)["signed_amount"]
        .sum()
        .rename(columns={"signed_amount": "net_contribution"})
        .sort_values("net_contribution", ascending=False)
    )
    return grouped


def render_page() -> None:
    st.set_page_config(page_title="Contributions", layout="wide")
    st.header("Contributions")
    st.caption("External deposits and withdrawals for the selected global account scope.")

    engine = initialize_engine()
    accounts = load_accounts(engine)
    account_filter_id = render_global_account_scope(accounts)
    render_scope_caption(accounts, account_filter_id)

    with Session(engine) as session:
        net_total = net_contributions(session, account_id=account_filter_id)
        monthly_frame = monthly_contributions_dataframe(session, account_filter_id)
        activity_frame = external_cash_activity_dataframe(session, account_filter_id)

    account_frame = account_contributions_dataframe(activity_frame)

    c1, c2, c3 = st.columns(3)
    c1.metric("Net Contributions", money(float(net_total)))
    c2.metric("Monthly Buckets", len(monthly_frame))
    c3.metric("External Cash Rows", len(activity_frame))

    tab_monthly, tab_accounts, tab_activity = st.tabs(
        ["Monthly", "By Account", "External Cash Activity"]
    )

    with tab_monthly:
        if monthly_frame.empty:
            st.info("No monthly contribution rows for the selected scope.")
        else:
            st.dataframe(monthly_frame, use_container_width=True, hide_index=True)
            csv_download(
                monthly_frame,
                label="Download monthly contributions CSV",
                filename=export_filename("contributions_monthly", accounts, account_filter_id),
                key="download_contrib_monthly_csv",
            )

    with tab_accounts:
        if account_frame.empty:
            st.info("No account contribution rows for the selected scope.")
        else:
            st.dataframe(account_frame, use_container_width=True, hide_index=True)
            csv_download(
                account_frame,
                label="Download account contribution CSV",
                filename=export_filename("contributions_by_account", accounts, account_filter_id),
                key="download_contrib_account_csv",
            )

    with tab_activity:
        if activity_frame.empty:
            st.info("No external cash activity rows for the selected scope.")
        else:
            st.dataframe(activity_frame, use_container_width=True, hide_index=True)
            csv_download(
                activity_frame,
                label="Download external cash activity CSV",
                filename=export_filename("contributions_activity", accounts, account_filter_id),
                key="download_contrib_activity_csv",
            )

