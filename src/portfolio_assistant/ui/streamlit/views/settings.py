from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.db.models import Account, CashActivity, PnlRealized, PositionOpen, TradeNormalized
from portfolio_assistant.ui.streamlit.views.common import (
    account_label,
    csv_download,
    export_filename,
    initialize_engine,
    load_accounts,
    render_global_account_scope,
    render_scope_caption,
)


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _scalar_count(session: Session, stmt) -> int:
    return int(session.scalar(stmt) or 0)


def settings_metrics(session: Session, account_filter_id: str | None) -> dict[str, int]:
    trade_stmt = select(func.count()).select_from(TradeNormalized)
    realized_stmt = select(func.count()).select_from(PnlRealized)
    open_stmt = select(func.count()).select_from(PositionOpen)
    cash_stmt = select(func.count()).select_from(CashActivity)
    external_cash_stmt = select(func.count()).select_from(CashActivity).where(
        CashActivity.is_external.is_(True)
    )
    unclassified_cash_stmt = select(func.count()).select_from(CashActivity).where(
        CashActivity.is_external.is_(None)
    )

    if account_filter_id:
        trade_stmt = trade_stmt.where(TradeNormalized.account_id == account_filter_id)
        realized_stmt = realized_stmt.where(PnlRealized.account_id == account_filter_id)
        open_stmt = open_stmt.where(PositionOpen.account_id == account_filter_id)
        cash_stmt = cash_stmt.where(CashActivity.account_id == account_filter_id)
        external_cash_stmt = external_cash_stmt.where(CashActivity.account_id == account_filter_id)
        unclassified_cash_stmt = unclassified_cash_stmt.where(
            CashActivity.account_id == account_filter_id
        )

    return {
        "normalized_trades": _scalar_count(session, trade_stmt),
        "realized_rows": _scalar_count(session, realized_stmt),
        "open_positions": _scalar_count(session, open_stmt),
        "cash_rows": _scalar_count(session, cash_stmt),
        "external_cash_rows": _scalar_count(session, external_cash_stmt),
        "unclassified_cash_rows": _scalar_count(session, unclassified_cash_stmt),
    }


def settings_metrics_dataframe(metrics: dict[str, int]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"metric": metric, "value": value} for metric, value in metrics.items()]
    )


def account_catalog_dataframe(accounts: list[Account]) -> pd.DataFrame:
    if not accounts:
        return pd.DataFrame(columns=["id", "broker", "account_label", "account_type", "display"])

    rows = []
    for account in accounts:
        rows.append(
            {
                "id": account.id,
                "broker": account.broker,
                "account_label": account.account_label,
                "account_type": _enum_value(account.account_type),
                "display": account_label(account),
            }
        )
    return pd.DataFrame(rows).sort_values(["broker", "account_label"])


def render_page() -> None:
    st.set_page_config(page_title="Settings", layout="wide")
    st.header("Settings")
    st.caption("Data quality checks and account metadata for the selected account scope.")

    engine = initialize_engine()
    accounts = load_accounts(engine)
    account_filter_id = render_global_account_scope(accounts)
    render_scope_caption(accounts, account_filter_id)

    if st.button("Recompute analytics for scope", key="settings_recompute_scope"):
        with Session(engine) as session:
            stats = recompute_pnl(session, account_id=account_filter_id)
            session.commit()
        st.success(
            f"Recomputed {stats['realized_rows']} realized rows and {stats['open_rows']} open positions."
        )

    with Session(engine) as session:
        metrics = settings_metrics(session, account_filter_id)

    metrics_frame = settings_metrics_dataframe(metrics)
    accounts_frame = account_catalog_dataframe(accounts)

    c1, c2, c3 = st.columns(3)
    c1.metric("Normalized Trades", metrics["normalized_trades"])
    c2.metric("Realized Rows", metrics["realized_rows"])
    c3.metric("Open Positions", metrics["open_positions"])

    tab_quality, tab_accounts = st.tabs(["Data Quality", "Accounts"])

    with tab_quality:
        st.dataframe(metrics_frame, use_container_width=True, hide_index=True)
        csv_download(
            metrics_frame,
            label="Download data quality CSV",
            filename=export_filename("settings_data_quality", accounts, account_filter_id),
            key="download_settings_quality_csv",
        )

    with tab_accounts:
        st.dataframe(accounts_frame, use_container_width=True, hide_index=True)
        csv_download(
            accounts_frame,
            label="Download accounts CSV",
            filename=export_filename("settings_accounts", accounts, account_filter_id),
            key="download_settings_accounts_csv",
        )

