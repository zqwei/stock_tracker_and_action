from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.reconciliation import realized_by_symbol
from portfolio_assistant.db.models import Account, PnlRealized
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


PnlDetailColumns = [
    "account_id",
    "account",
    "symbol",
    "instrument_type",
    "close_date",
    "quantity",
    "proceeds",
    "cost_basis",
    "fees",
    "pnl",
    "notes",
]


def _to_iso(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def realized_detail_dataframe(session: Session, account_filter_id: str | None) -> pd.DataFrame:
    stmt = select(PnlRealized, Account).join(Account, Account.id == PnlRealized.account_id)
    if account_filter_id:
        stmt = stmt.where(PnlRealized.account_id == account_filter_id)
    stmt = stmt.order_by(
        PnlRealized.close_date.desc(),
        PnlRealized.symbol.asc(),
        PnlRealized.id.asc(),
    )

    rows: list[dict[str, Any]] = []
    for realized_row, account in session.execute(stmt).all():
        rows.append(
            {
                "account_id": realized_row.account_id,
                "account": account_label(account),
                "symbol": realized_row.symbol,
                "instrument_type": _enum_value(realized_row.instrument_type),
                "close_date": _to_iso(realized_row.close_date),
                "quantity": float(realized_row.quantity),
                "proceeds": float(realized_row.proceeds),
                "cost_basis": float(realized_row.cost_basis),
                "fees": float(realized_row.fees),
                "pnl": float(realized_row.pnl),
                "notes": realized_row.notes or "",
            }
        )
    if not rows:
        return pd.DataFrame(columns=PnlDetailColumns)
    return pd.DataFrame(rows)


def realized_summary_dataframe(session: Session, account_filter_id: str | None) -> pd.DataFrame:
    rows = realized_by_symbol(session, account_id=account_filter_id)
    if not rows:
        return pd.DataFrame(columns=["symbol", "instrument_type", "realized_pnl"])
    frame = pd.DataFrame(rows)
    frame["realized_pnl"] = pd.to_numeric(frame["realized_pnl"], errors="coerce").fillna(0.0)
    frame["abs_realized_pnl"] = frame["realized_pnl"].abs()
    frame = frame.sort_values(
        by=["abs_realized_pnl", "symbol"], ascending=[False, True]
    ).drop(columns=["abs_realized_pnl"])
    return frame


def pnl_summary(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {
            "total_realized": 0.0,
            "rows": 0.0,
            "winners": 0.0,
            "losers": 0.0,
        }
    pnl_series = pd.to_numeric(frame["pnl"], errors="coerce").fillna(0.0)
    return {
        "total_realized": float(pnl_series.sum()),
        "rows": float(len(frame)),
        "winners": float((pnl_series > 0).sum()),
        "losers": float((pnl_series < 0).sum()),
    }


def _instrument_slice(frame: pd.DataFrame, instrument: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return frame.loc[frame["instrument_type"] == instrument].copy()


def render_page() -> None:
    st.set_page_config(page_title="P&L", layout="wide")
    st.header("P&L")
    st.caption("Realized P&L tables for the selected global account scope.")

    engine = initialize_engine()
    accounts = load_accounts(engine)
    account_filter_id = render_global_account_scope(accounts)
    render_scope_caption(accounts, account_filter_id)

    with Session(engine) as session:
        detail_frame = realized_detail_dataframe(session, account_filter_id)
        summary_frame = realized_summary_dataframe(session, account_filter_id)

    totals = pnl_summary(detail_frame)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Realized P&L", money(totals["total_realized"]))
    c2.metric("Realized Rows", int(totals["rows"]))
    c3.metric("Winning Rows", int(totals["winners"]))
    c4.metric("Losing Rows", int(totals["losers"]))

    if summary_frame.empty:
        st.info("No realized P&L rows found for the selected scope.")
        return

    stock_frame = _instrument_slice(summary_frame, "STOCK")
    option_frame = _instrument_slice(summary_frame, "OPTION")
    tab_stock, tab_option, tab_detail = st.tabs(
        ["Stocks", "Options", "Realized Lots"]
    )

    with tab_stock:
        if stock_frame.empty:
            st.info("No stock realized rows.")
        else:
            st.dataframe(stock_frame, use_container_width=True, hide_index=True)
            csv_download(
                stock_frame,
                label="Download stock P&L CSV",
                filename=export_filename("pnl_stocks", accounts, account_filter_id),
                key="download_pnl_stock_csv",
            )

    with tab_option:
        if option_frame.empty:
            st.info("No option realized rows.")
        else:
            st.dataframe(option_frame, use_container_width=True, hide_index=True)
            csv_download(
                option_frame,
                label="Download option P&L CSV",
                filename=export_filename("pnl_options", accounts, account_filter_id),
                key="download_pnl_option_csv",
            )

    with tab_detail:
        st.dataframe(detail_frame, use_container_width=True, hide_index=True)
        csv_download(
            detail_frame,
            label="Download realized lots CSV",
            filename=export_filename("pnl_realized_rows", accounts, account_filter_id),
            key="download_pnl_detail_csv",
        )

