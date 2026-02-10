from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import Account, PositionOpen
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


HOLDINGS_COLUMNS = [
    "account_id",
    "account",
    "instrument_type",
    "symbol",
    "option_symbol_raw",
    "quantity",
    "avg_cost",
    "last_price",
    "market_value",
    "unrealized_pnl",
    "as_of",
]


def holdings_dataframe(session: Session, account_filter_id: str | None) -> pd.DataFrame:
    stmt = select(PositionOpen, Account).join(Account, Account.id == PositionOpen.account_id)
    if account_filter_id:
        stmt = stmt.where(PositionOpen.account_id == account_filter_id)
    stmt = stmt.order_by(
        PositionOpen.account_id.asc(),
        PositionOpen.instrument_type.asc(),
        PositionOpen.symbol.asc(),
        PositionOpen.option_symbol_raw.asc(),
        PositionOpen.id.asc(),
    )

    rows: list[dict[str, Any]] = []
    for position, account in session.execute(stmt).all():
        rows.append(
            {
                "account_id": position.account_id,
                "account": account_label(account),
                "instrument_type": _enum_value(position.instrument_type),
                "symbol": position.symbol,
                "option_symbol_raw": position.option_symbol_raw,
                "quantity": float(position.quantity),
                "avg_cost": float(position.avg_cost),
                "last_price": (
                    float(position.last_price)
                    if position.last_price is not None
                    else None
                ),
                "market_value": (
                    float(position.market_value)
                    if position.market_value is not None
                    else None
                ),
                "unrealized_pnl": (
                    float(position.unrealized_pnl)
                    if position.unrealized_pnl is not None
                    else None
                ),
                "as_of": position.as_of.isoformat() if position.as_of else None,
            }
        )

    if not rows:
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)

    frame = pd.DataFrame(rows)
    frame["market_value"] = pd.to_numeric(frame["market_value"], errors="coerce")
    frame["unrealized_pnl"] = pd.to_numeric(frame["unrealized_pnl"], errors="coerce")
    frame["abs_unrealized_pnl"] = frame["unrealized_pnl"].abs()
    frame = frame.sort_values(
        by=["abs_unrealized_pnl", "market_value"],
        ascending=[False, False],
    )
    frame = frame.drop(columns=["abs_unrealized_pnl"])
    return frame


def holdings_summary(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {
            "positions": 0.0,
            "market_value": 0.0,
            "unrealized_pnl": 0.0,
            "stock_positions": 0.0,
            "option_positions": 0.0,
        }
    return {
        "positions": float(len(frame)),
        "market_value": float(pd.to_numeric(frame["market_value"], errors="coerce").fillna(0.0).sum()),
        "unrealized_pnl": float(
            pd.to_numeric(frame["unrealized_pnl"], errors="coerce").fillna(0.0).sum()
        ),
        "stock_positions": float((frame["instrument_type"] == "STOCK").sum()),
        "option_positions": float((frame["instrument_type"] == "OPTION").sum()),
    }


def render_page() -> None:
    st.set_page_config(page_title="Holdings", layout="wide")
    st.header("Holdings")
    st.caption("Open positions for the selected global account scope.")

    engine = initialize_engine()
    accounts = load_accounts(engine)
    account_filter_id = render_global_account_scope(accounts)
    render_scope_caption(accounts, account_filter_id)

    with Session(engine) as session:
        frame = holdings_dataframe(session, account_filter_id)

    summary = holdings_summary(frame)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Open Positions", int(summary["positions"]))
    c2.metric("Market Value", money(summary["market_value"]))
    c3.metric("Unrealized P&L", money(summary["unrealized_pnl"]))
    c4.metric("Stock Positions", int(summary["stock_positions"]))
    c5.metric("Option Positions", int(summary["option_positions"]))

    if frame.empty:
        st.info("No open positions found for the selected scope.")
        return

    st.dataframe(frame, use_container_width=True, hide_index=True)
    csv_download(
        frame,
        label="Download holdings CSV",
        filename=export_filename("holdings", accounts, account_filter_id),
        key="download_holdings_csv",
    )

