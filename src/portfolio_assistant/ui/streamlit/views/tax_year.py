from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.reconciliation import validate_tax_report_summary
from portfolio_assistant.analytics.tax_year_report import generate_tax_year_report
from portfolio_assistant.ui.streamlit.views.common import (
    csv_download,
    export_filename,
    initialize_engine,
    load_accounts,
    money,
    render_global_account_scope,
    render_scope_caption,
)

SUMMARY_FIELDS = [
    "tax_year",
    "rows",
    "total_proceeds",
    "total_cost_basis",
    "total_gain_or_loss_raw",
    "total_gain_or_loss",
    "short_term_gain_or_loss",
    "long_term_gain_or_loss",
    "unknown_term_gain_or_loss",
    "total_wash_sale_disallowed_broker",
    "total_wash_sale_disallowed_irs",
    "wash_sale_mode_difference",
]


def tax_year_summary_dataframe(summary: dict[str, Any]) -> pd.DataFrame:
    rows = [{"metric": key, "value": summary.get(key)} for key in SUMMARY_FIELDS]
    return pd.DataFrame(rows)


def tax_year_detail_dataframe(report: dict[str, Any]) -> pd.DataFrame:
    detail_rows = report.get("detail_rows") or []
    if not detail_rows:
        return pd.DataFrame()
    frame = pd.DataFrame(detail_rows)
    if "date_sold" in frame.columns:
        frame = frame.sort_values(["date_sold", "symbol"], ascending=[False, True])
    return frame


def wash_sale_matches_dataframe(report: dict[str, Any]) -> pd.DataFrame:
    wash_sale_summary = report.get("wash_sale_summary") or {}
    rows: list[dict[str, Any]] = []
    for mode in ("broker", "irs"):
        mode_payload = wash_sale_summary.get(mode) or {}
        for sale in mode_payload.get("sales", []):
            sale_base = {
                "mode": mode,
                "sale_row_id": sale.get("sale_row_id"),
                "symbol": sale.get("symbol"),
                "sale_date": sale.get("sale_date"),
                "sale_quantity": sale.get("sale_quantity"),
                "sale_loss": sale.get("sale_loss"),
                "disallowed_loss": sale.get("disallowed_loss"),
            }
            matches = sale.get("matches") or []
            if not matches:
                rows.append(sale_base)
                continue
            for match in matches:
                row = dict(sale_base)
                row.update(
                    {
                        "buy_account_id": match.get("buy_account_id"),
                        "buy_account_type": match.get("buy_account_type"),
                        "buy_date": match.get("buy_date"),
                        "buy_instrument_type": match.get("buy_instrument_type"),
                        "buy_quantity": match.get("buy_quantity"),
                        "cross_account": bool(match.get("cross_account")),
                        "ira_replacement": bool(match.get("ira_replacement")),
                        "allocated_replacement_quantity_equiv": match.get(
                            "allocated_replacement_quantity_equiv"
                        ),
                    }
                )
                rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def render_page() -> None:
    st.set_page_config(page_title="Tax Year", layout="wide")
    st.header("Tax Year")
    st.caption("Selected-year realized gain/loss report with broker vs IRS wash-sale modes.")
    st.info("Educational analytics only, not tax advice.")

    engine = initialize_engine()
    accounts = load_accounts(engine)
    account_filter_id = render_global_account_scope(accounts)
    render_scope_caption(accounts, account_filter_id)

    current_year = date.today().year
    tax_year = st.number_input(
        "Tax year",
        min_value=2000,
        max_value=current_year + 1,
        value=current_year,
        step=1,
        key="tax_year_selected_year",
    )

    with Session(engine) as session:
        report = generate_tax_year_report(
            session,
            tax_year=int(tax_year),
            account_id=account_filter_id,
        )
    validation = validate_tax_report_summary(report)

    summary = report.get("summary") or {}
    detail_frame = tax_year_detail_dataframe(report)
    summary_frame = tax_year_summary_dataframe(summary)
    wash_frame = wash_sale_matches_dataframe(report)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rows", int(summary.get("rows", 0) or 0))
    c2.metric("Adjusted Gain/Loss", money(float(summary.get("total_gain_or_loss", 0.0) or 0.0)))
    c3.metric(
        "Raw Gain/Loss",
        money(float(summary.get("total_gain_or_loss_raw", 0.0) or 0.0)),
    )
    c4.metric(
        "Broker Wash",
        money(float(summary.get("total_wash_sale_disallowed_broker", 0.0) or 0.0)),
    )
    c5.metric(
        "IRS Wash",
        money(float(summary.get("total_wash_sale_disallowed_irs", 0.0) or 0.0)),
    )

    if validation.get("ok"):
        st.success("Tax-year summary validation checks passed.")
    else:
        st.warning("Tax-year summary validation checks failed. Review summary and detail rows.")

    tab_summary, tab_detail, tab_wash = st.tabs(["Summary", "8949-like Detail", "Wash Sale Matches"])

    with tab_summary:
        st.dataframe(summary_frame, use_container_width=True, hide_index=True)
        csv_download(
            summary_frame,
            label="Download summary CSV",
            filename=export_filename("tax_year_summary", accounts, account_filter_id),
            key="download_tax_year_summary_csv",
        )

    with tab_detail:
        if detail_frame.empty:
            st.info("No realized rows for the selected year and account scope.")
        else:
            st.dataframe(detail_frame, use_container_width=True, hide_index=True)
            csv_download(
                detail_frame,
                label="Download detail CSV",
                filename=export_filename("tax_year_detail", accounts, account_filter_id),
                key="download_tax_year_detail_csv",
            )

    with tab_wash:
        if wash_frame.empty:
            st.info("No wash-sale matches were identified for the selected scope.")
        else:
            st.dataframe(wash_frame, use_container_width=True, hide_index=True)
            csv_download(
                wash_frame,
                label="Download wash-sale matches CSV",
                filename=export_filename("tax_year_wash_matches", accounts, account_filter_id),
                key="download_tax_year_wash_csv",
            )

