from __future__ import annotations

from datetime import date
import io
from typing import Any
import zipfile

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.reconciliation import (
    compare_totals,
    tax_report_totals,
)
from portfolio_assistant.analytics.tax_year_report import generate_tax_year_report
from portfolio_assistant.ui.streamlit.views.common import (
    apply_page_theme,
    csv_download,
    export_filename,
    initialize_engine,
    load_accounts,
    money,
    render_global_account_scope,
    render_scope_caption,
)
from portfolio_assistant.ui.streamlit.views.tax_year import (
    tax_year_detail_dataframe,
    tax_year_summary_dataframe,
)

NUMERIC_FIELDS = [
    "total_proceeds",
    "total_cost_basis",
    "total_gain_or_loss",
    "short_term_gain_or_loss",
    "long_term_gain_or_loss",
    "total_wash_sale_disallowed",
]

BROKER_CANONICAL_FIELDS = {
    "symbol": "Symbol",
    "date_sold": "Sale/Close Date",
    "term": "Term (SHORT/LONG)",
    "proceeds": "Proceeds",
    "cost_basis": "Cost Basis",
    "gain_or_loss": "Gain/Loss",
    "wash_sale_disallowed": "Wash Sale Disallowed",
}

BROKER_FIELD_HELP = {
    "symbol": "Ticker/underlying symbol for the sold lot.",
    "date_sold": "Sale or close date for the realized row.",
    "term": "Holding term if available (SHORT/LONG). Optional.",
    "proceeds": "Sales proceeds. Optional when gain/loss is already provided.",
    "cost_basis": "Cost basis. Optional when gain/loss is already provided.",
    "gain_or_loss": "Realized gain/loss for the row. Used directly in reconciliation totals.",
    "wash_sale_disallowed": "Disallowed wash-sale amount if provided by broker.",
}


def _default_broker_totals() -> dict[str, float]:
    return {key: 0.0 for key in NUMERIC_FIELDS}


def _normalized_column_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def infer_broker_mapping(columns: list[str]) -> dict[str, str | None]:
    normalized_lookup = {_normalized_column_name(column): column for column in columns}
    patterns = {
        "symbol": ["symbol", "ticker", "description"],
        "date_sold": ["saledate", "closedate", "tradedate", "datesold"],
        "term": ["term", "stlt", "holdingperiod"],
        "proceeds": ["proceeds", "salesproceeds"],
        "cost_basis": ["costbasis", "basis"],
        "gain_or_loss": ["gainloss", "realizedgainloss", "pnl"],
        "wash_sale_disallowed": ["washsale", "disallowed", "adjustmentamount"],
    }

    mapping: dict[str, str | None] = {}
    for field, candidates in patterns.items():
        selected: str | None = None
        for candidate in candidates:
            for normalized, original in normalized_lookup.items():
                if candidate in normalized:
                    selected = original
                    break
            if selected is not None:
                break
        mapping[field] = selected
    return mapping


def _coerce_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def normalize_broker_dataframe(
    raw_frame: pd.DataFrame,
    mapping: dict[str, str | None],
) -> pd.DataFrame:
    if raw_frame.empty:
        return pd.DataFrame(columns=list(BROKER_CANONICAL_FIELDS))

    normalized = pd.DataFrame(index=raw_frame.index)
    for field in BROKER_CANONICAL_FIELDS:
        source_col = mapping.get(field)
        if source_col and source_col in raw_frame.columns:
            normalized[field] = raw_frame[source_col]
        else:
            normalized[field] = None

    normalized["symbol"] = normalized["symbol"].fillna("").astype(str).str.strip().str.upper()
    normalized["date_sold"] = normalized["date_sold"].fillna("").astype(str).str.strip()
    normalized["term"] = normalized["term"].fillna("UNKNOWN").astype(str).str.strip().str.upper()
    for key in ("proceeds", "cost_basis", "gain_or_loss", "wash_sale_disallowed"):
        normalized[key] = _coerce_numeric_series(normalized[key])

    has_values = (
        normalized["symbol"].ne("")
        | normalized["date_sold"].ne("")
        | normalized["proceeds"].ne(0.0)
        | normalized["cost_basis"].ne(0.0)
        | normalized["gain_or_loss"].ne(0.0)
        | normalized["wash_sale_disallowed"].ne(0.0)
    )
    normalized = normalized.loc[has_values].reset_index(drop=True)
    return normalized


def comparison_dataframe(comparison: dict[str, dict[str, float]]) -> pd.DataFrame:
    rows = []
    for metric, values in comparison.items():
        rows.append(
            {
                "metric": metric,
                "app": float(values.get("app", 0.0) or 0.0),
                "broker": float(values.get("broker", 0.0) or 0.0),
                "delta": float(values.get("delta", 0.0) or 0.0),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["abs_delta"] = frame["delta"].abs()
    frame = frame.sort_values("abs_delta", ascending=False).drop(columns=["abs_delta"])
    return frame


def reconciliation_health(
    comparison_frame: pd.DataFrame, *, tolerance: float = 1e-6
) -> dict[str, float | int | bool]:
    if comparison_frame.empty:
        return {
            "in_sync": True,
            "max_abs_delta": 0.0,
            "mismatch_metrics": 0,
        }

    delta_series = pd.to_numeric(comparison_frame["delta"], errors="coerce").fillna(0.0)
    max_abs_delta = float(delta_series.abs().max())
    mismatch_metrics = int((delta_series.abs() > tolerance).sum())
    return {
        "in_sync": mismatch_metrics == 0,
        "max_abs_delta": max_abs_delta,
        "mismatch_metrics": mismatch_metrics,
    }


def build_reconciliation_packet_zip(
    *,
    app_summary_frame: pd.DataFrame,
    app_detail_frame: pd.DataFrame,
    comparison_frame: pd.DataFrame,
    checklist_frame: pd.DataFrame,
    broker_detail_frame: pd.DataFrame,
    symbol_diff: pd.DataFrame,
    date_diff: pd.DataFrame,
    term_diff: pd.DataFrame,
) -> bytes:
    file_map = {
        "app_summary.csv": app_summary_frame,
        "app_8949_like_detail.csv": app_detail_frame,
        "totals_comparison.csv": comparison_frame,
        "broker_rows_normalized.csv": broker_detail_frame,
        "diff_by_symbol.csv": symbol_diff,
        "diff_by_sale_date.csv": date_diff,
        "diff_by_term.csv": term_diff,
        "reconciliation_checklist.csv": checklist_frame,
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for filename, frame in file_map.items():
            archive.writestr(filename, frame.to_csv(index=False))
    return buffer.getvalue()


def _safe_groupby_sum(frame: pd.DataFrame, key_column: str) -> pd.Series:
    if frame.empty or key_column not in frame.columns:
        return pd.Series(dtype=float)
    grouped = frame.groupby(key_column, dropna=False)["gain_or_loss"].sum()
    grouped.index = grouped.index.fillna("UNKNOWN")
    return grouped


def diff_table_by_key(
    app_detail_frame: pd.DataFrame,
    broker_detail_frame: pd.DataFrame,
    key_column: str,
) -> pd.DataFrame:
    app_group = _safe_groupby_sum(app_detail_frame, key_column).rename("app_gain_or_loss")
    broker_group = _safe_groupby_sum(broker_detail_frame, key_column).rename(
        "broker_gain_or_loss"
    )
    combined = app_group.to_frame().join(broker_group.to_frame(), how="outer").fillna(0.0)
    if combined.empty:
        return pd.DataFrame(columns=[key_column, "app_gain_or_loss", "broker_gain_or_loss", "delta"])
    combined["delta"] = combined["app_gain_or_loss"] - combined["broker_gain_or_loss"]
    combined["abs_delta"] = combined["delta"].abs()
    combined = combined.sort_values("abs_delta", ascending=False).drop(columns=["abs_delta"])
    combined = combined.reset_index().rename(columns={"index": key_column})
    return combined


def checklist_dataframe(
    report: dict[str, Any],
    comparison: dict[str, dict[str, float]],
    app_detail_frame: pd.DataFrame,
) -> pd.DataFrame:
    gain_delta = abs(float(comparison.get("total_gain_or_loss", {}).get("delta", 0.0)))
    wash_delta = abs(
        float(comparison.get("total_wash_sale_disallowed", {}).get("delta", 0.0))
    )

    date_series = pd.to_datetime(
        app_detail_frame.get("date_sold", pd.Series(dtype=str)),
        errors="coerce",
    )
    boundary_sales_present = bool(date_series.dt.month.isin([1, 12]).any())

    irs_sales = (report.get("wash_sale_summary") or {}).get("irs", {}).get("sales", [])
    cross_account_replacements = any(
        any(bool(match.get("cross_account")) for match in sale.get("matches", []))
        for sale in irs_sales
    )
    options_replacements = any(
        any(str(match.get("buy_instrument_type", "")).upper() == "OPTION" for match in sale.get("matches", []))
        for sale in irs_sales
    )

    symbols = app_detail_frame.get("symbol", pd.Series(dtype=str)).fillna("").astype(str)
    corporate_action_hint = bool(symbols.str.contains(r"[./-]").any())
    lot_method_hint = gain_delta > 1e-6 and wash_delta <= 1e-6
    missing_boundary_hint = gain_delta > 1e-6 and boundary_sales_present

    rows = [
        {
            "check": "Missing boundary data",
            "flagged": missing_boundary_hint,
            "details": "Year-end sales exist and total gain/loss differs from broker totals.",
        },
        {
            "check": "Cross-account replacements likely",
            "flagged": cross_account_replacements,
            "details": "IRS-mode wash-sale matches include replacement buys in another account.",
        },
        {
            "check": "Options replacements likely",
            "flagged": options_replacements,
            "details": "IRS-mode wash-sale matches include option replacement acquisitions.",
        },
        {
            "check": "Lot method mismatch likely",
            "flagged": lot_method_hint,
            "details": "Gain/loss differs while wash-sale delta is negligible.",
        },
        {
            "check": "Corporate actions present",
            "flagged": corporate_action_hint,
            "details": "Symbols suggest splits/renames might need broker-specific handling.",
        },
    ]
    return pd.DataFrame(rows)


def render_page() -> None:
    st.set_page_config(page_title="Reconciliation", layout="wide")
    apply_page_theme()
    st.header("Reconciliation")
    st.caption("Compare app tax-year totals against broker totals, then drill into differences.")
    st.info(
        "Recommended flow: verify app totals -> load broker totals -> inspect symbol/date/term "
        "deltas -> review mismatch checklist -> export a reconciliation packet."
    )

    engine = initialize_engine()
    accounts = load_accounts(engine)
    account_filter_id = render_global_account_scope(accounts)
    render_scope_caption(accounts, account_filter_id)

    current_year = date.today().year
    tax_year = int(
        st.number_input(
            "Tax year",
            min_value=2000,
            max_value=current_year + 1,
            value=current_year,
            step=1,
            key="recon_tax_year",
        )
    )

    with Session(engine) as session:
        report = generate_tax_year_report(
            session,
            tax_year=tax_year,
            account_id=account_filter_id,
        )
    app_detail_frame = tax_year_detail_dataframe(report)
    app_summary_frame = tax_year_summary_dataframe(report.get("summary") or {})
    app_totals = tax_report_totals(report.get("detail_rows") or [])

    broker_input_mode = st.radio(
        "Broker totals source",
        options=["Manual totals", "Upload broker CSV"],
        horizontal=True,
        key="recon_broker_input_mode",
    )

    broker_detail_frame = pd.DataFrame(columns=list(BROKER_CANONICAL_FIELDS))
    broker_totals = _default_broker_totals()

    if broker_input_mode == "Manual totals":
        cols = st.columns(3)
        manual_totals = {}
        for idx, field in enumerate(NUMERIC_FIELDS):
            manual_totals[field] = cols[idx % 3].number_input(
                field.replace("_", " ").title(),
                value=0.0,
                step=100.0,
                format="%.2f",
                key=f"manual_{field}",
            )
        broker_totals = {field: float(manual_totals[field]) for field in NUMERIC_FIELDS}
    else:
        upload = st.file_uploader(
            "Upload broker realized gain/loss CSV",
            type=["csv"],
            key="recon_broker_csv_uploader",
        )
        if upload is None:
            st.info("Upload a broker CSV to compute broker totals and diff drilldowns.")
        else:
            raw_broker_frame = pd.read_csv(upload)
            st.dataframe(raw_broker_frame.head(30), use_container_width=True, hide_index=True)

            default_mapping = infer_broker_mapping(list(raw_broker_frame.columns))
            mapping: dict[str, str | None] = {}
            with st.expander("Broker CSV mapping", expanded=False):
                st.caption("Hover the info icon on each field for mapping guidance.")
                for field, label in BROKER_CANONICAL_FIELDS.items():
                    options = ["<none>"] + list(raw_broker_frame.columns)
                    default_col = default_mapping.get(field)
                    default_index = options.index(default_col) if default_col in options else 0
                    selected = st.selectbox(
                        label,
                        options=options,
                        index=default_index,
                        key=f"recon_map_{field}",
                        help=BROKER_FIELD_HELP.get(field, ""),
                    )
                    mapping[field] = None if selected == "<none>" else selected

            broker_detail_frame = normalize_broker_dataframe(raw_broker_frame, mapping)
            broker_totals = tax_report_totals(broker_detail_frame.to_dict(orient="records"))
            st.caption(f"Broker rows after normalization: {len(broker_detail_frame)}")

    comparison = compare_totals(app_totals, broker_totals)
    comparison_frame = comparison_dataframe(comparison)
    checklist_frame = checklist_dataframe(report, comparison, app_detail_frame)
    health = reconciliation_health(comparison_frame)

    symbol_diff = pd.DataFrame(
        columns=["symbol", "app_gain_or_loss", "broker_gain_or_loss", "delta"]
    )
    date_diff = pd.DataFrame(
        columns=["date_sold", "app_gain_or_loss", "broker_gain_or_loss", "delta"]
    )
    term_diff = pd.DataFrame(
        columns=["term", "app_gain_or_loss", "broker_gain_or_loss", "delta"]
    )
    if not broker_detail_frame.empty:
        symbol_diff = diff_table_by_key(app_detail_frame, broker_detail_frame, "symbol")
        date_diff = diff_table_by_key(app_detail_frame, broker_detail_frame, "date_sold")
        term_diff = diff_table_by_key(app_detail_frame, broker_detail_frame, "term")

    packet_bytes = build_reconciliation_packet_zip(
        app_summary_frame=app_summary_frame,
        app_detail_frame=app_detail_frame,
        comparison_frame=comparison_frame,
        checklist_frame=checklist_frame,
        broker_detail_frame=broker_detail_frame,
        symbol_diff=symbol_diff,
        date_diff=date_diff,
        term_diff=term_diff,
    )
    packet_scope = (
        "all_accounts" if account_filter_id is None else account_filter_id.replace("-", "")
    )
    packet_name = f"reconciliation_packet_{tax_year}_{packet_scope}.zip"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(
        "App Gain/Loss",
        money(float(app_totals.get("total_gain_or_loss", 0.0) or 0.0)),
    )
    c2.metric(
        "Broker Gain/Loss",
        money(float(broker_totals.get("total_gain_or_loss", 0.0) or 0.0)),
    )
    c3.metric(
        "Gain/Loss Delta",
        money(float(comparison.get("total_gain_or_loss", {}).get("delta", 0.0) or 0.0)),
    )
    c4.metric("Metrics with Delta", int(health["mismatch_metrics"]))
    c5.metric("Max Absolute Delta", money(float(health["max_abs_delta"])))

    if bool(health["in_sync"]):
        st.success("App and broker totals are aligned within tolerance.")
    else:
        st.warning("Differences detected. Use Diff Drilldowns and Checklist to reconcile.")

    st.download_button(
        label="Download reconciliation packet (.zip)",
        data=packet_bytes,
        file_name=packet_name,
        mime="application/zip",
        key="download_reconciliation_packet_zip",
        use_container_width=True,
    )

    tab_totals, tab_diffs, tab_checklist = st.tabs(
        ["Totals Comparison", "Diff Drilldowns", "Checklist"]
    )

    with tab_totals:
        st.subheader("App Summary")
        st.dataframe(app_summary_frame, use_container_width=True, hide_index=True)
        csv_download(
            app_summary_frame,
            label="Download app summary CSV",
            filename=export_filename("recon_app_summary", accounts, account_filter_id),
            key="download_recon_app_summary_csv",
        )
        if not app_detail_frame.empty:
            csv_download(
                app_detail_frame,
                label="Download app detail CSV",
                filename=export_filename("recon_app_detail", accounts, account_filter_id),
                key="download_recon_app_detail_csv",
            )

        st.subheader("App vs Broker Totals")
        st.dataframe(comparison_frame, use_container_width=True, hide_index=True)
        csv_download(
            comparison_frame,
            label="Download totals comparison CSV",
            filename=export_filename("recon_totals_comparison", accounts, account_filter_id),
            key="download_recon_totals_csv",
        )

        if not broker_detail_frame.empty:
            csv_download(
                broker_detail_frame,
                label="Download normalized broker rows CSV",
                filename=export_filename("recon_broker_rows", accounts, account_filter_id),
                key="download_recon_broker_rows_csv",
            )

    with tab_diffs:
        if broker_detail_frame.empty:
            st.info("Upload broker CSV rows to enable symbol/date/term drilldowns.")
        else:
            st.markdown("**Diff by symbol**")
            st.dataframe(symbol_diff, use_container_width=True, hide_index=True)
            csv_download(
                symbol_diff,
                label="Download diff-by-symbol CSV",
                filename=export_filename("recon_diff_symbol", accounts, account_filter_id),
                key="download_recon_diff_symbol_csv",
            )

            st.markdown("**Diff by sale date**")
            st.dataframe(date_diff, use_container_width=True, hide_index=True)
            csv_download(
                date_diff,
                label="Download diff-by-date CSV",
                filename=export_filename("recon_diff_date", accounts, account_filter_id),
                key="download_recon_diff_date_csv",
            )

            st.markdown("**Diff by term**")
            st.dataframe(term_diff, use_container_width=True, hide_index=True)
            csv_download(
                term_diff,
                label="Download diff-by-term CSV",
                filename=export_filename("recon_diff_term", accounts, account_filter_id),
                key="download_recon_diff_term_csv",
            )

    with tab_checklist:
        st.dataframe(checklist_frame, use_container_width=True, hide_index=True)
        csv_download(
            checklist_frame,
            label="Download checklist CSV",
            filename=export_filename("recon_checklist", accounts, account_filter_id),
            key="download_recon_checklist_csv",
        )
