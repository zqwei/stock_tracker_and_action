from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import func, select
from sqlalchemy.orm import Session

# app.py -> streamlit -> ui -> portfolio_assistant -> src -> repo root
REPO_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.analytics.reconciliation import (
    contributions_by_month,
    daily_realized_pnl,
    net_contributions,
    realized_by_symbol,
)
from portfolio_assistant.analytics.wash_sale import detect_wash_sale_risks
from portfolio_assistant.assistant.tools_db import (
    create_account,
    get_engine,
    insert_cash_activity,
    insert_trade_import,
    list_accounts,
)
from portfolio_assistant.config.paths import ensure_data_dirs
from portfolio_assistant.db.migrate import migrate
from portfolio_assistant.db.models import (
    Account,
    CashActivity,
    PnlRealized,
    PositionOpen,
    TradeNormalized,
)
from portfolio_assistant.ingest.csv_import import (
    load_cash_csv_preview,
    load_trade_csv_preview,
    normalize_cash_records,
    normalize_trade_records,
)
from portfolio_assistant.ingest.csv_mapping import (
    CASH_CANONICAL_FIELDS,
    CASH_REQUIRED_FIELDS,
    TRADE_CANONICAL_FIELDS,
    TRADE_REQUIRED_FIELDS,
    get_saved_trade_mapping,
    missing_required_fields,
    save_trade_mapping,
)
from portfolio_assistant.ingest.validators import parse_datetime

try:
    import altair as alt
except Exception:  # pragma: no cover - fallback path
    alt = None


NAV_ITEMS = [
    "Accounts",
    "Import Trades",
    "Import Cash",
    "Overview",
    "Calendar",
    "Wash Sale Risk",
    "Data Quality",
]


@st.cache_resource
def _initialize_app_engine():
    ensure_data_dirs()
    migrate()
    return get_engine()


def _load_accounts(engine) -> list[Account]:
    with Session(engine) as session:
        return list_accounts(session)


def _select_account(accounts: list[Account], key_prefix: str) -> Account | None:
    if not accounts:
        return None
    options = [f"{a.broker} | {a.account_label} | {a.account_type.value}" for a in accounts]
    selected = st.selectbox("Account", options, key=f"{key_prefix}_account_selector")
    idx = options.index(selected)
    return accounts[idx]


def _render_accounts(engine, accounts: list[Account]) -> None:
    st.header("Accounts")
    st.caption("Create and manage brokerage accounts (taxable + IRA).")

    with st.form("add_account_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        broker = c1.text_input("Broker", placeholder="Webull")
        label = c2.text_input("Account label", placeholder="Taxable #1")
        account_type = c3.selectbox("Account type", ["TAXABLE", "TRAD_IRA", "ROTH_IRA"])
        submitted = st.form_submit_button("Add account")

    if submitted:
        if not broker.strip() or not label.strip():
            st.error("Broker and account label are required.")
        else:
            try:
                with Session(engine) as session:
                    create_account(session, broker=broker, account_label=label, account_type=account_type)
                    session.commit()
                st.success("Account added.")
                st.rerun()
            except Exception as exc:  # pragma: no cover - streamlit interaction
                st.error(f"Could not add account: {exc}")

    if not accounts:
        st.info("No accounts yet. Add a taxable and/or IRA account to start.")
        return

    rows = [
        {
            "account_id": a.id,
            "broker": a.broker,
            "account_label": a.account_label,
            "account_type": a.account_type.value,
            "created_at": a.created_at,
        }
        for a in accounts
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True)


def _render_import_trades(engine, accounts: list[Account]) -> None:
    st.header("Import Trades")
    st.caption("Upload trade CSV, review mapping, persist raw + normalized rows.")

    if not accounts:
        st.warning("Add at least one account before importing trades.")
        return

    account = _select_account(accounts, key_prefix="trades")
    if account is None:
        return

    broker = st.text_input("Broker template", value=account.broker or "generic")
    uploaded_file = st.file_uploader("Upload trade CSV", type=["csv"], key="trade_csv")
    if not uploaded_file:
        st.info("Upload a trade CSV to begin.")
        return

    uploaded_file.seek(0)
    preview = load_trade_csv_preview(uploaded_file, broker=broker)
    uploaded_file.seek(0)
    df = pd.read_csv(uploaded_file)

    st.write("File signature:", preview.signature)
    st.write("Detected columns:", preview.columns)

    saved_mapping = get_saved_trade_mapping(broker=broker, signature=preview.signature)
    mapping_seed = saved_mapping or preview.mapping
    st.subheader("Column Mapping")
    st.caption("Map source CSV columns to canonical trade fields.")

    current_mapping: dict[str, str] = {}
    options = ["--"] + preview.columns
    for canonical in TRADE_CANONICAL_FIELDS:
        default_col = mapping_seed.get(canonical, "--")
        default_idx = options.index(default_col) if default_col in options else 0
        selected = st.selectbox(
            canonical,
            options=options,
            index=default_idx,
            key=f"trade_map_{canonical}",
        )
        if selected != "--":
            current_mapping[canonical] = selected

    missing = missing_required_fields(current_mapping, required_fields=TRADE_REQUIRED_FIELDS)
    if missing:
        st.error(f"Missing required mappings: {', '.join(missing)}")
    else:
        st.success("Required mappings are complete.")

    normalized_rows, issues = normalize_trade_records(
        df=df, mapping=current_mapping, account_id=account.id, broker=broker
    )
    if issues:
        st.warning(f"{len(issues)} row issues detected. Invalid rows will be skipped.")
        st.write(issues[:20])

    if normalized_rows:
        st.subheader("Normalized preview")
        st.dataframe(pd.DataFrame(normalized_rows).head(200), use_container_width=True)

    if st.button("Save mapping + import trades", type="primary", key="import_trades_btn"):
        if missing:
            st.error("Fix required mappings first.")
            return
        if not normalized_rows:
            st.error("No valid normalized rows to import.")
            return

        try:
            save_trade_mapping(
                broker=broker,
                signature=preview.signature,
                columns=preview.columns,
                mapping=current_mapping,
            )
        except ValueError as exc:
            st.error(f"Could not save mapping: {exc}")
            return

        source_file = f"{uploaded_file.name}:{datetime.utcnow().isoformat(timespec='seconds')}"
        mapping_name = f"{broker}:{preview.signature}"
        raw_rows = df.fillna("").to_dict(orient="records")

        with Session(engine) as session:
            raw_count, normalized_count = insert_trade_import(
                session,
                account_id=account.id,
                broker=broker,
                source_file=source_file,
                file_sig=preview.signature,
                mapping_name=mapping_name,
                raw_rows=raw_rows,
                normalized_rows=normalized_rows,
            )
            pnl_stats = recompute_pnl(session)
            session.commit()

        st.success(
            f"Imported {normalized_count} normalized trades ({raw_count} raw rows). "
            f"Recomputed analytics: {pnl_stats['realized_rows']} realized rows, "
            f"{pnl_stats['open_rows']} open positions."
        )


def _render_import_cash(engine, accounts: list[Account]) -> None:
    st.header("Import Cash")
    st.caption("Upload cash CSV, tag external vs internal transfers, save to DB.")

    if not accounts:
        st.warning("Add at least one account before importing cash activity.")
        return

    account = _select_account(accounts, key_prefix="cash")
    if account is None:
        return

    broker = st.text_input("Cash broker template", value=account.broker or "generic")
    uploaded_file = st.file_uploader("Upload cash CSV", type=["csv"], key="cash_csv")
    if not uploaded_file:
        st.info("Upload a cash activity CSV to begin.")
        return

    uploaded_file.seek(0)
    preview = load_cash_csv_preview(uploaded_file, broker=broker)
    uploaded_file.seek(0)
    df = pd.read_csv(uploaded_file)

    st.write("File signature:", preview.signature)
    st.write("Detected columns:", preview.columns)

    st.subheader("Column Mapping")
    current_mapping: dict[str, str] = {}
    options = ["--"] + preview.columns
    for canonical in CASH_CANONICAL_FIELDS:
        default_col = preview.mapping.get(canonical, "--")
        default_idx = options.index(default_col) if default_col in options else 0
        selected = st.selectbox(
            canonical,
            options=options,
            index=default_idx,
            key=f"cash_map_{canonical}",
        )
        if selected != "--":
            current_mapping[canonical] = selected

    missing = missing_required_fields(current_mapping, required_fields=CASH_REQUIRED_FIELDS)
    if missing:
        st.error(f"Missing required mappings: {', '.join(missing)}")
        return

    cash_rows, issues = normalize_cash_records(
        df=df, mapping=current_mapping, account_id=account.id, broker=broker
    )
    if issues:
        st.warning(f"{len(issues)} row issues detected. Invalid rows will be skipped.")
        st.write(issues[:20])
    if not cash_rows:
        st.error("No valid cash rows to import.")
        return

    editable_df = pd.DataFrame(cash_rows)
    editable_df["posted_at"] = editable_df["posted_at"].astype(str)
    edited_df = st.data_editor(
        editable_df[
            ["posted_at", "activity_type", "amount", "description", "source", "is_external"]
        ],
        use_container_width=True,
        hide_index=True,
    )

    if st.button("Import cash activity", type="primary", key="import_cash_btn"):
        final_rows = []
        for row in edited_df.to_dict(orient="records"):
            posted_at = parse_datetime(row.get("posted_at"))
            if posted_at is None:
                continue
            is_external = row.get("is_external")
            if pd.isna(is_external):
                is_external = None
            final_rows.append(
                {
                    "account_id": account.id,
                    "broker": broker,
                    "posted_at": posted_at,
                    "activity_type": row.get("activity_type", "DEPOSIT"),
                    "amount": float(row.get("amount", 0.0)),
                    "description": str(row.get("description", "")),
                    "source": str(row.get("source", "")) or None,
                    "is_external": is_external,
                }
            )

        with Session(engine) as session:
            inserted = insert_cash_activity(session, final_rows)
            session.commit()

        st.success(f"Imported {inserted} cash activity rows.")


def _render_overview(engine, account_filter_id: str | None) -> None:
    st.header("Overview")
    st.caption("Consolidated or per-account snapshot of P&L and contributions.")

    with Session(engine) as session:
        realized_stmt = select(func.coalesce(func.sum(PnlRealized.pnl), 0.0))
        unrealized_stmt = select(func.coalesce(func.sum(PositionOpen.unrealized_pnl), 0.0))
        if account_filter_id:
            realized_stmt = realized_stmt.where(PnlRealized.account_id == account_filter_id)
            unrealized_stmt = unrealized_stmt.where(PositionOpen.account_id == account_filter_id)

        realized_total = float(session.scalar(realized_stmt) or 0.0)
        unrealized_total = float(session.scalar(unrealized_stmt) or 0.0)
        contributions_total = net_contributions(session, account_id=account_filter_id)

        c1, c2, c3 = st.columns(3)
        c1.metric("Realized P&L", f"{realized_total:,.2f}")
        c2.metric("Unrealized P&L", f"{unrealized_total:,.2f}")
        c3.metric("Net Contributions", f"{contributions_total:,.2f}")

        if st.button("Recompute analytics", key="recompute_btn"):
            stats = recompute_pnl(session, account_id=account_filter_id)
            session.commit()
            st.success(
                f"Recomputed: {stats['realized_rows']} realized rows, "
                f"{stats['open_rows']} open positions."
            )

        realized_rows = realized_by_symbol(session, account_id=account_filter_id)
        if realized_rows:
            st.subheader("Realized P&L by symbol")
            st.dataframe(pd.DataFrame(realized_rows), use_container_width=True)

        contrib_rows = contributions_by_month(session, account_id=account_filter_id)
        if contrib_rows:
            st.subheader("Net contributions by month")
            st.dataframe(pd.DataFrame(contrib_rows), use_container_width=True)

        pos_stmt = select(PositionOpen)
        if account_filter_id:
            pos_stmt = pos_stmt.where(PositionOpen.account_id == account_filter_id)
        positions = list(session.scalars(pos_stmt).all())
        if positions:
            st.subheader("Open positions")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "account_id": p.account_id,
                            "instrument_type": p.instrument_type.value,
                            "symbol": p.symbol,
                            "option_symbol_raw": p.option_symbol_raw,
                            "quantity": p.quantity,
                            "avg_cost": p.avg_cost,
                            "last_price": p.last_price,
                            "market_value": p.market_value,
                            "unrealized_pnl": p.unrealized_pnl,
                        }
                        for p in positions
                    ]
                ),
                use_container_width=True,
            )


def _render_calendar(engine, account_filter_id: str | None) -> None:
    st.header("Calendar")
    st.caption("Daily realized P&L heatmap and detail table.")

    with Session(engine) as session:
        rows = daily_realized_pnl(session, account_id=account_filter_id)

    if not rows:
        st.info("No realized P&L rows yet.")
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["close_date"])
    df = df.sort_values("date")

    if alt is not None:
        chart = (
            alt.Chart(df)
            .mark_rect()
            .encode(
                x=alt.X("yearmonth(date):O", title="Month"),
                y=alt.Y("date(date):O", title="Day"),
                color=alt.Color("pnl:Q", title="Realized P&L"),
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("pnl:Q", title="P&L", format=",.2f"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.bar_chart(df.set_index("date")["pnl"])

    st.subheader("Daily details")
    st.dataframe(df[["close_date", "pnl"]], use_container_width=True)


def _render_wash_sale(engine, account_filter_id: str | None) -> None:
    st.header("Wash Sale Risk")
    st.caption("Basic +/-30 day same-ticker replacement buy detection across accounts.")

    with Session(engine) as session:
        risks = detect_wash_sale_risks(session, account_id=account_filter_id, window_days=30)

    if not risks:
        st.success("No wash-sale risk matches detected with current data.")
        return

    st.warning(f"Detected {len(risks)} potential wash-sale risk matches.")
    risk_df = pd.DataFrame(risks)
    st.dataframe(risk_df, use_container_width=True)


def _render_data_quality(engine, account_filter_id: str | None) -> None:
    st.header("Data Quality")
    st.caption("Data completeness checks for imports and normalization.")

    with Session(engine) as session:
        trade_stmt = select(func.count()).select_from(PnlRealized)
        normalized_stmt = select(func.count()).select_from(PositionOpen)
        unknown_external_stmt = select(func.count()).select_from(CashActivity).where(
            CashActivity.is_external.is_(None)
        )
        if account_filter_id:
            trade_stmt = trade_stmt.where(PnlRealized.account_id == account_filter_id)
            normalized_stmt = normalized_stmt.where(
                PositionOpen.account_id == account_filter_id
            )
        if account_filter_id:
            unknown_external_stmt = unknown_external_stmt.where(
                CashActivity.account_id == account_filter_id
            )

        realized_count = int(session.scalar(trade_stmt) or 0)
        open_positions_count = int(session.scalar(normalized_stmt) or 0)
        unknown_external_count = int(session.scalar(unknown_external_stmt) or 0)

        option_missing_stmt = select(func.count()).select_from(TradeNormalized).where(
            TradeNormalized.instrument_type == "OPTION",
            (
                (TradeNormalized.underlying.is_(None))
                | (TradeNormalized.expiration.is_(None))
                | (TradeNormalized.strike.is_(None))
                | (TradeNormalized.call_put.is_(None))
            ),
        )
        if account_filter_id:
            option_missing_stmt = option_missing_stmt.where(
                TradeNormalized.account_id == account_filter_id
            )
        option_missing = int(session.scalar(option_missing_stmt) or 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Realized rows", realized_count)
    c2.metric("Open positions", open_positions_count)
    c3.metric("Cash rows needing external tag", unknown_external_count)
    c4.metric("Options with missing parsed fields", option_missing)


def main() -> None:
    st.set_page_config(page_title="Portfolio Assistant", layout="wide")
    engine = _initialize_app_engine()
    accounts = _load_accounts(engine)

    account_filter_id = None
    with st.sidebar:
        st.title("Portfolio Assistant")
        nav = st.radio("Navigate", NAV_ITEMS, index=0)

        labels = ["All accounts (consolidated)"] + [
            f"{a.broker} | {a.account_label} | {a.account_type.value}" for a in accounts
        ]
        selected_label = st.selectbox("Global account filter", labels, index=0)
        if selected_label != labels[0]:
            idx = labels.index(selected_label) - 1
            account_filter_id = accounts[idx].id

        st.caption("Phase 1 MVP")

    if nav == "Accounts":
        _render_accounts(engine, accounts)
    elif nav == "Import Trades":
        _render_import_trades(engine, accounts)
    elif nav == "Import Cash":
        _render_import_cash(engine, accounts)
    elif nav == "Overview":
        _render_overview(engine, account_filter_id)
    elif nav == "Calendar":
        _render_calendar(engine, account_filter_id)
    elif nav == "Wash Sale Risk":
        _render_wash_sale(engine, account_filter_id)
    else:
        _render_data_quality(engine, account_filter_id)


if __name__ == "__main__":
    main()
