from __future__ import annotations

import sys
from datetime import date, datetime
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


def _money(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,.2f}"


def _safe_key(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value.lower())


def _account_label(account: Account) -> str:
    return f"{account.broker} | {account.account_label} | {account.account_type.value}"


def _account_lookup(accounts: list[Account]) -> dict[str, Account]:
    return {account.id: account for account in accounts}


def _set_nav_page(page: str) -> None:
    st.session_state["nav_item"] = page
    st.rerun()


def _render_sidebar(accounts: list[Account]) -> tuple[str, str | None]:
    if "nav_item" not in st.session_state:
        st.session_state["nav_item"] = NAV_ITEMS[0]

    account_by_id = _account_lookup(accounts)
    account_options: list[str | None] = [None] + [account.id for account in accounts]
    filter_state_key = "global_account_filter_id"
    current_filter = st.session_state.get(filter_state_key)
    if current_filter not in set(account_options):
        st.session_state[filter_state_key] = None

    with st.sidebar:
        st.title("Portfolio Assistant")
        nav = st.radio("Navigate", NAV_ITEMS, key="nav_item")

        account_filter_id = st.selectbox(
            "Global account filter",
            options=account_options,
            format_func=lambda account_id: (
                "All accounts (consolidated)"
                if account_id is None
                else _account_label(account_by_id[account_id])
            ),
            key=filter_state_key,
        )
        st.caption("Phase 1 MVP")

    return nav, account_filter_id


def _render_flow_header(nav: str, accounts: list[Account], account_filter_id: str | None) -> None:
    idx = NAV_ITEMS.index(nav)
    progress = float(idx + 1) / float(len(NAV_ITEMS))
    st.progress(progress)

    if account_filter_id is None:
        st.caption(f"Workflow step {idx + 1}/{len(NAV_ITEMS)}. Account scope: all accounts.")
    else:
        account = _account_lookup(accounts).get(account_filter_id)
        if account is not None:
            st.caption(
                "Workflow step "
                f"{idx + 1}/{len(NAV_ITEMS)}. "
                f"Account scope: {_account_label(account)}."
            )

    col_prev, col_next = st.columns(2)
    if col_prev.button(
        "Previous",
        key="nav_previous_btn",
        disabled=idx == 0,
        use_container_width=True,
    ):
        _set_nav_page(NAV_ITEMS[idx - 1])
    if col_next.button(
        "Next",
        key="nav_next_btn",
        disabled=idx >= len(NAV_ITEMS) - 1,
        use_container_width=True,
    ):
        _set_nav_page(NAV_ITEMS[idx + 1])


def _select_account(
    accounts: list[Account],
    key_prefix: str,
    *,
    label: str,
    default_account_id: str | None = None,
) -> Account | None:
    if not accounts:
        return None

    account_by_id = _account_lookup(accounts)
    option_ids = [account.id for account in accounts]
    state_key = f"{key_prefix}_account_selector"
    if st.session_state.get(state_key) not in set(option_ids):
        if state_key in st.session_state:
            del st.session_state[state_key]

    default_index = 0
    if default_account_id in set(option_ids):
        default_index = option_ids.index(default_account_id)

    selected_id = st.selectbox(
        label,
        options=option_ids,
        index=default_index,
        format_func=lambda account_id: _account_label(account_by_id[account_id]),
        key=state_key,
    )
    return account_by_id[selected_id]


def _select_import_account(
    accounts: list[Account], account_filter_id: str | None, key_prefix: str
) -> Account | None:
    if not accounts:
        return None

    account_by_id = _account_lookup(accounts)
    default_account = account_by_id.get(account_filter_id) if account_filter_id else None

    if default_account is not None:
        st.caption(f"Global account preselected: {_account_label(default_account)}")
        use_global = st.checkbox(
            "Use global account for this import",
            value=True,
            key=f"{key_prefix}_use_global_account",
        )
        if use_global:
            return default_account

    return _select_account(
        accounts,
        key_prefix=key_prefix,
        label="Import target account",
        default_account_id=account_filter_id,
    )


def _render_mapping_inputs(
    *,
    columns: list[str],
    mapping_seed: dict[str, str],
    canonical_fields: list[str],
    required_fields: list[str],
    key_prefix: str,
) -> tuple[dict[str, str], list[str]]:
    options = ["--"] + columns
    required_set = set(required_fields)
    required_order = [field for field in canonical_fields if field in required_set]
    optional_order = [field for field in canonical_fields if field not in required_set]

    current_mapping: dict[str, str] = {}

    st.markdown("**Required fields**")
    required_cols = st.columns(2)
    for idx, canonical in enumerate(required_order):
        default_col = mapping_seed.get(canonical, "--")
        default_idx = options.index(default_col) if default_col in options else 0
        selected = required_cols[idx % 2].selectbox(
            canonical,
            options=options,
            index=default_idx,
            key=f"{key_prefix}_required_{canonical}",
        )
        if selected != "--":
            current_mapping[canonical] = selected

    if optional_order:
        with st.expander("Optional fields", expanded=False):
            optional_cols = st.columns(2)
            for idx, canonical in enumerate(optional_order):
                default_col = mapping_seed.get(canonical, "--")
                default_idx = options.index(default_col) if default_col in options else 0
                selected = optional_cols[idx % 2].selectbox(
                    canonical,
                    options=options,
                    index=default_idx,
                    key=f"{key_prefix}_optional_{canonical}",
                )
                if selected != "--":
                    current_mapping[canonical] = selected

    missing = missing_required_fields(current_mapping, required_fields=required_fields)
    required_done = len(required_fields) - len(missing)
    optional_done = max(len(current_mapping) - required_done, 0)

    m1, m2, m3 = st.columns(3)
    m1.metric("Required mapped", f"{required_done}/{len(required_fields)}")
    m2.metric("Optional mapped", optional_done)
    m3.metric("Detected CSV columns", len(columns))

    if missing:
        st.error(f"Missing required mappings: {', '.join(missing)}")
    else:
        st.success("Required mappings are complete.")

    return current_mapping, missing


def _render_row_issues(issues: list[str], label: str) -> None:
    if not issues:
        return

    st.warning(f"{len(issues)} {label}. Invalid rows will be skipped.")
    issue_df = pd.DataFrame({"issue": issues})
    issue_df["category"] = issue_df["issue"].map(
        lambda text: text.split(":", 1)[1].strip() if ":" in text else text
    )
    summary_df = (
        issue_df.groupby("category", as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
    )

    c1, c2 = st.columns([1, 2])
    c1.dataframe(summary_df.head(10), use_container_width=True, hide_index=True)
    c2.dataframe(issue_df.head(75), use_container_width=True, hide_index=True)


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
                    create_account(
                        session,
                        broker=broker,
                        account_label=label,
                        account_type=account_type,
                    )
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
            "account_id": account.id,
            "broker": account.broker,
            "account_label": account.account_label,
            "account_type": account.account_type.value,
            "created_at": account.created_at,
        }
        for account in accounts
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    if st.button("Next: Import Trades", key="accounts_next_to_import_trades"):
        _set_nav_page("Import Trades")


def _render_import_trades(
    engine,
    accounts: list[Account],
    account_filter_id: str | None,
) -> None:
    st.header("Import Trades")
    st.caption("Upload trade CSV, map columns once, and import normalized rows.")

    if not accounts:
        st.warning("Add at least one account before importing trades.")
        return

    account = _select_import_account(accounts, account_filter_id, key_prefix="trade_import")
    if account is None:
        return

    broker = (
        st.text_input(
            "Broker template",
            value=account.broker or "generic",
            key="trade_broker_template",
        ).strip()
        or "generic"
    )
    uploaded_file = st.file_uploader("Upload trade CSV", type=["csv"], key="trade_csv")
    if not uploaded_file:
        st.info("Upload a trade CSV to begin.")
        return

    try:
        uploaded_file.seek(0)
        preview = load_trade_csv_preview(uploaded_file, broker=broker)
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file)
    except Exception as exc:
        st.error(f"Could not parse trade CSV: {exc}")
        return

    m1, m2, m3 = st.columns(3)
    m1.metric("Rows in file", len(df))
    m2.metric("Columns detected", len(preview.columns))
    m3.metric("Required fields", len(TRADE_REQUIRED_FIELDS))

    st.caption(f"File signature: `{preview.signature}`")
    with st.expander("Detected columns", expanded=False):
        st.write(preview.columns)
    with st.expander("Raw CSV preview (first 20 rows)", expanded=False):
        st.dataframe(df.head(20), use_container_width=True, hide_index=True)

    saved_mapping = get_saved_trade_mapping(broker=broker, signature=preview.signature)
    if saved_mapping:
        st.info("Loaded saved mapping for this broker + file signature.")
    elif preview.missing_required:
        st.warning(
            "Auto-mapping is incomplete for required fields: "
            f"{', '.join(preview.missing_required)}"
        )

    st.subheader("Column Mapping")
    st.caption("Map required fields first. Optional fields are collapsed by default.")

    mapping_seed = saved_mapping or preview.mapping
    mapping_key = f"trade_map_{_safe_key(broker)}_{preview.signature[:10]}"
    current_mapping, missing = _render_mapping_inputs(
        columns=preview.columns,
        mapping_seed=mapping_seed,
        canonical_fields=TRADE_CANONICAL_FIELDS,
        required_fields=TRADE_REQUIRED_FIELDS,
        key_prefix=mapping_key,
    )

    normalized_rows: list[dict] = []
    issues: list[str] = []
    if not missing:
        normalized_rows, issues = normalize_trade_records(
            df=df,
            mapping=current_mapping,
            account_id=account.id,
            broker=broker,
        )

    _render_row_issues(issues, "trade row issues")

    if normalized_rows:
        st.subheader("Normalized preview")
        normalized_df = pd.DataFrame(normalized_rows).head(200)
        preferred_order = [
            "executed_at",
            "instrument_type",
            "symbol",
            "side",
            "quantity",
            "price",
            "fees",
            "net_amount",
            "option_symbol_raw",
            "currency",
        ]
        ordered_cols = [col for col in preferred_order if col in normalized_df.columns]
        ordered_cols += [col for col in normalized_df.columns if col not in ordered_cols]
        st.dataframe(normalized_df[ordered_cols], use_container_width=True, hide_index=True)

    valid_count = len(normalized_rows)
    skipped_count = len(issues)
    c_valid, c_skipped = st.columns(2)
    c_valid.metric("Valid normalized rows", valid_count)
    c_skipped.metric("Skipped rows", skipped_count)

    can_import = not missing and valid_count > 0
    if not can_import:
        if missing:
            st.info("Complete required mappings to enable import.")
        elif valid_count == 0:
            st.info("No valid rows were found after normalization.")

    if st.button(
        "Save mapping + import trades",
        type="primary",
        key="import_trades_btn",
        disabled=not can_import,
    ):
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

        raw_skipped = max(len(raw_rows) - raw_count, 0)
        normalized_skipped = max(len(normalized_rows) - normalized_count, 0)
        if raw_skipped > 0 or normalized_skipped > 0:
            st.info(
                "Duplicate rows were skipped: "
                f"{raw_skipped} raw, {normalized_skipped} normalized."
            )

    if st.button("Next: Import Cash", key="import_trades_next_to_cash"):
        _set_nav_page("Import Cash")


def _render_import_cash(
    engine,
    accounts: list[Account],
    account_filter_id: str | None,
) -> None:
    st.header("Import Cash")
    st.caption("Upload cash CSV, map columns, and tag external transfers before import.")

    if not accounts:
        st.warning("Add at least one account before importing cash activity.")
        return

    account = _select_import_account(accounts, account_filter_id, key_prefix="cash_import")
    if account is None:
        return

    broker = (
        st.text_input(
            "Cash broker template",
            value=account.broker or "generic",
            key="cash_broker_template",
        ).strip()
        or "generic"
    )
    uploaded_file = st.file_uploader("Upload cash CSV", type=["csv"], key="cash_csv")
    if not uploaded_file:
        st.info("Upload a cash activity CSV to begin.")
        return

    try:
        uploaded_file.seek(0)
        preview = load_cash_csv_preview(uploaded_file, broker=broker)
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file)
    except Exception as exc:
        st.error(f"Could not parse cash CSV: {exc}")
        return

    m1, m2, m3 = st.columns(3)
    m1.metric("Rows in file", len(df))
    m2.metric("Columns detected", len(preview.columns))
    m3.metric("Required fields", len(CASH_REQUIRED_FIELDS))

    st.caption(f"File signature: `{preview.signature}`")
    with st.expander("Detected columns", expanded=False):
        st.write(preview.columns)
    with st.expander("Raw CSV preview (first 20 rows)", expanded=False):
        st.dataframe(df.head(20), use_container_width=True, hide_index=True)

    st.subheader("Column Mapping")
    mapping_key = f"cash_map_{_safe_key(broker)}_{preview.signature[:10]}"
    current_mapping, missing = _render_mapping_inputs(
        columns=preview.columns,
        mapping_seed=preview.mapping,
        canonical_fields=CASH_CANONICAL_FIELDS,
        required_fields=CASH_REQUIRED_FIELDS,
        key_prefix=mapping_key,
    )

    cash_rows: list[dict] = []
    issues: list[str] = []
    if not missing:
        cash_rows, issues = normalize_cash_records(
            df=df,
            mapping=current_mapping,
            account_id=account.id,
            broker=broker,
        )

    _render_row_issues(issues, "cash row issues")

    if cash_rows:
        editable_df = pd.DataFrame(cash_rows)
        editable_df["posted_at"] = editable_df["posted_at"].astype(str)
        edited_df = st.data_editor(
            editable_df[
                [
                    "posted_at",
                    "activity_type",
                    "amount",
                    "description",
                    "source",
                    "is_external",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        edited_df = pd.DataFrame()

    valid_count = len(cash_rows)
    skipped_count = len(issues)
    c_valid, c_skipped = st.columns(2)
    c_valid.metric("Valid cash rows", valid_count)
    c_skipped.metric("Skipped rows", skipped_count)

    can_import = not missing and valid_count > 0
    if not can_import:
        if missing:
            st.info("Complete required mappings to enable cash import.")
        elif valid_count == 0:
            st.info("No valid cash rows were found after normalization.")

    if st.button(
        "Import cash activity",
        type="primary",
        key="import_cash_btn",
        disabled=not can_import,
    ):
        invalid_after_edit = 0
        final_rows: list[dict] = []

        for row in edited_df.to_dict(orient="records"):
            posted_at = parse_datetime(row.get("posted_at"))
            if posted_at is None:
                invalid_after_edit += 1
                continue

            amount_value = row.get("amount", 0.0)
            try:
                amount = float(amount_value)
            except (TypeError, ValueError):
                invalid_after_edit += 1
                continue

            if amount <= 0:
                invalid_after_edit += 1
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
                    "amount": amount,
                    "description": str(row.get("description", "")),
                    "source": str(row.get("source", "")) or None,
                    "is_external": is_external,
                }
            )

        if not final_rows:
            st.error("No valid cash rows left to import after editing.")
            return

        with Session(engine) as session:
            inserted = insert_cash_activity(session, final_rows)
            session.commit()

        st.success(f"Imported {inserted} cash activity rows.")
        if invalid_after_edit > 0:
            st.warning(f"Skipped {invalid_after_edit} edited rows with invalid date/amount.")

        deduped = max(len(final_rows) - inserted, 0)
        if deduped > 0:
            st.info(f"Skipped {deduped} duplicate cash rows.")

    if st.button("Next: Overview", key="import_cash_next_to_overview"):
        _set_nav_page("Overview")


def _render_overview(
    engine,
    account_filter_id: str | None,
    accounts: list[Account],
) -> None:
    st.header("Overview")
    st.caption("Snapshot of P&L, contributions, and open positions for the current account scope.")

    if st.button("Recompute analytics", key="recompute_btn"):
        with Session(engine) as session:
            stats = recompute_pnl(session, account_id=account_filter_id)
            session.commit()
        st.success(
            f"Recomputed: {stats['realized_rows']} realized rows, "
            f"{stats['open_rows']} open positions."
        )
        st.rerun()

    with Session(engine) as session:
        realized_stmt = select(func.coalesce(func.sum(PnlRealized.pnl), 0.0))
        unrealized_stmt = select(func.coalesce(func.sum(PositionOpen.unrealized_pnl), 0.0))
        if account_filter_id:
            realized_stmt = realized_stmt.where(PnlRealized.account_id == account_filter_id)
            unrealized_stmt = unrealized_stmt.where(PositionOpen.account_id == account_filter_id)

        realized_total = float(session.scalar(realized_stmt) or 0.0)
        unrealized_total = float(session.scalar(unrealized_stmt) or 0.0)
        contributions_total = net_contributions(session, account_id=account_filter_id)

        realized_rows = realized_by_symbol(session, account_id=account_filter_id)
        contrib_rows = contributions_by_month(session, account_id=account_filter_id)

        pos_stmt = select(PositionOpen)
        if account_filter_id:
            pos_stmt = pos_stmt.where(PositionOpen.account_id == account_filter_id)
        positions = list(session.scalars(pos_stmt).all())

    total_pnl = realized_total + unrealized_total
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total P&L", _money(total_pnl))
    c2.metric("Realized P&L", _money(realized_total))
    c3.metric("Unrealized P&L", _money(unrealized_total))
    c4.metric("Net Contributions", _money(contributions_total))
    c5.metric("Open Positions", len(positions))

    tab_realized, tab_contrib, tab_positions = st.tabs(
        ["Realized by Symbol", "Contributions", "Open Positions"]
    )

    with tab_realized:
        if not realized_rows:
            st.info("No realized P&L rows yet.")
        else:
            realized_df = pd.DataFrame(realized_rows)
            realized_df["realized_pnl"] = pd.to_numeric(
                realized_df["realized_pnl"], errors="coerce"
            ).fillna(0.0)
            realized_df["abs_pnl"] = realized_df["realized_pnl"].abs()
            realized_df = realized_df.sort_values("abs_pnl", ascending=False)
            realized_df = realized_df.drop(columns=["abs_pnl"])
            st.dataframe(realized_df, use_container_width=True, hide_index=True)

            if alt is not None and not realized_df.empty:
                top_symbols = realized_df.head(12).copy()
                chart = (
                    alt.Chart(top_symbols)
                    .mark_bar()
                    .encode(
                        x=alt.X("symbol:N", sort="-y", title="Symbol"),
                        y=alt.Y("realized_pnl:Q", title="Realized P&L"),
                        color=alt.condition(
                            "datum.realized_pnl >= 0",
                            alt.value("#1b9e77"),
                            alt.value("#d95f02"),
                        ),
                        tooltip=[
                            alt.Tooltip("symbol:N", title="Symbol"),
                            alt.Tooltip("instrument_type:N", title="Instrument"),
                            alt.Tooltip("realized_pnl:Q", title="P&L", format=",.2f"),
                        ],
                    )
                    .properties(height=280)
                )
                st.altair_chart(chart, use_container_width=True)

    with tab_contrib:
        if not contrib_rows:
            st.info("No external cash contribution rows yet.")
        else:
            contrib_df = pd.DataFrame(contrib_rows)
            contrib_df["net_contribution"] = pd.to_numeric(
                contrib_df["net_contribution"], errors="coerce"
            ).fillna(0.0)
            st.dataframe(contrib_df, use_container_width=True, hide_index=True)

            if alt is not None and not contrib_df.empty:
                chart = (
                    alt.Chart(contrib_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("month:N", title="Month"),
                        y=alt.Y("net_contribution:Q", title="Net Contribution"),
                        color=alt.condition(
                            "datum.net_contribution >= 0",
                            alt.value("#1b9e77"),
                            alt.value("#d95f02"),
                        ),
                        tooltip=[
                            alt.Tooltip("month:N", title="Month"),
                            alt.Tooltip(
                                "net_contribution:Q",
                                title="Net Contribution",
                                format=",.2f",
                            ),
                        ],
                    )
                    .properties(height=280)
                )
                st.altair_chart(chart, use_container_width=True)

    with tab_positions:
        if not positions:
            st.info("No open positions yet.")
        else:
            account_by_id = _account_lookup(accounts)
            positions_df = pd.DataFrame(
                [
                    {
                        "account": _account_label(account_by_id[p.account_id])
                        if p.account_id in account_by_id
                        else p.account_id,
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
            )
            positions_df["unrealized_abs"] = positions_df["unrealized_pnl"].abs()
            positions_df = positions_df.sort_values("unrealized_abs", ascending=False)
            positions_df = positions_df.drop(columns=["unrealized_abs"])
            st.dataframe(positions_df, use_container_width=True, hide_index=True)


def _render_calendar(engine, account_filter_id: str | None) -> None:
    st.header("Calendar")
    st.caption("Daily realized P&L view with date filtering and summary stats.")

    with Session(engine) as session:
        rows = daily_realized_pnl(session, account_id=account_filter_id)

    if not rows:
        st.info("No realized P&L rows yet.")
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["close_date"]).dt.date
    df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
    df = df.sort_values("date")

    min_date = df["date"].min()
    max_date = df["date"].max()
    selected_range = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key="calendar_date_range",
    )

    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
    elif isinstance(selected_range, list) and len(selected_range) == 2:
        start_date, end_date = selected_range[0], selected_range[1]
    elif isinstance(selected_range, date):
        start_date = selected_range
        end_date = selected_range
    else:
        start_date = min_date
        end_date = max_date

    filtered = df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
    if filtered.empty:
        st.warning("No realized P&L rows in the selected date range.")
        return

    total_pnl = float(filtered["pnl"].sum())
    avg_pnl = float(filtered["pnl"].mean())
    best_day = float(filtered["pnl"].max())
    worst_day = float(filtered["pnl"].min())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Trading days", len(filtered))
    c2.metric("Net realized P&L", _money(total_pnl))
    c3.metric("Average/day", _money(avg_pnl))
    c4.metric("Best day", _money(best_day))
    c5.metric("Worst day", _money(worst_day))

    chart_df = filtered.copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"])

    if alt is not None:
        daily_chart = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("pnl:Q", title="Realized P&L"),
                color=alt.condition(
                    "datum.pnl >= 0",
                    alt.value("#1b9e77"),
                    alt.value("#d95f02"),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("pnl:Q", title="P&L", format=",.2f"),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(daily_chart, use_container_width=True)

        monthly_df = chart_df.copy()
        monthly_df["month"] = monthly_df["date"].dt.to_period("M").dt.to_timestamp()
        monthly_df = (
            monthly_df.groupby("month", as_index=False)["pnl"]
            .sum()
            .sort_values("month")
        )
        monthly_chart = (
            alt.Chart(monthly_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("pnl:Q", title="Monthly realized P&L"),
                tooltip=[
                    alt.Tooltip("month:T", title="Month"),
                    alt.Tooltip("pnl:Q", title="P&L", format=",.2f"),
                ],
            )
            .properties(height=220)
        )
        st.altair_chart(monthly_chart, use_container_width=True)
    else:
        st.bar_chart(chart_df.set_index("date")["pnl"])

    details_df = filtered.copy()
    details_df["weekday"] = pd.to_datetime(details_df["date"]).dt.day_name()
    details_df = details_df.sort_values("date", ascending=False)
    st.subheader("Daily details")
    st.dataframe(
        details_df[["date", "weekday", "pnl"]],
        use_container_width=True,
        hide_index=True,
    )


def _render_wash_sale(engine, account_filter_id: str | None) -> None:
    st.header("Wash Sale Risk")
    st.caption("Potential same-ticker replacement buys within +/-30 days around taxable losses.")

    with Session(engine) as session:
        risks = detect_wash_sale_risks(session, account_id=account_filter_id, window_days=30)

    if not risks:
        st.success("No wash-sale risk matches detected with current data.")
        return

    risk_df = pd.DataFrame(risks)
    risk_df["sale_date"] = pd.to_datetime(risk_df["sale_date"], errors="coerce").dt.date
    risk_df["buy_date"] = pd.to_datetime(risk_df["buy_date"], errors="coerce").dt.date
    risk_df["sale_loss"] = pd.to_numeric(risk_df["sale_loss"], errors="coerce").fillna(0.0)
    risk_df["days_from_sale"] = pd.to_numeric(
        risk_df["days_from_sale"], errors="coerce"
    ).fillna(0).astype(int)
    risk_df["allocated_replacement_quantity_equiv"] = pd.to_numeric(
        risk_df["allocated_replacement_quantity_equiv"], errors="coerce"
    ).fillna(0.0)

    risk_df["risk_type"] = "Same-account replacement"
    risk_df.loc[risk_df["cross_account"], "risk_type"] = "Cross-account replacement"
    risk_df.loc[risk_df["ira_replacement"], "risk_type"] = "IRA replacement"

    symbols = sorted(str(symbol) for symbol in risk_df["symbol"].dropna().unique())
    controls = st.columns([2, 1, 1, 1])
    selected_symbols = controls[0].multiselect(
        "Symbols",
        options=symbols,
        default=symbols,
    )
    cross_only = controls[1].checkbox("Cross-account only", value=False)
    ira_only = controls[2].checkbox("IRA only", value=False)
    boundary_only = controls[3].checkbox("Boundary day only", value=False)

    filtered = risk_df.copy()
    if selected_symbols:
        filtered = filtered[filtered["symbol"].isin(selected_symbols)]
    else:
        filtered = filtered.iloc[0:0]
    if cross_only:
        filtered = filtered[filtered["cross_account"]]
    if ira_only:
        filtered = filtered[filtered["ira_replacement"]]
    if boundary_only:
        filtered = filtered[filtered["is_boundary_day"]]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Potential matches", len(filtered))
    c2.metric("Unique symbols", int(filtered["symbol"].nunique()))
    c3.metric("Cross-account", int(filtered["cross_account"].sum()))
    c4.metric("IRA replacements", int(filtered["ira_replacement"].sum()))

    st.caption("Informational flags only, not tax advice.")

    if filtered.empty:
        st.warning("No rows match the selected filters.")
        return

    display_df = filtered[
        [
            "risk_type",
            "symbol",
            "sale_date",
            "buy_date",
            "days_from_sale",
            "sale_account_label",
            "buy_account_label",
            "buy_account_type",
            "sale_loss",
            "allocated_replacement_quantity_equiv",
            "buy_trade_id",
        ]
    ].rename(
        columns={
            "risk_type": "Risk Type",
            "symbol": "Symbol",
            "sale_date": "Sale Date",
            "buy_date": "Buy Date",
            "days_from_sale": "Days From Sale",
            "sale_account_label": "Sale Account",
            "buy_account_label": "Buy Account",
            "buy_account_type": "Buy Account Type",
            "sale_loss": "Loss Amount",
            "allocated_replacement_quantity_equiv": "Matched Qty (Equiv)",
            "buy_trade_id": "Buy Trade ID",
        }
    )

    display_df = display_df.sort_values(
        ["Sale Date", "Symbol", "Days From Sale"],
        ascending=[False, True, True],
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


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

    nav, account_filter_id = _render_sidebar(accounts)
    _render_flow_header(nav, accounts, account_filter_id)

    if nav == "Accounts":
        _render_accounts(engine, accounts)
    elif nav == "Import Trades":
        _render_import_trades(engine, accounts, account_filter_id)
    elif nav == "Import Cash":
        _render_import_cash(engine, accounts, account_filter_id)
    elif nav == "Overview":
        _render_overview(engine, account_filter_id, accounts)
    elif nav == "Calendar":
        _render_calendar(engine, account_filter_id)
    elif nav == "Wash Sale Risk":
        _render_wash_sale(engine, account_filter_id)
    else:
        _render_data_quality(engine, account_filter_id)


if __name__ == "__main__":
    main()
