from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.analytics.reconciliation import compare_totals, tax_report_totals
from portfolio_assistant.analytics.tax_year_report import generate_tax_year_report
from portfolio_assistant.db.models import TradeNormalized
from portfolio_assistant.ui.streamlit.views.common import dataframe_to_csv_bytes
from portfolio_assistant.ui.streamlit.views.contributions import (
    account_contributions_dataframe,
    external_cash_activity_dataframe,
    monthly_contributions_dataframe,
)
from portfolio_assistant.ui.streamlit.views.holdings import holdings_dataframe
from portfolio_assistant.ui.streamlit.views.pnl import (
    realized_detail_dataframe,
    realized_summary_dataframe,
)
from portfolio_assistant.ui.streamlit.views.reconciliation import (
    build_reconciliation_packet_zip,
    checklist_dataframe,
    comparison_dataframe,
    diff_table_by_key,
    normalize_broker_dataframe,
    reconciliation_health,
)
from portfolio_assistant.ui.streamlit.views.settings import (
    account_catalog_dataframe,
    settings_metrics,
)
from portfolio_assistant.ui.streamlit.views.tax_year import (
    tax_year_detail_dataframe,
    tax_year_summary_dataframe,
    wash_sale_matches_dataframe,
)


def test_nav_display_label_prefers_icon_mapping():
    from portfolio_assistant.ui.streamlit import app as streamlit_app

    label = streamlit_app._nav_display_label("Import Trades")
    assert label.startswith("📥 ")
    assert label.endswith("Import Trades")

    fallback = streamlit_app._nav_display_label("Unknown")
    assert fallback == "• Unknown"


def test_instrument_type_decision_context_variants():
    from portfolio_assistant.ui.streamlit import app as streamlit_app

    missing = streamlit_app._instrument_type_decision_context(
        instrument_type_column=None,
        instrument_values_clear=False,
    )
    assert missing[0] is True
    assert "No Instrument Type column is mapped" in missing[1]

    unclear_values = streamlit_app._instrument_type_decision_context(
        instrument_type_column="Instrument Type",
        instrument_values_clear=False,
    )
    assert unclear_values[0] is True
    assert "appear unclear" in unclear_values[1]

    non_type_like = streamlit_app._instrument_type_decision_context(
        instrument_type_column="status",
        instrument_values_clear=True,
    )
    assert non_type_like[0] is True
    assert "non-type-like column" in non_type_like[1]

    reliable = streamlit_app._instrument_type_decision_context(
        instrument_type_column="Asset Type",
        instrument_values_clear=True,
    )
    assert reliable == (False, "Instrument Type mapping looks reliable (`Asset Type`).")


def test_delete_confirmation_ready_requires_checkbox_and_phrase():
    from portfolio_assistant.ui.streamlit import app as streamlit_app

    phrase = streamlit_app.DELETE_ACCOUNT_CONFIRMATION_PHRASE
    assert not streamlit_app._delete_confirmation_ready(phrase, False)
    assert not streamlit_app._delete_confirmation_ready("DELETE", True)
    assert streamlit_app._delete_confirmation_ready("delete account", True)
    assert streamlit_app._delete_confirmation_ready(f"  {phrase}  ", True)


def test_render_row_issues_routes_info_only_to_info(monkeypatch):
    from portfolio_assistant.ui.streamlit import app as streamlit_app

    calls: dict[str, list[str]] = {"info": [], "warning": [], "error": []}

    class _DummyCol:
        def dataframe(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(streamlit_app.st, "columns", lambda _spec: (_DummyCol(), _DummyCol()))
    monkeypatch.setattr(streamlit_app.st, "info", lambda value: calls["info"].append(str(value)))
    monkeypatch.setattr(
        streamlit_app.st,
        "warning",
        lambda value: calls["warning"].append(str(value)),
    )
    monkeypatch.setattr(streamlit_app.st, "error", lambda value: calls["error"].append(str(value)))

    streamlit_app._render_row_issues(
        ["Row 1: skipped non-filled row (quantity <= 0)"],
        "trade row issues",
    )

    assert calls["error"] == []
    assert calls["warning"] == []
    assert calls["info"] == ["1 informational trade row issues (expected skips)."]


def test_render_row_issues_surfaces_warning_and_error(monkeypatch):
    from portfolio_assistant.ui.streamlit import app as streamlit_app

    calls: dict[str, list[str]] = {"info": [], "warning": [], "error": []}

    class _DummyCol:
        def dataframe(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(streamlit_app.st, "columns", lambda _spec: (_DummyCol(), _DummyCol()))
    monkeypatch.setattr(streamlit_app.st, "info", lambda value: calls["info"].append(str(value)))
    monkeypatch.setattr(
        streamlit_app.st,
        "warning",
        lambda value: calls["warning"].append(str(value)),
    )
    monkeypatch.setattr(streamlit_app.st, "error", lambda value: calls["error"].append(str(value)))

    streamlit_app._render_row_issues(
        [
            "Mapping error: missing required field 'executed_at'",
            "Cash row 1: invalid amount",
        ],
        "cash row issues",
    )

    assert calls["info"] == []
    assert calls["warning"] == ["1 cash row issues. These rows were skipped."]
    assert calls["error"] == [
        "1 blocking cash row issues. Fix mapping or source-data errors before importing."
    ]


def test_render_readiness_panel_shows_pending_state(monkeypatch):
    from portfolio_assistant.ui.streamlit import app as streamlit_app

    progress_calls: list[float] = []
    caption_calls: list[str] = []
    success_calls: list[str] = []
    info_calls: list[str] = []

    monkeypatch.setattr(streamlit_app.st, "progress", lambda value: progress_calls.append(value))
    monkeypatch.setattr(streamlit_app.st, "caption", lambda value: caption_calls.append(str(value)))
    monkeypatch.setattr(streamlit_app.st, "success", lambda value: success_calls.append(str(value)))
    monkeypatch.setattr(streamlit_app.st, "info", lambda value: info_calls.append(str(value)))

    streamlit_app._render_readiness_panel(
        steps=[("CSV uploaded", True), ("Required mappings complete", False)],
        ready_label="ready",
        pending_label="pending",
    )

    assert progress_calls == [pytest.approx(0.5)]
    assert "Readiness 1/2 (50%)." in caption_calls
    assert "[done] CSV uploaded" in caption_calls
    assert "[pending] Required mappings complete" in caption_calls
    assert success_calls == []
    assert info_calls == ["pending"]


def test_render_readiness_panel_shows_ready_state(monkeypatch):
    from portfolio_assistant.ui.streamlit import app as streamlit_app

    progress_calls: list[float] = []
    success_calls: list[str] = []
    info_calls: list[str] = []

    monkeypatch.setattr(streamlit_app.st, "progress", lambda value: progress_calls.append(value))
    monkeypatch.setattr(streamlit_app.st, "caption", lambda _value: None)
    monkeypatch.setattr(streamlit_app.st, "success", lambda value: success_calls.append(str(value)))
    monkeypatch.setattr(streamlit_app.st, "info", lambda value: info_calls.append(str(value)))

    streamlit_app._render_readiness_panel(
        steps=[("CSV uploaded", True), ("Required mappings complete", True)],
        ready_label="ready",
        pending_label="pending",
    )

    assert progress_calls == [pytest.approx(1.0)]
    assert success_calls == ["ready"]
    assert info_calls == []


def _add_open_trade(session, account_id: str, symbol: str) -> None:
    session.add(
        TradeNormalized(
            account_id=account_id,
            broker="B1",
            executed_at=datetime(2025, 2, 15, 10, 0, 0),
            instrument_type="STOCK",
            symbol=symbol,
            side="BUY",
            quantity=3,
            price=120.0,
            fees=0.0,
            net_amount=-360.0,
            multiplier=1,
            currency="USD",
        )
    )
    session.flush()


def test_holdings_dataframe_respects_global_account_scope(
    db_session,
    seeded_two_account_activity,
):
    taxable_id = seeded_two_account_activity.taxable_id
    ira_id = seeded_two_account_activity.ira_id
    _add_open_trade(db_session, taxable_id, "NVDA")

    recompute_pnl(db_session)
    db_session.commit()

    all_frame = holdings_dataframe(db_session, None)
    taxable_frame = holdings_dataframe(db_session, taxable_id)
    ira_frame = holdings_dataframe(db_session, ira_id)

    assert set(all_frame["account_id"]) == {taxable_id, ira_id}
    assert set(taxable_frame["account_id"]) == {taxable_id}
    assert set(ira_frame["account_id"]) == {ira_id}


def test_pnl_dataframes_split_by_scope(db_session, seeded_two_account_activity):
    taxable_id = seeded_two_account_activity.taxable_id
    ira_id = seeded_two_account_activity.ira_id

    recompute_pnl(db_session)
    db_session.commit()

    all_summary = realized_summary_dataframe(db_session, None)
    taxable_summary = realized_summary_dataframe(db_session, taxable_id)
    ira_detail = realized_detail_dataframe(db_session, ira_id)

    assert {"AAPL", "MSFT", "QQQ"}.issubset(set(all_summary["symbol"]))
    assert set(taxable_summary["symbol"]) == {"AAPL", "MSFT"}
    assert set(ira_detail["account_id"]) == {ira_id}


def test_contributions_page_uses_external_cash_only(db_session, seeded_two_account_activity):
    taxable_id = seeded_two_account_activity.taxable_id
    ira_id = seeded_two_account_activity.ira_id

    monthly_all = monthly_contributions_dataframe(db_session, None)
    activity_all = external_cash_activity_dataframe(db_session, None)
    account_totals = account_contributions_dataframe(activity_all)

    assert float(monthly_all["net_contribution"].sum()) == 5550.0
    assert len(activity_all) == 4

    by_account = {
        row["account_id"]: float(row["net_contribution"])
        for row in account_totals.to_dict(orient="records")
    }
    assert by_account[taxable_id] == 4900.0
    assert by_account[ira_id] == 650.0


def test_tax_year_and_reconciliation_helpers(db_session, seeded_two_account_activity):
    recompute_pnl(db_session)
    db_session.commit()

    report = generate_tax_year_report(db_session, tax_year=2025)
    app_detail = tax_year_detail_dataframe(report)
    app_summary = tax_year_summary_dataframe(report["summary"])
    wash_matches = wash_sale_matches_dataframe(report)

    broker_raw = pd.DataFrame(
        [
            {
                "Symbol": "AAPL",
                "Sale Date": "2025-01-10",
                "Term": "SHORT",
                "Proceeds": 900.0,
                "Cost Basis": 1000.0,
                "Gain/Loss": -100.0,
                "Wash": 0.0,
            },
            {
                "Symbol": "MSFT",
                "Sale Date": "2025-01-12",
                "Term": "SHORT",
                "Proceeds": 275.0,
                "Cost Basis": 250.0,
                "Gain/Loss": 25.0,
                "Wash": 0.0,
            },
        ]
    )
    broker_mapping = {
        "symbol": "Symbol",
        "date_sold": "Sale Date",
        "term": "Term",
        "proceeds": "Proceeds",
        "cost_basis": "Cost Basis",
        "gain_or_loss": "Gain/Loss",
        "wash_sale_disallowed": "Wash",
    }
    broker_detail = normalize_broker_dataframe(broker_raw, broker_mapping)
    app_totals = tax_report_totals(report["detail_rows"])
    broker_totals = tax_report_totals(broker_detail.to_dict(orient="records"))
    comparison = compare_totals(app_totals, broker_totals)

    comparison_frame = comparison_dataframe(comparison)
    symbol_diff = diff_table_by_key(app_detail, broker_detail, "symbol")
    checklist = checklist_dataframe(report, comparison, app_detail)

    assert not app_detail.empty
    assert not app_summary.empty
    assert not wash_matches.empty
    assert not comparison_frame.empty
    assert "AAPL" in set(symbol_diff["symbol"])
    assert bool(
        checklist.loc[
            checklist["check"] == "Cross-account replacements likely",
            "flagged",
        ].iloc[0]
    )

    health = reconciliation_health(comparison_frame)
    assert not bool(health["in_sync"])
    assert int(health["mismatch_metrics"]) >= 1

    packet = build_reconciliation_packet_zip(
        app_summary_frame=app_summary,
        app_detail_frame=app_detail,
        comparison_frame=comparison_frame,
        checklist_frame=checklist,
        broker_detail_frame=broker_detail,
        symbol_diff=symbol_diff,
        date_diff=diff_table_by_key(app_detail, broker_detail, "date_sold"),
        term_diff=diff_table_by_key(app_detail, broker_detail, "term"),
    )
    assert packet[:2] == b"PK"


def test_settings_helpers_and_csv_bytes(db_session, seeded_two_account_activity):
    taxable_id = seeded_two_account_activity.taxable_id

    recompute_pnl(db_session)
    db_session.commit()

    metrics_all = settings_metrics(db_session, None)
    metrics_taxable = settings_metrics(db_session, taxable_id)

    from portfolio_assistant.assistant.tools_db import list_accounts

    catalog = account_catalog_dataframe(list_accounts(db_session))
    csv_bytes = dataframe_to_csv_bytes(catalog)

    assert metrics_all["normalized_trades"] > metrics_taxable["normalized_trades"]
    assert metrics_all["open_positions"] >= metrics_taxable["open_positions"]
    assert len(catalog) == 2
    assert csv_bytes.startswith(b"id,broker,account_label,account_type,display")
