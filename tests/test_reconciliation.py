from __future__ import annotations

from math import isclose

from portfolio_assistant.analytics.reconciliation import (
    broker_vs_irs_diffs,
    build_app_vs_broker_diff_tables,
    build_broker_vs_irs_reconciliation,
    build_reconciliation_checklist,
)


def test_broker_vs_irs_diffs_include_trade_symbol_date_and_term_groupings():
    detail_rows = [
        {
            "sale_row_id": 1,
            "symbol": "AAPL",
            "date_sold": "2025-01-10",
            "term": "SHORT",
            "raw_gain_or_loss": -100.0,
            "wash_sale_disallowed_broker": 0.0,
            "wash_sale_disallowed_irs": 70.0,
            "gain_or_loss": -30.0,
        },
        {
            "sale_row_id": 2,
            "symbol": "MSFT",
            "date_sold": "2025-02-01",
            "term": "LONG",
            "raw_gain_or_loss": 20.0,
            "wash_sale_disallowed_broker": 0.0,
            "wash_sale_disallowed_irs": 0.0,
            "gain_or_loss": 20.0,
        },
    ]

    diffs = broker_vs_irs_diffs(detail_rows)
    assert diffs["totals"]["rows"] == 2
    assert isclose(float(diffs["totals"]["gain_or_loss_delta"]), 70.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(
        float(diffs["totals"]["wash_sale_disallowed_delta"]), 70.0, rel_tol=0.0, abs_tol=1e-9
    )

    by_symbol = {row["symbol"]: row for row in diffs["by_symbol"]}
    assert isclose(float(by_symbol["AAPL"]["gain_or_loss_delta"]), 70.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(
        float(by_symbol["AAPL"]["wash_sale_disallowed_delta"]), 70.0, rel_tol=0.0, abs_tol=1e-9
    )

    by_term = {row["term"]: row for row in diffs["by_term"]}
    assert isclose(float(by_term["SHORT"]["gain_or_loss_delta"]), 70.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(by_term["LONG"]["gain_or_loss_delta"]), 0.0, rel_tol=0.0, abs_tol=1e-9)


def test_reconciliation_checklist_flags_boundary_cross_account_options_and_corporate_actions():
    report = {
        "summary": {"tax_year": 2025},
        "detail_rows": [
            {
                "sale_row_id": 1,
                "symbol": "AAPL",
                "description": "AAPL",
                "date_sold": "2025-12-31",
                "term": "SHORT",
                "raw_gain_or_loss": -100.0,
                "wash_sale_disallowed_broker": 0.0,
                "wash_sale_disallowed_irs": 70.0,
                "gain_or_loss": -30.0,
            },
            {
                "sale_row_id": 2,
                "symbol": "TSLA",
                "description": "TSLA split adjustment",
                "date_sold": "2025-06-15",
                "term": "SHORT",
                "raw_gain_or_loss": 10.0,
                "wash_sale_disallowed_broker": 0.0,
                "wash_sale_disallowed_irs": 0.0,
                "gain_or_loss": 10.0,
            },
        ],
        "wash_sale_summary": {
            "irs": {
                "sales": [
                    {
                        "sale_row_id": 1,
                        "symbol": "AAPL",
                        "sale_date": "2025-12-31",
                        "matches": [
                            {
                                "buy_date": "2026-01-05",
                                "days_from_sale": 5,
                                "cross_account": True,
                                "ira_replacement": False,
                                "buy_instrument_type": "OPTION",
                            }
                        ],
                    }
                ]
            }
        },
    }

    mode_diffs = broker_vs_irs_diffs(report["detail_rows"])
    checklist = build_reconciliation_checklist(report, mode_diffs=mode_diffs)
    by_key = {row["key"]: row for row in checklist}

    assert by_key["missing_boundary_data"]["flag"]
    assert by_key["cross_account_replacements_likely"]["flag"]
    assert by_key["options_replacements_likely"]["flag"]
    assert by_key["corporate_actions_present"]["flag"]
    assert not by_key["lot_method_mismatch"]["flag"]

    bundled = build_broker_vs_irs_reconciliation(report)
    assert "mode_diffs" in bundled
    assert "checklist" in bundled
    assert bundled["checklist"]


def test_app_vs_broker_diff_tables_support_symbol_date_and_term_drilldowns():
    app_rows = [
        {
            "symbol": "AAPL",
            "date_sold": "2025-01-10",
            "term": "SHORT",
            "proceeds": 900.0,
            "cost_basis": 1000.0,
            "gain_or_loss": -30.0,
            "wash_sale_disallowed": 70.0,
        },
        {
            "symbol": "MSFT",
            "date_sold": "2025-02-01",
            "term": "LONG",
            "proceeds": 200.0,
            "cost_basis": 150.0,
            "gain_or_loss": 50.0,
            "wash_sale_disallowed": 0.0,
        },
    ]
    broker_rows = [
        {
            "symbol": "AAPL",
            "date_sold": "2025-01-10",
            "term": "SHORT",
            "proceeds": 900.0,
            "cost_basis": 1000.0,
            "gain_or_loss": -100.0,
            "wash_sale_disallowed": 0.0,
        }
    ]

    diffs = build_app_vs_broker_diff_tables(app_rows, broker_rows)
    by_symbol = {row["symbol"]: row for row in diffs["by_symbol"]}
    assert isclose(
        float(by_symbol["AAPL"]["delta_gain_or_loss"]),
        70.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(
        float(by_symbol["AAPL"]["delta_wash_sale_disallowed"]),
        70.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(
        float(by_symbol["MSFT"]["delta_gain_or_loss"]),
        50.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )


def test_reconciliation_checklist_infers_boundary_warning_from_mode_deltas():
    report = {
        "summary": {"tax_year": 2025},
        "detail_rows": [
            {
                "sale_row_id": 10,
                "symbol": "AAPL",
                "description": "AAPL",
                "date_sold": "2025-12-29",
                "term": "SHORT",
                "raw_gain_or_loss": -100.0,
                "wash_sale_disallowed_broker": 0.0,
                "wash_sale_disallowed_irs": 70.0,
                "gain_or_loss": -30.0,
            }
        ],
        "wash_sale_summary": {"irs": {"sales": []}},
    }

    mode_diffs = broker_vs_irs_diffs(report["detail_rows"])
    checklist = build_reconciliation_checklist(report, mode_diffs=mode_diffs)
    by_key = {row["key"]: row for row in checklist}
    boundary = by_key["missing_boundary_data"]
    assert boundary["flag"]
    assert boundary["signal_count"] == 1
    assert "Boundary-period sales plus material broker-vs-IRS deltas" in boundary["reason"]


def test_reconciliation_checklist_mentions_partial_replacement_context():
    report = {
        "summary": {"tax_year": 2025},
        "detail_rows": [
            {
                "sale_row_id": 20,
                "symbol": "AAPL",
                "description": "AAPL",
                "date_sold": "2025-12-15",
                "term": "SHORT",
                "raw_gain_or_loss": -100.0,
                "wash_sale_disallowed_broker": 0.0,
                "wash_sale_disallowed_irs": 50.0,
                "gain_or_loss": -50.0,
            }
        ],
        "wash_sale_summary": {
            "irs": {
                "sales": [
                    {
                        "sale_row_id": 20,
                        "symbol": "AAPL",
                        "sale_date": "2025-12-15",
                        "sale_quantity_equiv": 10.0,
                        "matched_replacement_quantity_equiv": 5.0,
                        "matches": [],
                    }
                ]
            }
        },
        "year_boundary_diagnostics": {
            "partial_replacement_sale_count": 1,
            "partial_replacement_unmatched_quantity_equiv_total": 5.0,
        },
    }

    mode_diffs = broker_vs_irs_diffs(report["detail_rows"])
    checklist = build_reconciliation_checklist(report, mode_diffs=mode_diffs)
    by_key = {row["key"]: row for row in checklist}

    lot_method = by_key["lot_method_mismatch"]
    assert not lot_method["flag"]
    assert "Partial replacement patterns detected on 1 sale(s)." in lot_method["reason"]
    assert "Unmatched replacement quantity: 5 share-equivalent." in lot_method["reason"]


def test_reconciliation_checklist_boundary_reason_includes_carryover_context():
    report = {
        "summary": {"tax_year": 2025},
        "detail_rows": [
            {
                "sale_row_id": 30,
                "symbol": "AAPL",
                "description": "AAPL",
                "date_sold": "2025-12-31",
                "term": "SHORT",
                "raw_gain_or_loss": -100.0,
                "wash_sale_disallowed_broker": 0.0,
                "wash_sale_disallowed_irs": 30.0,
                "gain_or_loss": -70.0,
            }
        ],
        "wash_sale_summary": {
            "irs": {
                "sales": [
                    {
                        "sale_row_id": 30,
                        "symbol": "AAPL",
                        "sale_date": "2025-12-31",
                        "matches": [
                            {
                                "buy_date": "2024-12-28",
                                "days_from_sale": -3,
                                "cross_account": False,
                                "ira_replacement": False,
                                "buy_instrument_type": "STOCK",
                            },
                            {
                                "buy_date": "2026-01-15",
                                "days_from_sale": 15,
                                "cross_account": False,
                                "ira_replacement": False,
                                "buy_instrument_type": "STOCK",
                            },
                        ],
                    }
                ]
            }
        },
        "year_boundary_diagnostics": {
            "disallowed_loss_allocated_to_prior_year_replacements": 10.0,
            "disallowed_loss_allocated_to_next_year_or_later_replacements": 20.0,
        },
    }

    mode_diffs = broker_vs_irs_diffs(report["detail_rows"])
    checklist = build_reconciliation_checklist(report, mode_diffs=mode_diffs)
    by_key = {row["key"]: row for row in checklist}

    boundary = by_key["missing_boundary_data"]
    assert boundary["flag"]
    assert "Year-boundary context" in boundary["reason"]
    assert "pre-2025 replacement buys" in boundary["reason"]
    assert "post-2025 replacement buys" in boundary["reason"]
