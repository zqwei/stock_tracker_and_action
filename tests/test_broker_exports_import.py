from __future__ import annotations

from datetime import date

import pandas as pd

from portfolio_assistant.ingest.broker_exports_import import (
    broker_export_totals,
    import_reconciliation_inputs,
    normalize_broker_export_records,
    validate_broker_export_mapping,
)
from portfolio_assistant.ingest.pdf_import import PdfImportResult


def test_normalize_broker_export_records_infers_columns_and_values():
    df = pd.DataFrame(
        [
            {
                "Description": "AAPL common stock",
                "Symbol": "AAPL",
                "Date Acquired": "01/02/2024",
                "Date Sold": "01/05/2025",
                "Proceeds": "$1,250.00",
                "Cost Basis": "$1,000.00",
                "Wash Sale Disallowed": "$50.00",
                "Term": "Short-Term",
            }
        ]
    )

    rows, issues = normalize_broker_export_records(df, mapping=None, broker="B1")

    assert issues == []
    assert len(rows) == 1
    row = rows[0]
    assert row["broker"] == "B1"
    assert row["symbol"] == "AAPL"
    assert row["date_acquired"] == date(2024, 1, 2)
    assert row["date_sold"] == date(2025, 1, 5)
    assert row["proceeds"] == 1250.0
    assert row["cost_basis"] == 1000.0
    assert row["wash_sale_disallowed"] == 50.0
    assert row["gain_or_loss"] == 250.0
    assert row["term"] == "ST"


def test_normalize_broker_export_records_handles_realized_activity_schema():
    df = pd.DataFrame(
        [
            {
                "Buy/Sell": "BUY",
                "Symbol": "MARA",
                "Description": "MARA HOLDINGS INC",
                "Trade Date": "01/03/2024",
                "Unit Price": "35.0",
                "Quantity": "1000",
                "Total Cost": "24670.51",
                "Short Term Gain/Loss": "",
                "Long Term Gain/Loss": "",
                "Long/Short Position": "LONG",
                "Disallowed Loss": "0.0",
            },
            {
                "Buy/Sell": "SELL",
                "Symbol": "MARA",
                "Description": "MARA HOLDINGS INC",
                "Trade Date": "01/10/2025",
                "Unit Price": "45.0",
                "Quantity": "-10",
                "Total Cost": "450.00",
                "Short Term Gain/Loss": "50.0",
                "Long Term Gain/Loss": "0.0",
                "Long/Short Position": "SHORT",
                "Disallowed Loss": "0.0",
            },
        ]
    )

    rows, issues = normalize_broker_export_records(df, mapping=None, broker="B1")

    assert issues == []
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "MARA"
    assert row["date_sold"] == date(2025, 1, 10)
    assert row["term"] == "ST"
    assert row["proceeds"] == 450.0
    assert row["gain_or_loss"] == 50.0
    assert row["cost_basis"] == 400.0


def test_validate_broker_export_mapping_resolves_normalized_source_column_names():
    mapping = {
        "version": 1,
        "mapping_kind": "broker_tax_export",
        "name": "Normalized mapping",
        "broker": "Example",
        "output_schema": {"canonical_row_version": 1, "fields": ["date_sold", "proceeds", "cost_basis"]},
        "columns": {
            " date sold ": {"field": "date_sold", "type": "date", "required": True},
            " proceeds ": {"field": "proceeds", "type": "money", "required": True},
            " cost basis ": {"field": "cost_basis", "type": "money", "required": True},
            " gain/loss ": {
                "field": "gain_or_loss",
                "type": "string",
                "transform": "normalize_term",
                "required": False,
            },
        },
    }
    csv_columns = ["Date Sold", "Proceeds", "Cost Basis", "Gain/Loss"]

    normalized_mapping, errors = validate_broker_export_mapping(mapping, columns=csv_columns)

    assert errors == []
    # Required fields still resolve through normalized column names.
    assert normalized_mapping["columns"]
    resolved_by_field = {
        rule["field"]: rule["source_column"]
        for rule in normalized_mapping["columns"]
    }
    assert resolved_by_field["date_sold"] == "Date Sold"
    assert resolved_by_field["proceeds"] == "Proceeds"
    assert resolved_by_field["cost_basis"] == "Cost Basis"


def test_validate_broker_export_mapping_resolves_token_subset_source_column_names():
    mapping = {
        "version": 1,
        "mapping_kind": "broker_tax_export",
        "name": "Token subset mapping",
        "broker": "Example",
        "output_schema": {"canonical_row_version": 1, "fields": ["date_sold", "proceeds", "cost_basis"]},
        "columns": {
            "date sold": {"field": "date_sold", "type": "date", "required": True},
            "sales proceeds": {"field": "proceeds", "type": "money", "required": True},
            "cost basis": {"field": "cost_basis", "type": "money", "required": True},
        },
    }
    csv_columns = ["Date Sold (MM/DD/YYYY)", "Sales Proceeds USD", "Cost Basis Amount"]

    normalized_mapping, errors = validate_broker_export_mapping(mapping, columns=csv_columns)

    assert errors == []
    resolved_by_field = {
        rule["field"]: rule["source_column"]
        for rule in normalized_mapping["columns"]
    }
    assert resolved_by_field["date_sold"] == "Date Sold (MM/DD/YYYY)"
    assert resolved_by_field["proceeds"] == "Sales Proceeds USD"
    assert resolved_by_field["cost_basis"] == "Cost Basis Amount"


def test_validate_broker_export_mapping_reports_ambiguous_token_subset_source_column_names():
    mapping = {
        "version": 1,
        "mapping_kind": "broker_tax_export",
        "name": "Ambiguous token mapping",
        "broker": "Example",
        "output_schema": {"canonical_row_version": 1, "fields": ["date_sold", "proceeds", "cost_basis"]},
        "columns": {
            "date sold": {"field": "date_sold", "type": "date", "required": True},
            "proceeds": {"field": "proceeds", "type": "money", "required": True},
            "cost basis": {"field": "cost_basis", "type": "money", "required": True},
        },
    }
    csv_columns = ["Date Sold Local", "Date Sold UTC", "Proceeds", "Cost Basis"]

    _normalized_mapping, errors = validate_broker_export_mapping(mapping, columns=csv_columns)

    assert any("matches multiple CSV columns by tokens" in error for error in errors), errors


def test_broker_export_totals_sums_by_term():
    rows = [
        {
            "proceeds": 150.0,
            "cost_basis": 100.0,
            "gain_or_loss": 50.0,
            "wash_sale_disallowed": 10.0,
            "term": "ST",
        },
        {
            "proceeds": 200.0,
            "cost_basis": 280.0,
            "gain_or_loss": -80.0,
            "wash_sale_disallowed": 0.0,
            "term": "LT",
        },
    ]

    totals = broker_export_totals(rows)

    assert totals["total_proceeds"] == 350.0
    assert totals["total_cost_basis"] == 380.0
    assert totals["total_gain_or_loss"] == -30.0
    assert totals["total_wash_sale_disallowed"] == 10.0
    assert totals["short_term_gain_or_loss"] == 50.0
    assert totals["long_term_gain_or_loss"] == -80.0


def test_import_reconciliation_inputs_falls_back_to_pdf(monkeypatch, tmp_path):
    csv_path = tmp_path / "broker.csv"
    csv_path.write_text("Unknown,Cols\nx,y\n", encoding="utf-8")
    pdf_path = tmp_path / "broker.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake content")

    fallback_rows = [
        {
            "description": "AAPL",
            "symbol": "AAPL",
            "date_acquired": date(2024, 1, 2),
            "date_sold": date(2025, 1, 5),
            "proceeds": 100.0,
            "cost_basis": 90.0,
            "wash_sale_disallowed": 0.0,
            "gain_or_loss": 10.0,
            "term": "ST",
            "currency": "USD",
            "broker": "B1",
        }
    ]

    def _fake_pdf_import(_file_obj, *, broker: str = "generic") -> PdfImportResult:
        return PdfImportResult(
            rows=[{**fallback_rows[0], "broker": broker}],
            issues=["pdf parsed"],
            needs_review=True,
            source="pdf",
        )

    monkeypatch.setattr(
        "portfolio_assistant.ingest.pdf_import.import_broker_1099b_pdf",
        _fake_pdf_import,
    )

    result = import_reconciliation_inputs(
        csv_file=csv_path,
        pdf_file=pdf_path,
        broker="B1",
    )

    assert result.source == "pdf"
    assert result.used_pdf_fallback is True
    assert result.needs_review is True
    assert len(result.rows) == 1
    assert result.rows[0]["broker"] == "B1"
    assert any("Mapping error" in issue for issue in result.issues)
    assert "pdf parsed" in result.issues
