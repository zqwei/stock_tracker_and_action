from __future__ import annotations

from datetime import date

from portfolio_assistant.ingest import pdf_import


def test_import_broker_1099b_pdf_parses_text_fallback(monkeypatch):
    payload = (
        b"AAPL 01/02/2024 01/05/2025 $1,250.00 $1,000.00 $50.00 $200.00 Short-Term\n"
    )

    monkeypatch.setattr(
        "portfolio_assistant.ingest.pdf_import._extract_with_pdfplumber",
        lambda _payload, broker: ([], [], ["forced fallback"]),
    )

    result = pdf_import.import_broker_1099b_pdf(payload, broker="B1")

    assert result.source == "pdf"
    assert result.needs_review is True
    assert "forced fallback" in result.issues
    assert len(result.rows) == 1

    row = result.rows[0]
    assert row["broker"] == "B1"
    assert row["symbol"] == "AAPL"
    assert row["date_acquired"] == date(2024, 1, 2)
    assert row["date_sold"] == date(2025, 1, 5)
    assert row["proceeds"] == 1250.0
    assert row["cost_basis"] == 1000.0
    assert row["wash_sale_disallowed"] == 50.0
    assert row["gain_or_loss"] == 200.0
    assert row["term"] == "ST"


def test_extract_1099b_rows_from_text_computes_gain_when_missing_gain_column():
    rows, issues = pdf_import.extract_1099b_rows_from_text(
        [
            "MSFT 2024-02-01 2025-02-03 500.00 450.00 LT",
        ],
        broker="B1",
    )

    assert issues == []
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "MSFT"
    assert row["proceeds"] == 500.0
    assert row["cost_basis"] == 450.0
    assert row["wash_sale_disallowed"] == 0.0
    assert row["gain_or_loss"] == 50.0
    assert row["term"] == "LT"


def test_import_broker_1099b_pdf_prefers_table_rows_when_available(monkeypatch):
    table_row = {
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

    monkeypatch.setattr(
        "portfolio_assistant.ingest.pdf_import._extract_with_pdfplumber",
        lambda _payload, broker: ([{**table_row, "broker": broker}], ["ignored"], ["table parser"]),
    )

    result = pdf_import.import_broker_1099b_pdf(b"%PDF-1.4 fake", broker="B1")

    assert result.needs_review is True
    assert result.rows == [{**table_row, "broker": "B1"}]
    assert result.issues == ["table parser"]
