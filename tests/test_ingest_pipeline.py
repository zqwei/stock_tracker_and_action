from __future__ import annotations

import json
from datetime import datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from portfolio_assistant.assistant.tools_db import (
    delete_account_if_empty,
    insert_cash_activity,
    insert_trade_import,
)
from portfolio_assistant.db.models import (
    Account,
    Base,
    CashActivity,
    PnlRealized,
    PositionOpen,
    ReconciliationArtifact,
    ReconciliationRun,
    TradeNormalized,
    TradeRaw,
    WashSaleAdjustment,
)
from portfolio_assistant.ingest.csv_import import (
    normalize_cash_records,
    normalize_trade_records,
    parse_import_issue,
)
from portfolio_assistant.ingest.csv_mapping import (
    get_saved_trade_mapping,
    infer_trade_column_map,
    save_trade_mapping,
    suggest_trade_column_candidates,
    trade_mapping_hints,
    validate_mapping,
)


def _setup_account(session: Session) -> Account:
    account = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
    session.add(account)
    session.flush()
    return account


def _trade_import_rows(account_id: str, start: int, count: int) -> tuple[list[dict], list[dict]]:
    raw_rows: list[dict] = []
    normalized_rows: list[dict] = []
    for idx in range(start, start + count):
        executed_at = datetime(2025, 1, 1, 9, 30, 0) + timedelta(minutes=idx)
        raw_rows.append(
            {
                "Date": executed_at.isoformat(),
                "Type": "STOCK",
                "Side": "BUY",
                "Qty": "1",
                "Price": "10",
                "Symbol": "AAPL",
            }
        )
        normalized_rows.append(
            {
                "account_id": account_id,
                "broker": "B1",
                "trade_id": f"T-{idx}",
                "executed_at": executed_at,
                "instrument_type": "STOCK",
                "symbol": "AAPL",
                "side": "BUY",
                "option_symbol_raw": None,
                "underlying": None,
                "expiration": None,
                "strike": None,
                "call_put": None,
                "multiplier": 1,
                "quantity": 1.0,
                "price": 10.0,
                "fees": 0.0,
                "net_amount": -10.0,
                "currency": "USD",
            }
        )
    return raw_rows, normalized_rows


def _cash_rows(account_id: str, start: int, count: int) -> list[dict]:
    rows: list[dict] = []
    for idx in range(start, start + count):
        posted_at = datetime(2025, 1, 5, 12, 0, 0) + timedelta(minutes=idx)
        rows.append(
            {
                "account_id": account_id,
                "broker": "B1",
                "posted_at": posted_at,
                "activity_type": "DEPOSIT",
                "amount": 100.0,
                "description": f"ACH deposit {idx}",
                "source": "ACH",
                "is_external": True,
            }
        )
    return rows


def test_normalize_trade_records_rejects_duplicate_source_mapping():
    df = pd.DataFrame(
        [
            {
                "Date": "2025-01-05",
                "Type": "STOCK",
                "Action": "BUY",
                "Qty": "1",
                "Price": "100",
            }
        ]
    )
    mapping = {
        "executed_at": "Date",
        "instrument_type": "Action",
        "side": "Action",
        "quantity": "Qty",
        "price": "Price",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
    )

    assert rows == []
    assert any("multiple fields" in issue for issue in issues), issues
    severities = [parse_import_issue(issue)[0] for issue in issues]
    assert severities and all(severity == "ERROR" for severity in severities)


def test_infer_trade_column_map_webull_prefers_filled_time_avg_price_and_filled_qty():
    columns = [
        "Name",
        "Symbol",
        "Side",
        "Status",
        "Filled",
        "Total Qty",
        "Price",
        "Avg Price",
        "Placed Time",
        "Filled Time",
    ]

    mapping = infer_trade_column_map(columns, broker="webull")

    assert mapping["executed_at"] == "Filled Time"
    assert mapping["quantity"] == "Filled"
    assert mapping["price"] == "Avg Price"
    # Webull exports often omit explicit "Type"; we should not force-map it to symbol/name.
    assert "instrument_type" not in mapping

    hints = trade_mapping_hints(columns, broker="webull")
    assert any("Filled Time" in hint for hint in hints)
    assert any("Avg Price" in hint for hint in hints)


def test_infer_trade_column_map_handles_token_variant_headers():
    columns = [
        "Trade Date / Time",
        "Buy Or Sell",
        "Filled Quantity",
        "Average Fill Price",
        "Ticker Symbol",
    ]

    mapping = infer_trade_column_map(columns, broker="generic")

    assert mapping["executed_at"] == "Trade Date / Time"
    assert mapping["side"] == "Buy Or Sell"
    assert mapping["quantity"] == "Filled Quantity"
    assert mapping["price"] == "Average Fill Price"
    assert mapping["symbol"] == "Ticker Symbol"


def test_normalize_trade_records_handles_webull_option_symbol_and_timezone():
    df = pd.DataFrame(
        [
            {
                "Name": "TQQQ251121C00140000",
                "Symbol": "TQQQ251121C00140000",
                "Side": "Sell",
                "Status": "Filled",
                "Filled": "3",
                "Total Qty": "3",
                "Price": "@0.5200000000",
                "Avg Price": "0.5200000000",
                "Placed Time": "09/22/2025 09:45:29 EDT",
                "Filled Time": "09/22/2025 09:46:31 EDT",
            }
        ]
    )
    mapping = infer_trade_column_map(list(df.columns), broker="webull")

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-webull",
        broker="webull",
    )

    assert issues == []
    assert len(rows) == 1
    row = rows[0]
    assert row["instrument_type"] == "OPTION"
    assert row["symbol"] == "TQQQ"
    assert row["underlying"] == "TQQQ"
    assert row["call_put"] == "C"
    assert row["strike"] == 140.0
    assert row["quantity"] == 3.0
    assert row["price"] == 0.52
    assert row["option_symbol_raw"] == "TQQQ251121C00140000"
    assert row["executed_at"].tzinfo is None


def test_normalize_trade_records_accepts_punctuation_variant_mapping_source_names():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-05",
                "Buy / Sell": "BUY",
                "Qty.": "2",
                "Unit Price($)": "100",
                "Ticker Symbol": "AAPL",
            }
        ]
    )
    mapping = {
        "executed_at": "trade_date",
        "side": "buy_sell",
        "quantity": "qty",
        "price": "unit_price",
        "symbol": "ticker_symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
        default_instrument_type="STOCK",
    )

    assert issues == []
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["side"] == "BUY"
    assert rows[0]["quantity"] == 2.0


def test_normalize_trade_records_accepts_token_subset_mapping_source_names():
    df = pd.DataFrame(
        [
            {
                "Trade Date / Time": "2025-01-05 10:35:00",
                "Buy Or Sell": "BUY",
                "Filled Quantity": "3",
                "Average Fill Price": "12.5",
                "Ticker Symbol": "NVDA",
            }
        ]
    )
    mapping = {
        "executed_at": "trade date",
        "side": "buy sell",
        "quantity": "quantity",
        "price": "avg price",
        "symbol": "ticker symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
        default_instrument_type="STOCK",
    )

    assert issues == []
    assert len(rows) == 1
    assert rows[0]["symbol"] == "NVDA"
    assert rows[0]["price"] == 12.5


def test_normalize_trade_records_marks_non_filled_skip_as_info():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-05",
                "Buy/Sell": "BUY",
                "Quantity": "0",
                "Unit Price": "25.50",
                "Symbol": "AAPL",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "symbol": "Symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
        default_instrument_type="STOCK",
    )

    assert rows == []
    assert len(issues) == 1
    severity, message = parse_import_issue(issues[0])
    assert severity == "INFO"
    assert "skipped non-filled row" in message


def test_normalize_trade_records_marks_malformed_row_as_warning():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "",
                "Buy/Sell": "BUY",
                "Quantity": "1",
                "Unit Price": "25.50",
                "Symbol": "AAPL",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "symbol": "Symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
        default_instrument_type="STOCK",
    )

    assert rows == []
    assert len(issues) == 1
    severity, message = parse_import_issue(issues[0])
    assert severity == "WARNING"
    assert "invalid executed_at" in message


def test_validate_mapping_reports_ambiguous_token_subset_source_name():
    columns = ["Trade Date Local", "Trade Date UTC", "Side", "Qty", "Price"]
    mapping = {
        "executed_at": "trade date",
        "side": "side",
        "quantity": "qty",
        "price": "price",
    }

    cleaned, errors = validate_mapping(
        mapping,
        columns=columns,
        canonical_fields=["executed_at", "side", "quantity", "price"],
        required_fields=["executed_at", "side", "quantity", "price"],
    )

    assert cleaned["side"] == "Side"
    assert cleaned["quantity"] == "Qty"
    assert cleaned["price"] == "Price"
    assert any("matches multiple CSV columns" in error for error in errors), errors


def test_suggest_trade_column_candidates_returns_best_matches_for_missing_required_fields():
    columns = ["Trade Date", "Buy / Sell", "Filled Qty", "Average Fill Price", "Ticker Symbol"]

    side_candidates = suggest_trade_column_candidates(columns, "side", broker="generic")
    quantity_candidates = suggest_trade_column_candidates(columns, "quantity", broker="generic")
    price_candidates = suggest_trade_column_candidates(columns, "price", broker="generic")

    assert side_candidates[0] == "Buy / Sell"
    assert quantity_candidates[0] == "Filled Qty"
    assert price_candidates[0] == "Average Fill Price"


def test_parse_import_issue_defaults_to_warning_for_legacy_text():
    severity, message = parse_import_issue("Row 1: invalid executed_at")
    assert severity == "WARNING"
    assert message == "Row 1: invalid executed_at"


def test_normalize_cash_records_requires_positive_amount():
    df = pd.DataFrame(
        [
            {
                "Date": "2025-01-05",
                "Type": "DEPOSIT",
                "Amount": "0",
            }
        ]
    )
    mapping = {
        "posted_at": "Date",
        "activity_type": "Type",
        "amount": "Amount",
    }

    rows, issues = normalize_cash_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
    )

    assert rows == []
    assert any("amount must be > 0" in issue for issue in issues), issues
    severities = [parse_import_issue(issue)[0] for issue in issues]
    assert severities and all(severity == "WARNING" for severity in severities)


def test_save_trade_mapping_validates_and_roundtrips(tmp_path, monkeypatch):
    mapping_path = tmp_path / "mappings" / "trade_column_mappings.json"
    monkeypatch.setattr(
        "portfolio_assistant.ingest.csv_mapping._mapping_store_path",
        lambda: mapping_path,
    )

    columns = ["Date", "Type", "Side", "Qty", "Price"]
    invalid_mapping = {
        "executed_at": "Date",
        "instrument_type": "Side",
        "side": "Side",
        "quantity": "Qty",
        "price": "Price",
    }
    with pytest.raises(ValueError):
        save_trade_mapping(
            broker="webull",
            signature="sig1",
            columns=columns,
            mapping=invalid_mapping,
        )

    valid_mapping = {
        "executed_at": "Date",
        "instrument_type": "Type",
        "side": "Side",
        "quantity": "Qty",
        "price": "Price",
    }
    save_trade_mapping(
        broker="webull",
        signature="sig2",
        columns=columns,
        mapping=valid_mapping,
    )
    loaded = get_saved_trade_mapping("webull", "sig2")
    assert loaded == valid_mapping


def test_save_trade_mapping_requires_required_fields(tmp_path, monkeypatch):
    mapping_path = tmp_path / "mappings" / "trade_column_mappings.json"
    monkeypatch.setattr(
        "portfolio_assistant.ingest.csv_mapping._mapping_store_path",
        lambda: mapping_path,
    )

    columns = ["Type", "Side", "Qty", "Price"]
    missing_required = {
        "instrument_type": "Type",
        "side": "Side",
        "quantity": "Qty",
        "price": "Price",
    }
    with pytest.raises(ValueError) as exc_info:
        save_trade_mapping(
            broker="webull",
            signature="sig-missing",
            columns=columns,
            mapping=missing_required,
        )
    assert "Missing required field mapping 'executed_at'" in str(exc_info.value)


def test_normalize_trade_records_uses_default_instrument_type_when_missing_column():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-05",
                "Buy/Sell": "BUY",
                "Quantity": "10",
                "Unit Price": "25.50",
                "Symbol": "AAPL",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "symbol": "Symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
        default_instrument_type="STOCK",
    )

    assert issues == []
    assert len(rows) == 1
    assert rows[0]["instrument_type"] == "STOCK"
    assert rows[0]["side"] == "BUY"
    assert rows[0]["price"] == 25.50


def test_normalize_trade_records_requires_explicit_default_when_instrument_type_is_ambiguous():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-05",
                "Buy/Sell": "BUY",
                "Quantity": "10",
                "Unit Price": "25.50",
                "Symbol": "AAPL",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "symbol": "Symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
    )

    assert rows == []
    assert len(issues) == 1
    severity, message = parse_import_issue(issues[0])
    assert severity == "WARNING"
    assert "instrument type is missing or ambiguous" in message


def test_normalize_trade_records_uses_default_instrument_type_for_unclear_mapped_values():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-05",
                "Type-Like": "AAPL",
                "Buy/Sell": "SELL",
                "Quantity": "5",
                "Unit Price": "10.00",
                "Symbol": "AAPL",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "instrument_type": "Type-Like",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "symbol": "Symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
        default_instrument_type="OPTION",
    )

    assert issues == []
    assert len(rows) == 1
    assert rows[0]["instrument_type"] == "OPTION"
    assert rows[0]["side"] == "SELL"
    assert rows[0]["multiplier"] == 100


def test_normalize_trade_records_keeps_explicit_option_variant_when_default_is_stock():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-05",
                "Type": "Equity Option",
                "Buy/Sell": "SELL",
                "Quantity": "1",
                "Unit Price": "4.2",
                "Option Symbol": "AAPL250221C00195000",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "instrument_type": "Type",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "option_symbol_raw": "Option Symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-1",
        broker="generic",
        default_instrument_type="STOCK",
    )

    assert issues == []
    assert len(rows) == 1
    assert rows[0]["instrument_type"] == "OPTION"
    assert rows[0]["symbol"] == "AAPL"


def test_normalize_trade_records_infers_fees_from_total_cost_for_options():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-10",
                "Buy/Sell": "BUY",
                "Quantity": "1",
                "Unit Price": "2.50",
                "Total Cost": "255.00",
                "Option Symbol": "AAPL250117C00150000",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "total_cost": "Total Cost",
        "option_symbol_raw": "Option Symbol",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-opt",
        broker="generic",
    )

    assert issues == []
    assert len(rows) == 1
    assert rows[0]["instrument_type"] == "OPTION"
    assert rows[0]["fees"] == pytest.approx(5.0, rel=1e-9)
    assert rows[0]["net_amount"] == pytest.approx(-255.0, rel=1e-9)


def test_normalize_trade_records_does_not_force_option_from_non_option_raw_symbol():
    df = pd.DataFrame(
        [
            {
                "Trade Date": "2025-01-10",
                "Buy/Sell": "SELL",
                "Quantity": "2",
                "Unit Price": "100",
                "Symbol": "MSFT",
                "Option Raw": "MSFT",
            }
        ]
    )
    mapping = {
        "executed_at": "Trade Date",
        "side": "Buy/Sell",
        "quantity": "Quantity",
        "price": "Unit Price",
        "symbol": "Symbol",
        "option_symbol_raw": "Option Raw",
    }

    rows, issues = normalize_trade_records(
        df=df,
        mapping=mapping,
        account_id="acc-stock",
        broker="generic",
        default_instrument_type="STOCK",
    )

    assert issues == []
    assert len(rows) == 1
    assert rows[0]["instrument_type"] == "STOCK"
    assert rows[0]["side"] == "SELL"


def test_save_trade_mapping_normalizes_column_name_matching(tmp_path, monkeypatch):
    mapping_path = tmp_path / "mappings" / "trade_column_mappings.json"
    monkeypatch.setattr(
        "portfolio_assistant.ingest.csv_mapping._mapping_store_path",
        lambda: mapping_path,
    )

    columns = [" Trade Date ", "Type", "SIDE", "Qty", "Price"]
    mapping = {
        "executed_at": "trade_date",
        "instrument_type": "type",
        "side": "side",
        "quantity": "QTY",
        "price": " price ",
    }
    save_trade_mapping(
        broker="webull",
        signature="sig-normalized",
        columns=columns,
        mapping=mapping,
    )

    loaded = get_saved_trade_mapping("webull", "sig-normalized")
    assert loaded == {
        "executed_at": " Trade Date ",
        "instrument_type": "Type",
        "side": "SIDE",
        "quantity": "Qty",
        "price": "Price",
    }


def test_get_saved_trade_mapping_ignores_invalid_store_record(tmp_path, monkeypatch):
    mapping_path = tmp_path / "mappings" / "trade_column_mappings.json"
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    mapping_path.write_text(
        json.dumps(
            {
                "webull::sig-bad": {
                    "columns": "not-a-list",
                    "mapping": {"executed_at": "Date"},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "portfolio_assistant.ingest.csv_mapping._mapping_store_path",
        lambda: mapping_path,
    )

    assert get_saved_trade_mapping("webull", "sig-bad") is None


def test_insert_trade_import_dedupes_reimports():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        raw_rows = [
            {"Date": "2025-01-02", "Type": "STOCK", "Side": "BUY", "Qty": "1", "Price": "10"},
            {
                "Date": "2025-01-03",
                "Type": "STOCK",
                "Side": "SELL",
                "Qty": "1",
                "Price": "12",
            },
        ]
        normalized_rows = [
            {
                "account_id": account.id,
                "broker": "B1",
                "trade_id": "T-1",
                "executed_at": datetime(2025, 1, 2, 10, 0, 0),
                "instrument_type": "STOCK",
                "symbol": "AAPL",
                "side": "BUY",
                "option_symbol_raw": None,
                "underlying": None,
                "expiration": None,
                "strike": None,
                "call_put": None,
                "multiplier": 1,
                "quantity": 1.0,
                "price": 10.0,
                "fees": 0.0,
                "net_amount": -10.0,
                "currency": "USD",
            },
            {
                "account_id": account.id,
                "broker": "B1",
                "trade_id": "T-2",
                "executed_at": datetime(2025, 1, 3, 10, 0, 0),
                "instrument_type": "STOCK",
                "symbol": "AAPL",
                "side": "SELL",
                "option_symbol_raw": None,
                "underlying": None,
                "expiration": None,
                "strike": None,
                "call_put": None,
                "multiplier": 1,
                "quantity": 1.0,
                "price": 12.0,
                "fees": 0.0,
                "net_amount": 12.0,
                "currency": "USD",
            },
        ]

        first = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="file-a.csv",
            file_sig="sig-a",
            mapping_name="m1",
            raw_rows=raw_rows,
            normalized_rows=normalized_rows,
        )
        second = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="file-b.csv",
            file_sig="sig-a",
            mapping_name="m1",
            raw_rows=raw_rows,
            normalized_rows=normalized_rows,
        )

        assert first == (2, 2)
        assert second == (0, 0)
        assert session.scalar(select(func.count()).select_from(TradeRaw)) == 2
        assert session.scalar(select(func.count()).select_from(TradeNormalized)) == 2


def test_insert_cash_activity_dedupes_reimports():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        rows = [
            {
                "account_id": account.id,
                "broker": "B1",
                "posted_at": datetime(2025, 1, 5, 12, 0, 0),
                "activity_type": "DEPOSIT",
                "amount": 100.0,
                "description": "ACH deposit",
                "source": "ACH",
                "is_external": True,
            }
        ]

        first = insert_cash_activity(session, rows)
        second = insert_cash_activity(session, rows)

        assert first == 1
        assert second == 0
        assert session.scalar(select(func.count()).select_from(CashActivity)) == 1


def test_insert_trade_import_dedupes_duplicate_rows_within_same_batch():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        raw_row = {"Date": "2025-01-02", "Type": "STOCK", "Side": "BUY", "Qty": "1", "Price": "10"}
        normalized_row = {
            "account_id": account.id,
            "broker": "B1",
            "trade_id": "T-1",
            "executed_at": datetime(2025, 1, 2, 10, 0, 0),
            "instrument_type": "STOCK",
            "symbol": "AAPL",
            "side": "BUY",
            "option_symbol_raw": None,
            "underlying": None,
            "expiration": None,
            "strike": None,
            "call_put": None,
            "multiplier": 1,
            "quantity": 1.0,
            "price": 10.0,
            "fees": 0.0,
            "net_amount": -10.0,
            "currency": "USD",
        }

        inserted = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="file-a.csv",
            file_sig="sig-a",
            mapping_name="m1",
            raw_rows=[raw_row, raw_row],
            normalized_rows=[normalized_row, normalized_row],
        )

        assert inserted == (1, 1)
        assert session.scalar(select(func.count()).select_from(TradeRaw)) == 1
        assert session.scalar(select(func.count()).select_from(TradeNormalized)) == 1


def test_insert_cash_activity_dedupes_duplicate_rows_within_same_batch():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        row = {
            "account_id": account.id,
            "broker": "B1",
            "posted_at": datetime(2025, 1, 5, 12, 0, 0),
            "activity_type": "DEPOSIT",
            "amount": 100.0,
            "description": "ACH deposit",
            "source": "ACH",
            "is_external": True,
        }

        inserted = insert_cash_activity(session, [row, row])
        assert inserted == 1
        assert session.scalar(select(func.count()).select_from(CashActivity)) == 1


def test_insert_trade_import_large_batches_handle_reimport_and_partial_conflicts():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        raw_rows_1, normalized_rows_1 = _trade_import_rows(account.id, start=0, count=6000)

        first = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="trades-1.csv",
            file_sig="sig-large-1",
            mapping_name="m1",
            raw_rows=raw_rows_1,
            normalized_rows=normalized_rows_1,
        )
        assert first == (6000, 6000)

        second = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="trades-2.csv",
            file_sig="sig-large-1",
            mapping_name="m1",
            raw_rows=raw_rows_1,
            normalized_rows=normalized_rows_1,
        )
        assert second == (0, 0)

        raw_rows_2, normalized_rows_2 = _trade_import_rows(account.id, start=3000, count=6000)
        third = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="trades-3.csv",
            file_sig="sig-large-2",
            mapping_name="m1",
            raw_rows=raw_rows_2,
            normalized_rows=normalized_rows_2,
        )
        assert third == (3000, 3000)

        assert session.scalar(select(func.count()).select_from(TradeRaw)) == 9000
        assert session.scalar(select(func.count()).select_from(TradeNormalized)) == 9000


def test_insert_cash_activity_large_batches_handle_reimport_and_partial_conflicts():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        rows_1 = _cash_rows(account.id, start=0, count=6000)

        first = insert_cash_activity(session, rows_1)
        second = insert_cash_activity(session, rows_1)
        assert first == 6000
        assert second == 0

        rows_2 = _cash_rows(account.id, start=3000, count=6000)
        third = insert_cash_activity(session, rows_2)
        assert third == 3000

        assert session.scalar(select(func.count()).select_from(CashActivity)) == 9000


def test_insert_trade_import_duplicate_heavy_batch_reports_perf_stats():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        raw_rows: list[dict] = []
        normalized_rows: list[dict] = []
        unique_trade_keys = 40
        repeats_per_key = 60

        for unique_idx in range(unique_trade_keys):
            executed_at = datetime(2025, 1, 1, 9, 30, 0) + timedelta(minutes=unique_idx)
            raw_payload = {
                "Date": executed_at.isoformat(),
                "Type": "STOCK",
                "Side": "BUY",
                "Qty": "1",
                "Price": "10",
                "Symbol": f"SYM{unique_idx:02d}",
            }
            normalized_payload = {
                "account_id": account.id,
                "broker": "B1",
                "trade_id": f"T-{unique_idx}",
                "executed_at": executed_at,
                "instrument_type": "STOCK",
                "symbol": "AAPL",
                "side": "BUY",
                "option_symbol_raw": None,
                "underlying": None,
                "expiration": None,
                "strike": None,
                "call_put": None,
                "multiplier": 1,
                "quantity": 1.0,
                "price": 10.0,
                "fees": 0.0,
                "net_amount": -10.0,
                "currency": "USD",
            }
            raw_rows.extend([raw_payload] * repeats_per_key)
            normalized_rows.extend([normalized_payload] * repeats_per_key)

        perf_stats: dict[str, float | int] = {}
        inserted = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="dup-heavy.csv",
            file_sig="sig-dup-heavy",
            mapping_name="m1",
            raw_rows=raw_rows,
            normalized_rows=normalized_rows,
            perf_stats=perf_stats,
        )

        assert inserted == (unique_trade_keys, unique_trade_keys)
        assert session.scalar(select(func.count()).select_from(TradeRaw)) == unique_trade_keys
        assert session.scalar(select(func.count()).select_from(TradeNormalized)) == unique_trade_keys

        expected_input = unique_trade_keys * repeats_per_key
        assert set(perf_stats) == {
            "input_raw_rows",
            "input_normalized_rows",
            "prepared_raw_rows",
            "prepared_normalized_rows",
            "deduped_raw_rows",
            "deduped_normalized_rows",
            "inserted_raw_rows",
            "inserted_normalized_rows",
            "conflict_raw_rows",
            "conflict_normalized_rows",
            "prepare_seconds",
            "dedupe_seconds",
            "insert_seconds",
            "total_seconds",
        }
        assert perf_stats["input_raw_rows"] == expected_input
        assert perf_stats["input_normalized_rows"] == expected_input
        assert perf_stats["prepared_raw_rows"] == unique_trade_keys
        assert perf_stats["prepared_normalized_rows"] == unique_trade_keys
        assert perf_stats["deduped_raw_rows"] == expected_input - unique_trade_keys
        assert perf_stats["deduped_normalized_rows"] == expected_input - unique_trade_keys
        assert perf_stats["inserted_raw_rows"] == unique_trade_keys
        assert perf_stats["inserted_normalized_rows"] == unique_trade_keys
        assert perf_stats["conflict_raw_rows"] == 0
        assert perf_stats["conflict_normalized_rows"] == 0
        assert float(perf_stats["total_seconds"]) >= float(perf_stats["insert_seconds"]) >= 0.0
        assert float(perf_stats["dedupe_seconds"]) >= 0.0

        reimport_stats: dict[str, float | int] = {}
        reimported = insert_trade_import(
            session=session,
            account_id=account.id,
            broker="B1",
            source_file="dup-heavy.csv",
            file_sig="sig-dup-heavy",
            mapping_name="m1",
            raw_rows=raw_rows,
            normalized_rows=normalized_rows,
            perf_stats=reimport_stats,
        )

        assert reimported == (0, 0)
        assert reimport_stats["prepared_raw_rows"] == unique_trade_keys
        assert reimport_stats["prepared_normalized_rows"] == unique_trade_keys
        assert reimport_stats["conflict_raw_rows"] == unique_trade_keys
        assert reimport_stats["conflict_normalized_rows"] == unique_trade_keys


def test_insert_cash_activity_duplicate_heavy_batch_reports_perf_stats():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        rows: list[dict] = []
        unique_rows = 35
        repeats_per_row = 80
        for unique_idx in range(unique_rows):
            posted_at = datetime(2025, 1, 5, 12, 0, 0) + timedelta(minutes=unique_idx)
            payload = {
                "account_id": account.id,
                "broker": "B1",
                "posted_at": posted_at,
                "activity_type": "DEPOSIT",
                "amount": 100.0,
                "description": f"ACH deposit {unique_idx}",
                "source": "ACH",
                "is_external": True,
            }
            rows.extend([payload] * repeats_per_row)

        perf_stats: dict[str, float | int] = {}
        inserted = insert_cash_activity(session, rows, perf_stats=perf_stats)

        assert inserted == unique_rows
        assert session.scalar(select(func.count()).select_from(CashActivity)) == unique_rows

        expected_input = unique_rows * repeats_per_row
        assert set(perf_stats) == {
            "input_rows",
            "prepared_rows",
            "deduped_rows",
            "inserted_rows",
            "conflict_rows",
            "prepare_seconds",
            "dedupe_seconds",
            "insert_seconds",
            "total_seconds",
        }
        assert perf_stats["input_rows"] == expected_input
        assert perf_stats["prepared_rows"] == unique_rows
        assert perf_stats["deduped_rows"] == expected_input - unique_rows
        assert perf_stats["inserted_rows"] == unique_rows
        assert perf_stats["conflict_rows"] == 0
        assert float(perf_stats["total_seconds"]) >= float(perf_stats["insert_seconds"]) >= 0.0
        assert float(perf_stats["dedupe_seconds"]) >= 0.0

        reimport_stats: dict[str, float | int] = {}
        reimported = insert_cash_activity(session, rows, perf_stats=reimport_stats)
        assert reimported == 0
        assert reimport_stats["prepared_rows"] == unique_rows
        assert reimport_stats["conflict_rows"] == unique_rows


def test_delete_account_if_empty_removes_account_without_dependencies():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        account_id = account.id
        ok, message = delete_account_if_empty(session, account_id)
        session.commit()

        assert ok is True
        assert "Removed account" in message
        assert "trade imports=0" in message
        assert session.get(Account, account_id) is None


def test_delete_account_if_empty_blocks_when_trade_data_exists():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)
        session.add(
            TradeNormalized(
                account_id=account.id,
                broker="B1",
                trade_id="T-1",
                executed_at=datetime(2025, 1, 3, 10, 0, 0),
                instrument_type="STOCK",
                symbol="AAPL",
                side="BUY",
                option_symbol_raw=None,
                underlying=None,
                expiration=None,
                strike=None,
                call_put=None,
                multiplier=1,
                quantity=1.0,
                price=100.0,
                fees=0.0,
                net_amount=-100.0,
                currency="USD",
            )
        )
        session.flush()

        ok, message = delete_account_if_empty(session, account.id)
        session.rollback()

        assert ok is False
        assert "Cannot remove account" in message
        assert "force=True" in message
        assert "normalized trades" in message


def test_delete_account_if_empty_force_deletes_dependencies_transactionally():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _setup_account(session)

        trade_1 = TradeNormalized(
            account_id=account.id,
            broker="B1",
            trade_id="T-1",
            executed_at=datetime(2025, 1, 2, 10, 0, 0),
            instrument_type="STOCK",
            symbol="AAPL",
            side="BUY",
            option_symbol_raw=None,
            underlying=None,
            expiration=None,
            strike=None,
            call_put=None,
            multiplier=1,
            quantity=1.0,
            price=100.0,
            fees=0.0,
            net_amount=-100.0,
            currency="USD",
        )
        trade_2 = TradeNormalized(
            account_id=account.id,
            broker="B1",
            trade_id="T-2",
            executed_at=datetime(2025, 1, 3, 10, 0, 0),
            instrument_type="STOCK",
            symbol="AAPL",
            side="SELL",
            option_symbol_raw=None,
            underlying=None,
            expiration=None,
            strike=None,
            call_put=None,
            multiplier=1,
            quantity=1.0,
            price=110.0,
            fees=0.0,
            net_amount=110.0,
            currency="USD",
        )
        session.add_all([trade_1, trade_2])
        session.flush()

        session.add(
            TradeRaw(
                account_id=account.id,
                broker="B1",
                source_file="trades.csv",
                file_signature="sig-a",
                row_index=0,
                row_hash="hash-a",
                raw_payload={"Date": "2025-01-02"},
                mapping_name="m1",
            )
        )
        session.add(
            CashActivity(
                account_id=account.id,
                broker="B1",
                posted_at=datetime(2025, 1, 5, 12, 0, 0),
                activity_type="DEPOSIT",
                amount=500.0,
                description="ACH deposit",
                source="ACH",
                is_external=True,
            )
        )
        realized = PnlRealized(
            account_id=account.id,
            symbol="AAPL",
            instrument_type="STOCK",
            close_date=datetime(2025, 1, 3).date(),
            quantity=1.0,
            proceeds=110.0,
            cost_basis=100.0,
            fees=0.0,
            pnl=10.0,
            notes="test",
        )
        position = PositionOpen(
            account_id=account.id,
            instrument_type="STOCK",
            symbol="AAPL",
            option_symbol_raw=None,
            quantity=1.0,
            avg_cost=100.0,
            last_price=110.0,
            market_value=110.0,
            unrealized_pnl=10.0,
            as_of=datetime(2025, 1, 6, 12, 0, 0),
        )
        run = ReconciliationRun(
            tax_year=2025,
            account_id=account.id,
            broker="B1",
            scope_label="single",
            broker_input_kind="csv",
            status="DRAFT",
        )
        session.add_all([realized, position, run])
        session.flush()

        session.add(
            ReconciliationArtifact(
                reconciliation_run_id=run.id,
                tax_year=2025,
                artifact_type="APP_SUMMARY",
                artifact_name="summary",
                storage_format="json",
                payload_json={"ok": True},
            )
        )
        session.add(
            WashSaleAdjustment(
                mode="IRS",
                tax_year=2025,
                reconciliation_run_id=run.id,
                loss_sale_row_id=realized.id,
                loss_trade_row_id=trade_1.id,
                replacement_trade_row_id=trade_2.id,
                replacement_account_id=account.id,
                sale_symbol="AAPL",
                sale_date=datetime(2025, 1, 3).date(),
                replacement_executed_at=datetime(2025, 1, 4, 9, 0, 0),
                window_offset_days=1,
                replacement_quantity_equiv=1.0,
                disallowed_loss=10.0,
                basis_adjustment=10.0,
                permanently_disallowed=False,
                adjustment_sequence=0,
                status="APPLIED",
            )
        )
        session.flush()

        ok, message = delete_account_if_empty(session, account.id, force=True)
        session.commit()

        assert ok is True
        assert message == (
            f"Force removed account '{account.broker} | {account.account_label}'. "
            "Deleted dependencies: trade imports=1, normalized trades=2, cash rows=1, "
            "realized rows=1, open positions=1, reconciliation artifacts=1, "
            "reconciliation runs=1, wash-sale matches=1."
        )

        assert session.scalar(select(func.count()).select_from(TradeRaw)) == 0
        assert session.scalar(select(func.count()).select_from(TradeNormalized)) == 0
        assert session.scalar(select(func.count()).select_from(CashActivity)) == 0
        assert session.scalar(select(func.count()).select_from(PnlRealized)) == 0
        assert session.scalar(select(func.count()).select_from(PositionOpen)) == 0
        assert session.scalar(select(func.count()).select_from(ReconciliationArtifact)) == 0
        assert session.scalar(select(func.count()).select_from(ReconciliationRun)) == 0
        assert session.scalar(select(func.count()).select_from(WashSaleAdjustment)) == 0
        assert session.get(Account, account.id) is None
