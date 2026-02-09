from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from portfolio_assistant.assistant.tools_db import insert_cash_activity, insert_trade_import
from portfolio_assistant.db.models import Account, Base, CashActivity, TradeNormalized, TradeRaw
from portfolio_assistant.ingest.csv_import import normalize_cash_records, normalize_trade_records
from portfolio_assistant.ingest.csv_mapping import get_saved_trade_mapping, save_trade_mapping


def _setup_account(session: Session) -> Account:
    account = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
    session.add(account)
    session.flush()
    return account


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

    columns = ["Date", "Side", "Qty", "Price"]
    missing_required = {
        "executed_at": "Date",
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
    assert "Missing required field mapping" in str(exc_info.value)


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
