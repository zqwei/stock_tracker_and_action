from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from portfolio_assistant.assistant.ask_gpt import (
    MAX_TOOL_ROWS,
    dispatch_read_only_tool,
    extract_response_sources,
)
from portfolio_assistant.db.models import Account, Base, TradeNormalized


def _create_account(session: Session, *, label: str) -> Account:
    account = Account(broker="B1", account_label=label, account_type="TAXABLE")
    session.add(account)
    session.flush()
    return account


def test_dispatch_read_only_tool_caps_limit():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _create_account(session, label="Taxable")
        for idx in range(MAX_TOOL_ROWS + 20):
            session.add(
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 1, 10, 0, 0) + timedelta(minutes=idx),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=1.0,
                    price=100.0,
                    fees=0.0,
                    net_amount=-100.0,
                    multiplier=1,
                    currency="USD",
                )
            )
        session.commit()

    payload = dispatch_read_only_tool(
        engine,
        name="get_recent_trades",
        arguments={"limit": 9999},
    )
    assert int(payload["count"]) == MAX_TOOL_ROWS


def test_dispatch_read_only_tool_enforces_account_scope_override():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        taxable = _create_account(session, label="Taxable")
        ira = _create_account(session, label="Roth IRA")
        taxable_id = taxable.id
        ira_id = ira.id

        session.add_all(
            [
                TradeNormalized(
                    account_id=taxable_id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 2, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=1.0,
                    price=100.0,
                    fees=0.0,
                    net_amount=-100.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=ira_id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 2, 10, 5, 0),
                    instrument_type="STOCK",
                    symbol="QQQ",
                    side="BUY",
                    quantity=1.0,
                    price=100.0,
                    fees=0.0,
                    net_amount=-100.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.commit()

    payload = dispatch_read_only_tool(
        engine,
        name="get_recent_trades",
        arguments={"account_id": ira_id, "limit": 20},
        account_scope_id=taxable_id,
    )
    assert int(payload["count"]) == 1
    assert all(row["account_id"] == taxable_id for row in payload["rows"])


def test_dispatch_read_only_tool_rejects_unknown_name():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with pytest.raises(ValueError):
        dispatch_read_only_tool(engine, name="drop_all_tables")


def test_extract_response_sources_reads_url_annotations():
    response = SimpleNamespace(
        output=[
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": "Summary",
                        "annotations": [
                            {
                                "type": "url_citation",
                                "title": "Example Source",
                                "url": "https://example.com/report",
                            }
                        ],
                    }
                ],
            }
        ]
    )
    sources = extract_response_sources(response)
    assert sources == [{"title": "Example Source", "url": "https://example.com/report"}]
