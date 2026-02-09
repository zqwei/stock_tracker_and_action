from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import Account, Base, CashActivity, TradeNormalized


@dataclass(frozen=True)
class TwoAccountFixture:
    taxable_id: str
    ira_id: str


@pytest.fixture
def db_session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture
def two_account_fixture(db_session: Session) -> TwoAccountFixture:
    taxable = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
    ira = Account(broker="B1", account_label="Roth IRA", account_type="ROTH_IRA")
    db_session.add_all([taxable, ira])
    db_session.flush()
    return TwoAccountFixture(taxable_id=taxable.id, ira_id=ira.id)


@pytest.fixture
def synthetic_trade_csv_rows() -> list[dict[str, str]]:
    return [
        {
            "Date": "2025-03-01 10:00:00",
            "Type": "STOCK",
            "Symbol": "AAPL",
            "Side": "BUY",
            "Qty": "1",
            "Price": "100.00",
            "Fees": "0.00",
        }
    ]


@pytest.fixture
def synthetic_cash_csv_rows() -> list[dict[str, str]]:
    return [
        {
            "Date": "2025-03-02 09:00:00",
            "Type": "DEPOSIT",
            "Amount": "500.00",
            "Description": "ACH transfer",
            "Source": "ACH",
        }
    ]


@pytest.fixture
def seeded_two_account_activity(
    db_session: Session, two_account_fixture: TwoAccountFixture
) -> TwoAccountFixture:
    taxable_id = two_account_fixture.taxable_id
    ira_id = two_account_fixture.ira_id

    db_session.add_all(
        [
            TradeNormalized(
                account_id=taxable_id,
                broker="B1",
                executed_at=datetime(2024, 11, 15, 10, 0, 0),
                instrument_type="STOCK",
                symbol="AAPL",
                side="BUY",
                quantity=10,
                price=100.0,
                fees=0.0,
                net_amount=-1000.0,
                multiplier=1,
                currency="USD",
            ),
            TradeNormalized(
                account_id=taxable_id,
                broker="B1",
                executed_at=datetime(2025, 1, 10, 10, 0, 0),
                instrument_type="STOCK",
                symbol="AAPL",
                side="SELL",
                quantity=10,
                price=90.0,
                fees=0.0,
                net_amount=900.0,
                multiplier=1,
                currency="USD",
            ),
            TradeNormalized(
                account_id=taxable_id,
                broker="B1",
                executed_at=datetime(2025, 1, 5, 10, 0, 0),
                instrument_type="STOCK",
                symbol="MSFT",
                side="BUY",
                quantity=5,
                price=50.0,
                fees=0.0,
                net_amount=-250.0,
                multiplier=1,
                currency="USD",
            ),
            TradeNormalized(
                account_id=taxable_id,
                broker="B1",
                executed_at=datetime(2025, 1, 12, 10, 0, 0),
                instrument_type="STOCK",
                symbol="MSFT",
                side="SELL",
                quantity=5,
                price=55.0,
                fees=0.0,
                net_amount=275.0,
                multiplier=1,
                currency="USD",
            ),
            TradeNormalized(
                account_id=ira_id,
                broker="B1",
                executed_at=datetime(2025, 1, 3, 10, 0, 0),
                instrument_type="STOCK",
                symbol="QQQ",
                side="BUY",
                quantity=2,
                price=100.0,
                fees=0.0,
                net_amount=-200.0,
                multiplier=1,
                currency="USD",
            ),
            TradeNormalized(
                account_id=ira_id,
                broker="B1",
                executed_at=datetime(2025, 1, 12, 10, 0, 0),
                instrument_type="STOCK",
                symbol="QQQ",
                side="SELL",
                quantity=2,
                price=110.0,
                fees=0.0,
                net_amount=220.0,
                multiplier=1,
                currency="USD",
            ),
            TradeNormalized(
                account_id=ira_id,
                broker="B1",
                executed_at=datetime(2025, 1, 20, 10, 0, 0),
                instrument_type="STOCK",
                symbol="AAPL",
                side="BUY",
                quantity=3,
                price=91.0,
                fees=0.0,
                net_amount=-273.0,
                multiplier=1,
                currency="USD",
            ),
        ]
    )
    db_session.add_all(
        [
            CashActivity(
                account_id=taxable_id,
                broker="B1",
                posted_at=datetime(2025, 1, 2, 9, 0, 0),
                activity_type="DEPOSIT",
                amount=5000.0,
                description="Payroll ACH",
                source="ACH",
                is_external=True,
            ),
            CashActivity(
                account_id=taxable_id,
                broker="B1",
                posted_at=datetime(2025, 1, 15, 9, 0, 0),
                activity_type="WITHDRAWAL",
                amount=300.0,
                description="Internal transfer to IRA",
                source="transfer",
                is_external=False,
            ),
            CashActivity(
                account_id=taxable_id,
                broker="B1",
                posted_at=datetime(2025, 1, 20, 9, 0, 0),
                activity_type="WITHDRAWAL",
                amount=100.0,
                description="Bank withdrawal",
                source="ACH",
                is_external=True,
            ),
            CashActivity(
                account_id=ira_id,
                broker="B1",
                posted_at=datetime(2025, 1, 4, 9, 0, 0),
                activity_type="DEPOSIT",
                amount=700.0,
                description="Initial IRA funding",
                source="ACH",
                is_external=True,
            ),
            CashActivity(
                account_id=ira_id,
                broker="B1",
                posted_at=datetime(2025, 1, 15, 9, 0, 0),
                activity_type="DEPOSIT",
                amount=300.0,
                description="Internal transfer from taxable",
                source="transfer",
                is_external=False,
            ),
            CashActivity(
                account_id=ira_id,
                broker="B1",
                posted_at=datetime(2025, 2, 1, 9, 0, 0),
                activity_type="WITHDRAWAL",
                amount=50.0,
                description="IRA external withdrawal",
                source="ACH",
                is_external=True,
            ),
        ]
    )
    db_session.flush()
    return two_account_fixture
