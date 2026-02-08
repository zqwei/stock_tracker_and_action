from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.analytics.wash_sale import detect_wash_sale_risks
from portfolio_assistant.db.models import Account, Base, TradeNormalized


def test_wash_sale_detects_cross_account_buy_within_30_days():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        taxable = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
        roth = Account(broker="B1", account_label="Roth", account_type="ROTH_IRA")
        session.add_all([taxable, roth])
        session.flush()

        session.add_all(
            [
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 2, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=100,
                    price=100.0,
                    fees=0.0,
                    net_amount=-10000.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 10, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="SELL",
                    quantity=100,
                    price=90.0,
                    fees=0.0,
                    net_amount=9000.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=roth.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 20, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=10,
                    price=91.0,
                    fees=0.0,
                    net_amount=-910.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        recompute_pnl(session)
        session.commit()

        risks = detect_wash_sale_risks(session)
        assert risks, "Expected at least one wash-sale risk."
        assert any(
            r["symbol"] == "AAPL" and r["buy_account_id"] == roth.id for r in risks
        ), risks
