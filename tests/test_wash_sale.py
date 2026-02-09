from __future__ import annotations

from datetime import datetime
from math import isclose

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.analytics.wash_sale import (
    detect_wash_sale_risks,
    estimate_wash_sale_disallowance,
)
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
                    executed_at=datetime(2024, 11, 15, 10, 0, 0),
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
            r["symbol"] == "AAPL"
            and r["buy_account_id"] == roth.id
            and r["cross_account"]
            and r["ira_replacement"]
            for r in risks
        ), risks


def test_wash_sale_boundaries_include_day_30_exclude_day_31_across_accounts():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        taxable_1 = Account(broker="B1", account_label="Taxable 1", account_type="TAXABLE")
        taxable_2 = Account(broker="B1", account_label="Taxable 2", account_type="TAXABLE")
        roth = Account(broker="B1", account_label="Roth", account_type="ROTH_IRA")
        session.add_all([taxable_1, taxable_2, roth])
        session.flush()

        session.add_all(
            [
                TradeNormalized(
                    account_id=taxable_1.id,
                    broker="B1",
                    executed_at=datetime(2024, 12, 1, 10, 0, 0),
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
                    account_id=taxable_1.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 1, 10, 0, 0),
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
                    account_id=roth.id,
                    broker="B1",
                    executed_at=datetime(2024, 12, 2, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=2,
                    price=95.0,
                    fees=0.0,
                    net_amount=-190.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable_2.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 31, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=3,
                    price=92.0,
                    fees=0.0,
                    net_amount=-276.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable_2.id,
                    broker="B1",
                    executed_at=datetime(2025, 2, 1, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=4,
                    price=93.0,
                    fees=0.0,
                    net_amount=-372.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        recompute_pnl(session)
        session.commit()

        risks = detect_wash_sale_risks(session)
        assert risks

        days = sorted({int(r["days_from_sale"]) for r in risks})
        assert -30 in days
        assert 30 in days
        assert 31 not in days
        assert all(abs(int(r["days_from_sale"])) <= 30 for r in risks)
        assert any(r["is_boundary_day"] for r in risks)
        assert any(r["ira_replacement"] for r in risks)

        allocated = sum(float(r["allocated_replacement_quantity_equiv"]) for r in risks)
        assert isclose(allocated, 5.0, rel_tol=0.0, abs_tol=1e-9)


def test_wash_sale_allocation_does_not_double_count_same_replacement_buy():
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
                    executed_at=datetime(2024, 11, 1, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=20,
                    price=100.0,
                    fees=0.0,
                    net_amount=-2000.0,
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
                    quantity=10,
                    price=90.0,
                    fees=0.0,
                    net_amount=900.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 15, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="SELL",
                    quantity=10,
                    price=80.0,
                    fees=0.0,
                    net_amount=800.0,
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
                    price=85.0,
                    fees=0.0,
                    net_amount=-850.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        recompute_pnl(session)
        session.commit()

        analysis = estimate_wash_sale_disallowance(session, mode="irs")
        assert isclose(float(analysis["total_disallowed_loss"]), 100.0, rel_tol=0.0, abs_tol=1e-9)
        assert len(analysis["sales"]) == 1
        assert analysis["sales"][0]["sale_date"] == "2025-01-10"

        risks = detect_wash_sale_risks(session)
        assert len(risks) == 1
        assert risks[0]["sale_date"] == "2025-01-10"
        assert isclose(
            float(risks[0]["allocated_replacement_quantity_equiv"]),
            10.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
