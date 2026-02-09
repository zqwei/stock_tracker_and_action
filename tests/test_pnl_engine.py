from __future__ import annotations

from datetime import datetime
from math import isclose

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.db.models import Account, Base, PnlRealized, PositionOpen, TradeNormalized


def test_recompute_pnl_handles_partial_fifo_and_option_buy_sell_matching():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
        session.add(account)
        session.flush()

        session.add_all(
            [
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 2, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=100,
                    price=10.0,
                    fees=1.0,
                    net_amount=-1001.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 3, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=50,
                    price=12.0,
                    fees=0.0,
                    net_amount=-600.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 5, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="SELL",
                    quantity=120,
                    price=11.0,
                    fees=2.0,
                    net_amount=1318.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 4, 10, 0, 0),
                    instrument_type="OPTION",
                    symbol="TSLA",
                    underlying="TSLA",
                    expiration=datetime(2025, 6, 20),
                    strike=200.0,
                    call_put="C",
                    option_symbol_raw="TSLA 2025-06-20 200 C",
                    side="BTO",
                    quantity=3,
                    price=1.0,
                    fees=3.0,
                    net_amount=-303.0,
                    multiplier=100,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 6, 10, 0, 0),
                    instrument_type="OPTION",
                    symbol="TSLA",
                    underlying="TSLA",
                    expiration=datetime(2025, 6, 20),
                    strike=200.0,
                    call_put="C",
                    option_symbol_raw=" tsla   2025-06-20   200.0 c ",
                    side="SELL",
                    quantity=2,
                    price=1.5,
                    fees=2.0,
                    net_amount=298.0,
                    multiplier=100,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 7, 10, 0, 0),
                    instrument_type="OPTION",
                    symbol="TSLA",
                    underlying="TSLA",
                    expiration=datetime(2025, 6, 20),
                    strike=200.0,
                    call_put="C",
                    option_symbol_raw=None,
                    side="SELL",
                    quantity=2,
                    price=1.2,
                    fees=2.0,
                    net_amount=238.0,
                    multiplier=100,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=account.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 8, 10, 0, 0),
                    instrument_type="OPTION",
                    symbol="TSLA",
                    underlying="TSLA",
                    expiration=datetime(2025, 6, 20),
                    strike=200.0,
                    call_put="C",
                    option_symbol_raw="TSLA 2025-06-20 200 C",
                    side="BUY",
                    quantity=1,
                    price=1.0,
                    fees=1.0,
                    net_amount=-101.0,
                    multiplier=100,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        stats = recompute_pnl(session)
        session.commit()

        assert stats["realized_rows"] == 5
        assert isclose(float(stats["unmatched_close_quantity"]), 0.0, abs_tol=1e-9)

        stock_realized = list(
            session.scalars(
                select(PnlRealized).where(
                    PnlRealized.account_id == account.id,
                    PnlRealized.symbol == "AAPL",
                )
            ).all()
        )
        assert len(stock_realized) == 2
        assert sorted(round(float(row.quantity), 8) for row in stock_realized) == [20.0, 100.0]
        total_stock_pnl = sum(float(row.pnl) for row in stock_realized)
        assert isclose(total_stock_pnl, 77.0, rel_tol=1e-9, abs_tol=1e-9)

        open_positions = list(
            session.scalars(
                select(PositionOpen).where(PositionOpen.account_id == account.id)
            ).all()
        )
        aapl_open = [p for p in open_positions if p.symbol == "AAPL"]
        assert len(aapl_open) == 1
        assert isclose(float(aapl_open[0].quantity), 30.0, rel_tol=0.0, abs_tol=1e-9)

        tsla_open = [p for p in open_positions if p.symbol == "TSLA"]
        assert not tsla_open

        option_realized_notes = [
            str(r.notes or "")
            for r in session.scalars(
                select(PnlRealized).where(PnlRealized.symbol == "TSLA")
            ).all()
        ]
        assert option_realized_notes
        assert all("from" in note for note in option_realized_notes)


def test_recompute_pnl_reports_unmatched_explicit_option_closes():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
        session.add(account)
        session.flush()

        session.add(
            TradeNormalized(
                account_id=account.id,
                broker="B1",
                executed_at=datetime(2025, 2, 1, 10, 0, 0),
                instrument_type="OPTION",
                symbol="MSFT",
                underlying="MSFT",
                expiration=datetime(2025, 3, 21),
                strike=300.0,
                call_put="C",
                option_symbol_raw="MSFT 2025-03-21 300 C",
                side="STC",
                quantity=1,
                price=2.0,
                fees=1.0,
                net_amount=199.0,
                multiplier=100,
                currency="USD",
            )
        )
        session.flush()

        stats = recompute_pnl(session)
        session.commit()

        assert stats["realized_rows"] == 0
        assert isclose(float(stats["unmatched_close_quantity"]), 1.0, rel_tol=0.0, abs_tol=1e-9)
        realized_rows = list(session.scalars(select(PnlRealized)).all())
        assert len(realized_rows) == 0
