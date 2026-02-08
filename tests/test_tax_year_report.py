from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.tax_year_report import generate_tax_year_report
from portfolio_assistant.db.models import Account, Base, PnlRealized


def test_tax_year_report_filters_rows_to_selected_year():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
        session.add(account)
        session.flush()

        session.add_all(
            [
                PnlRealized(
                    account_id=account.id,
                    symbol="AAPL",
                    instrument_type="STOCK",
                    close_date=date(2025, 1, 10),
                    quantity=10,
                    proceeds=1000,
                    cost_basis=900,
                    fees=0,
                    pnl=100,
                ),
                PnlRealized(
                    account_id=account.id,
                    symbol="MSFT",
                    instrument_type="STOCK",
                    close_date=date(2024, 12, 31),
                    quantity=5,
                    proceeds=500,
                    cost_basis=550,
                    fees=0,
                    pnl=-50,
                ),
            ]
        )
        session.commit()

        report = generate_tax_year_report(session, tax_year=2025, account_id=account.id)
        assert report["summary"]["rows"] == 1
        assert report["summary"]["total_gain_or_loss"] == 100.0
        assert report["detail_rows"][0]["symbol"] == "AAPL"
