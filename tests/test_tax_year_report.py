from __future__ import annotations

from datetime import date, datetime
from math import isclose

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.analytics.reconciliation import (
    compare_totals,
    tax_report_totals,
    validate_tax_report_summary,
)
from portfolio_assistant.analytics.tax_year_report import generate_tax_year_report
from portfolio_assistant.db.models import Account, Base, PnlRealized, TradeNormalized


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
                    notes="FIFO close from 2024-12-01",
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
                    notes="FIFO close from 2024-01-01",
                ),
            ]
        )
        session.commit()

        report = generate_tax_year_report(session, tax_year=2025, account_id=account.id)
        assert report["summary"]["rows"] == 1
        assert report["summary"]["total_gain_or_loss"] == 100.0
        assert report["detail_rows"][0]["symbol"] == "AAPL"
        assert report["detail_rows"][0]["term"] == "SHORT"


def test_tax_year_report_includes_broker_vs_irs_wash_sale_differences_and_math_checks():
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
                    account_id=taxable_1.id,
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
                    account_id=taxable_2.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 25, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=4,
                    price=92.0,
                    fees=0.0,
                    net_amount=-368.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=roth.id,
                    broker="B1",
                    executed_at=datetime(2025, 1, 30, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=3,
                    price=93.0,
                    fees=0.0,
                    net_amount=-279.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        recompute_pnl(session)
        session.commit()

        report = generate_tax_year_report(session, tax_year=2025, account_id=taxable_1.id)

        assert report["summary"]["rows"] == 1
        row = report["detail_rows"][0]
        assert row["symbol"] == "AAPL"
        assert row["date_acquired"] == "2024-11-15"
        assert row["term"] == "SHORT"
        assert isclose(float(row["raw_gain_or_loss"]), -100.0, rel_tol=0.0, abs_tol=1e-9)
        assert isclose(
            float(row["wash_sale_disallowed_broker"]), 0.0, rel_tol=0.0, abs_tol=1e-9
        )
        assert isclose(
            float(row["wash_sale_disallowed_irs"]), 70.0, rel_tol=0.0, abs_tol=1e-9
        )
        assert isclose(float(row["gain_or_loss"]), -30.0, rel_tol=0.0, abs_tol=1e-9)
        assert row["adjustment_codes"] == "W"

        summary = report["summary"]
        assert isclose(float(summary["total_gain_or_loss_raw"]), -100.0, rel_tol=0.0, abs_tol=1e-9)
        assert isclose(float(summary["total_gain_or_loss"]), -30.0, rel_tol=0.0, abs_tol=1e-9)
        assert isclose(
            float(summary["total_wash_sale_disallowed_broker"]),
            0.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["total_wash_sale_disallowed_irs"]),
            70.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["wash_sale_mode_difference"]),
            70.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["short_term_gain_or_loss"]),
            -30.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert summary["year_end_open_lot_count"] == 0
        assert bool(summary["math_check_raw"])
        assert bool(summary["math_check_adjusted"])

        validation = validate_tax_report_summary(report)
        assert validation["ok"], validation

        app_totals = tax_report_totals(report["detail_rows"])
        broker_totals = {
            "total_proceeds": 900.0,
            "total_cost_basis": 1000.0,
            "total_gain_or_loss": -100.0,
            "short_term_gain_or_loss": -100.0,
            "long_term_gain_or_loss": 0.0,
            "total_wash_sale_disallowed": 0.0,
        }
        comparison = compare_totals(app_totals, broker_totals)
        assert isclose(
            float(comparison["total_gain_or_loss"]["delta"]),
            70.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(comparison["total_wash_sale_disallowed"]["delta"]),
            70.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )

        mode_diffs = report["broker_vs_irs_reconciliation"]["mode_diffs"]
        assert isclose(
            float(mode_diffs["totals"]["gain_or_loss_delta"]),
            70.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        checklist = report["broker_vs_irs_reconciliation"]["checklist"]
        by_key = {row["key"]: row for row in checklist}
        assert by_key["cross_account_replacements_likely"]["flag"]
        assert not by_key["options_replacements_likely"]["flag"]


def test_tax_year_report_applies_january_replacements_to_december_loss_sales():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        taxable = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
        session.add(taxable)
        session.flush()

        session.add_all(
            [
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2025, 11, 1, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="XYZ",
                    side="BUY",
                    quantity=5,
                    price=50.0,
                    fees=0.0,
                    net_amount=-250.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2025, 12, 31, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="XYZ",
                    side="SELL",
                    quantity=5,
                    price=40.0,
                    fees=0.0,
                    net_amount=200.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2026, 1, 30, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="XYZ",
                    side="BUY",
                    quantity=5,
                    price=42.0,
                    fees=0.0,
                    net_amount=-210.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        recompute_pnl(session)
        session.commit()

        report = generate_tax_year_report(session, tax_year=2025, account_id=taxable.id)
        assert report["summary"]["rows"] == 1

        row = report["detail_rows"][0]
        assert row["date_sold"] == "2025-12-31"
        assert isclose(
            float(row["wash_sale_disallowed"]),
            50.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(float(row["gain_or_loss"]), 0.0, rel_tol=0.0, abs_tol=1e-9)

        summary = report["summary"]
        assert isclose(
            float(summary["total_wash_sale_disallowed"]),
            50.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(float(summary["total_gain_or_loss"]), 0.0, rel_tol=0.0, abs_tol=1e-9)
        assert report["year_end_lot_snapshot"] == []


def test_tax_year_report_year_end_lot_snapshot_includes_wash_adjusted_basis():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        taxable = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
        session.add(taxable)
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
                    quantity=10,
                    price=100.0,
                    fees=0.0,
                    net_amount=-1000.0,
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
                    executed_at=datetime(2025, 1, 20, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=4,
                    price=92.0,
                    fees=0.0,
                    net_amount=-368.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        recompute_pnl(session)
        session.commit()

        report = generate_tax_year_report(session, tax_year=2025, account_id=taxable.id)
        assert report["summary"]["rows"] == 1

        row = report["detail_rows"][0]
        assert isclose(
            float(row["wash_sale_disallowed"]),
            40.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )

        snapshot = report["year_end_lot_snapshot"]
        assert len(snapshot) == 1
        lot = snapshot[0]
        assert lot["symbol"] == "AAPL"
        assert lot["position_side"] == "LONG"
        assert isclose(float(lot["quantity"]), 4.0, rel_tol=0.0, abs_tol=1e-9)
        assert isclose(float(lot["raw_cost_basis"]), 368.0, rel_tol=0.0, abs_tol=1e-9)
        assert isclose(
            float(lot["wash_sale_basis_adjustment"]),
            40.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(float(lot["adjusted_cost_basis"]), 408.0, rel_tol=0.0, abs_tol=1e-9)

        summary = report["summary"]
        assert summary["year_end_open_lot_count"] == 1
        assert isclose(float(summary["year_end_raw_basis_total"]), 368.0, rel_tol=0.0, abs_tol=1e-9)
        assert isclose(
            float(summary["year_end_wash_basis_adjustment_total"]),
            40.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )


def test_tax_year_report_term_splits_and_partial_boundary_wash_accounting():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        taxable = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
        session.add(taxable)
        session.flush()

        session.add_all(
            [
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2023, 1, 2, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="MSFT",
                    side="BUY",
                    quantity=10,
                    price=100.0,
                    fees=0.0,
                    net_amount=-1000.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2025, 3, 3, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="MSFT",
                    side="SELL",
                    quantity=10,
                    price=130.0,
                    fees=0.0,
                    net_amount=1300.0,
                    multiplier=1,
                    currency="USD",
                ),
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2024, 12, 20, 10, 0, 0),
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
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2025, 12, 15, 10, 0, 0),
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
                    executed_at=datetime(2025, 12, 20, 10, 0, 0),
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
                TradeNormalized(
                    account_id=taxable.id,
                    broker="B1",
                    executed_at=datetime(2026, 1, 10, 10, 0, 0),
                    instrument_type="STOCK",
                    symbol="AAPL",
                    side="BUY",
                    quantity=2,
                    price=92.0,
                    fees=0.0,
                    net_amount=-184.0,
                    multiplier=1,
                    currency="USD",
                ),
            ]
        )
        session.flush()

        recompute_pnl(session)
        session.commit()

        report = generate_tax_year_report(session, tax_year=2025, account_id=taxable.id)
        assert report["summary"]["rows"] == 2

        detail_by_symbol = {row["symbol"]: row for row in report["detail_rows"]}
        assert detail_by_symbol["MSFT"]["term"] == "LONG"
        assert detail_by_symbol["AAPL"]["term"] == "SHORT"
        assert isclose(
            float(detail_by_symbol["AAPL"]["wash_sale_disallowed"]),
            50.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )

        summary = report["summary"]
        assert isclose(
            float(summary["short_term_gain_or_loss"]),
            -50.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["long_term_gain_or_loss"]),
            300.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(float(summary["total_gain_or_loss"]), 250.0, rel_tol=0.0, abs_tol=1e-9)
        assert isclose(
            float(summary["short_term_gain_or_loss_broker"]),
            -50.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["long_term_gain_or_loss_broker"]),
            300.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["total_gain_or_loss_broker"]),
            250.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["short_term_wash_sale_disallowed_irs"]),
            50.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["short_term_wash_sale_disallowed_broker"]),
            50.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert isclose(
            float(summary["long_term_wash_sale_disallowed_irs"]),
            0.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        assert bool(summary["math_check_term_split_irs"])
        assert bool(summary["math_check_term_split_broker"])
        assert bool(summary["math_check_wash_term_split_irs"])


def test_tax_report_totals_handles_zero_raw_gain_and_basis_fallback():
    detail_rows = [
        {
            "proceeds": 100.0,
            "cost_basis": 100.0,
            "gain_or_loss": 50.0,
            "raw_gain_or_loss": 0.0,
            "wash_sale_disallowed": 50.0,
            "wash_sale_disallowed_broker": 0.0,
            "wash_sale_disallowed_irs": 50.0,
            "term": "SHORT",
        },
        {
            "proceeds": 200.0,
            "basis": 150.0,
            "cost_basis": None,
            "gain_or_loss": 50.0,
            "raw_gain_or_loss": 50.0,
            "wash_sale_disallowed": 0.0,
            "wash_sale_disallowed_broker": 0.0,
            "wash_sale_disallowed_irs": 0.0,
            "term": "LONG",
        },
    ]

    totals = tax_report_totals(detail_rows)
    assert isclose(float(totals["total_proceeds"]), 300.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(totals["total_cost_basis"]), 250.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(totals["total_gain_or_loss_raw"]), 50.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(totals["total_gain_or_loss"]), 100.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(
        float(totals["total_wash_sale_disallowed_broker"]),
        0.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(
        float(totals["total_wash_sale_disallowed_irs"]),
        50.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(float(totals["wash_sale_mode_difference"]), 50.0, rel_tol=0.0, abs_tol=1e-9)

    report = {
        "summary": {
            **totals,
            "rows": len(detail_rows),
            "math_check_raw": True,
            "math_check_adjusted": True,
        },
        "detail_rows": detail_rows,
    }
    validation = validate_tax_report_summary(report)
    assert validation["ok"], validation
