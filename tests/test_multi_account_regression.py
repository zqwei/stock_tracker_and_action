from __future__ import annotations

from datetime import date
from math import isclose

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.analytics.reconciliation import (
    contributions_by_month,
    daily_realized_pnl,
    net_contributions,
)
from portfolio_assistant.analytics.wash_sale import detect_wash_sale_risks
from portfolio_assistant.assistant.tools_db import insert_cash_activity, insert_trade_import
from portfolio_assistant.db.models import CashActivity, PnlRealized, TradeNormalized, TradeRaw
from portfolio_assistant.ingest.csv_import import normalize_cash_records, normalize_trade_records


TRADE_MAPPING = {
    "executed_at": "Date",
    "instrument_type": "Type",
    "symbol": "Symbol",
    "side": "Side",
    "quantity": "Qty",
    "price": "Price",
    "fees": "Fees",
}

CASH_MAPPING = {
    "posted_at": "Date",
    "activity_type": "Type",
    "amount": "Amount",
    "description": "Description",
    "source": "Source",
}


def test_two_account_import_keeps_identical_rows_separate_by_account(
    db_session: Session,
    two_account_fixture,
    synthetic_trade_csv_rows: list[dict[str, str]],
    synthetic_cash_csv_rows: list[dict[str, str]],
):
    taxable_id = two_account_fixture.taxable_id
    ira_id = two_account_fixture.ira_id

    trade_df = pd.DataFrame(synthetic_trade_csv_rows)
    taxable_trades, taxable_trade_issues = normalize_trade_records(
        trade_df, TRADE_MAPPING, account_id=taxable_id, broker="B1"
    )
    ira_trades, ira_trade_issues = normalize_trade_records(
        trade_df, TRADE_MAPPING, account_id=ira_id, broker="B1"
    )
    assert not taxable_trade_issues
    assert not ira_trade_issues

    raw_rows = trade_df.to_dict(orient="records")
    assert insert_trade_import(
        session=db_session,
        account_id=taxable_id,
        broker="B1",
        source_file="synthetic-trades.csv",
        file_sig="sig-synthetic-trades",
        mapping_name="synthetic",
        raw_rows=raw_rows,
        normalized_rows=taxable_trades,
    ) == (1, 1)
    assert insert_trade_import(
        session=db_session,
        account_id=ira_id,
        broker="B1",
        source_file="synthetic-trades.csv",
        file_sig="sig-synthetic-trades",
        mapping_name="synthetic",
        raw_rows=raw_rows,
        normalized_rows=ira_trades,
    ) == (1, 1)
    assert insert_trade_import(
        session=db_session,
        account_id=taxable_id,
        broker="B1",
        source_file="synthetic-trades.csv",
        file_sig="sig-synthetic-trades",
        mapping_name="synthetic",
        raw_rows=raw_rows,
        normalized_rows=taxable_trades,
    ) == (0, 0)

    assert db_session.scalar(select(func.count()).select_from(TradeRaw)) == 2
    assert db_session.scalar(select(func.count()).select_from(TradeNormalized)) == 2

    cash_df = pd.DataFrame(synthetic_cash_csv_rows)
    taxable_cash, taxable_cash_issues = normalize_cash_records(
        cash_df, CASH_MAPPING, account_id=taxable_id, broker="B1"
    )
    ira_cash, ira_cash_issues = normalize_cash_records(
        cash_df, CASH_MAPPING, account_id=ira_id, broker="B1"
    )
    assert not taxable_cash_issues
    assert not ira_cash_issues

    inserted = insert_cash_activity(db_session, taxable_cash + ira_cash)
    assert inserted == 2
    assert insert_cash_activity(db_session, taxable_cash + ira_cash) == 0
    assert db_session.scalar(select(func.count()).select_from(CashActivity)) == 2


def test_recompute_pnl_consolidates_and_preserves_other_account_rows(
    db_session: Session, seeded_two_account_activity
):
    taxable_id = seeded_two_account_activity.taxable_id
    ira_id = seeded_two_account_activity.ira_id

    stats = recompute_pnl(db_session)
    assert stats["realized_rows"] == 3

    totals_by_account = dict(
        db_session.execute(
            select(PnlRealized.account_id, func.sum(PnlRealized.pnl)).group_by(
                PnlRealized.account_id
            )
        ).all()
    )
    assert isclose(float(totals_by_account[taxable_id]), -75.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(totals_by_account[ira_id]), 20.0, rel_tol=0.0, abs_tol=1e-9)

    ira_before = list(
        db_session.scalars(select(PnlRealized).where(PnlRealized.account_id == ira_id)).all()
    )
    recompute_pnl(db_session, account_id=taxable_id)
    ira_after = list(
        db_session.scalars(select(PnlRealized).where(PnlRealized.account_id == ira_id)).all()
    )
    assert len(ira_before) == len(ira_after) == 1
    assert isclose(float(ira_before[0].pnl), float(ira_after[0].pnl), rel_tol=0.0, abs_tol=1e-9)


def test_two_account_contributions_external_only_and_monthly_buckets(
    db_session: Session, seeded_two_account_activity
):
    taxable_id = seeded_two_account_activity.taxable_id
    ira_id = seeded_two_account_activity.ira_id

    assert isclose(net_contributions(db_session), 5550.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(
        net_contributions(db_session, account_id=taxable_id), 4900.0, rel_tol=0.0, abs_tol=1e-9
    )
    assert isclose(
        net_contributions(db_session, account_id=ira_id), 650.0, rel_tol=0.0, abs_tol=1e-9
    )

    assert contributions_by_month(db_session) == [
        {"month": "2025-01", "net_contribution": 5600.0},
        {"month": "2025-02", "net_contribution": -50.0},
    ]
    assert contributions_by_month(db_session, account_id=ira_id) == [
        {"month": "2025-01", "net_contribution": 700.0},
        {"month": "2025-02", "net_contribution": -50.0},
    ]


def test_daily_realized_calendar_aggregates_consolidated_and_account_views(
    db_session: Session, seeded_two_account_activity
):
    taxable_id = seeded_two_account_activity.taxable_id
    ira_id = seeded_two_account_activity.ira_id

    recompute_pnl(db_session)

    consolidated = {
        row["close_date"]: float(row["pnl"]) for row in daily_realized_pnl(db_session)
    }
    assert consolidated == {date(2025, 1, 10): -100.0, date(2025, 1, 12): 45.0}

    taxable_only = {
        row["close_date"]: float(row["pnl"])
        for row in daily_realized_pnl(db_session, account_id=taxable_id)
    }
    assert taxable_only == {date(2025, 1, 10): -100.0, date(2025, 1, 12): 25.0}

    ira_only = {
        row["close_date"]: float(row["pnl"])
        for row in daily_realized_pnl(db_session, account_id=ira_id)
    }
    assert ira_only == {date(2025, 1, 12): 20.0}


def test_wash_sale_warning_uses_cross_account_replacement_from_synthetic_fixture(
    db_session: Session, seeded_two_account_activity
):
    taxable_id = seeded_two_account_activity.taxable_id
    ira_id = seeded_two_account_activity.ira_id

    recompute_pnl(db_session)

    risks = detect_wash_sale_risks(db_session)
    assert len(risks) == 1
    risk = risks[0]
    assert risk["symbol"] == "AAPL"
    assert risk["sale_account_id"] == taxable_id
    assert risk["buy_account_id"] == ira_id
    assert risk["cross_account"]
    assert risk["ira_replacement"]
    assert int(risk["days_from_sale"]) == 10
    assert isclose(
        float(risk["allocated_replacement_quantity_equiv"]), 3.0, rel_tol=0.0, abs_tol=1e-9
    )

    taxable_only_risks = detect_wash_sale_risks(db_session, account_id=taxable_id)
    assert len(taxable_only_risks) == 1
    assert taxable_only_risks[0]["sale_account_id"] == taxable_id
