from __future__ import annotations

from datetime import date
from math import isclose

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.pnl_engine import recompute_pnl
from portfolio_assistant.analytics.reconciliation import (
    compare_totals,
    daily_realized_pnl,
    net_contributions,
    tax_report_totals,
    validate_tax_report_summary,
)
from portfolio_assistant.analytics.tax_year_report import generate_tax_year_report
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


def _import_trade_rows(
    session: Session,
    *,
    account_id: str,
    broker: str,
    source_file: str,
    file_sig: str,
    rows: list[dict[str, str]],
) -> tuple[int, int]:
    frame = pd.DataFrame(rows)
    normalized_rows, issues = normalize_trade_records(
        frame,
        TRADE_MAPPING,
        account_id=account_id,
        broker=broker,
    )
    assert not issues

    return insert_trade_import(
        session=session,
        account_id=account_id,
        broker=broker,
        source_file=source_file,
        file_sig=file_sig,
        mapping_name="regression-gate",
        raw_rows=frame.to_dict(orient="records"),
        normalized_rows=normalized_rows,
    )


def _import_cash_rows(
    session: Session,
    *,
    account_id: str,
    broker: str,
    rows: list[dict[str, str]],
) -> int:
    frame = pd.DataFrame(rows)
    normalized_rows, issues = normalize_cash_records(
        frame,
        CASH_MAPPING,
        account_id=account_id,
        broker=broker,
    )
    assert not issues
    return insert_cash_activity(session, normalized_rows)


def test_e2e_regression_gate_import_pnl_wash_tax_report_and_reconciliation(
    db_session: Session,
    two_account_fixture,
):
    taxable_id = two_account_fixture.taxable_id
    ira_id = two_account_fixture.ira_id

    taxable_trade_rows = [
        {
            "Date": "2024-11-15 10:00:00",
            "Type": "STOCK",
            "Symbol": "AAPL",
            "Side": "BUY",
            "Qty": "10",
            "Price": "100.00",
            "Fees": "0.00",
        },
        {
            "Date": "2025-01-10 10:00:00",
            "Type": "STOCK",
            "Symbol": "AAPL",
            "Side": "SELL",
            "Qty": "10",
            "Price": "90.00",
            "Fees": "0.00",
        },
        {
            "Date": "2025-01-05 10:00:00",
            "Type": "STOCK",
            "Symbol": "MSFT",
            "Side": "BUY",
            "Qty": "5",
            "Price": "50.00",
            "Fees": "0.00",
        },
        {
            "Date": "2025-01-12 10:00:00",
            "Type": "STOCK",
            "Symbol": "MSFT",
            "Side": "SELL",
            "Qty": "5",
            "Price": "55.00",
            "Fees": "0.00",
        },
    ]
    ira_trade_rows = [
        {
            "Date": "2025-01-03 10:00:00",
            "Type": "STOCK",
            "Symbol": "QQQ",
            "Side": "BUY",
            "Qty": "2",
            "Price": "100.00",
            "Fees": "0.00",
        },
        {
            "Date": "2025-01-12 10:00:00",
            "Type": "STOCK",
            "Symbol": "QQQ",
            "Side": "SELL",
            "Qty": "2",
            "Price": "110.00",
            "Fees": "0.00",
        },
        {
            "Date": "2025-01-20 10:00:00",
            "Type": "STOCK",
            "Symbol": "AAPL",
            "Side": "BUY",
            "Qty": "3",
            "Price": "91.00",
            "Fees": "0.00",
        },
    ]
    taxable_cash_rows = [
        {
            "Date": "2025-01-02 09:00:00",
            "Type": "DEPOSIT",
            "Amount": "5000.00",
            "Description": "Payroll ACH",
            "Source": "ACH",
        },
        {
            "Date": "2025-01-15 09:00:00",
            "Type": "WITHDRAWAL",
            "Amount": "300.00",
            "Description": "Internal transfer to IRA",
            "Source": "transfer",
        },
        {
            "Date": "2025-01-20 09:00:00",
            "Type": "WITHDRAWAL",
            "Amount": "100.00",
            "Description": "Bank withdrawal",
            "Source": "ACH",
        },
    ]
    ira_cash_rows = [
        {
            "Date": "2025-01-04 09:00:00",
            "Type": "DEPOSIT",
            "Amount": "700.00",
            "Description": "Initial IRA funding",
            "Source": "ACH",
        },
        {
            "Date": "2025-01-15 09:00:00",
            "Type": "DEPOSIT",
            "Amount": "300.00",
            "Description": "Internal transfer from taxable",
            "Source": "transfer",
        },
        {
            "Date": "2025-02-01 09:00:00",
            "Type": "WITHDRAWAL",
            "Amount": "50.00",
            "Description": "IRA external withdrawal",
            "Source": "ACH",
        },
    ]

    assert _import_trade_rows(
        db_session,
        account_id=taxable_id,
        broker="B1",
        source_file="taxable-trades.csv",
        file_sig="sig-taxable-trades",
        rows=taxable_trade_rows,
    ) == (4, 4)
    assert _import_trade_rows(
        db_session,
        account_id=ira_id,
        broker="B1",
        source_file="ira-trades.csv",
        file_sig="sig-ira-trades",
        rows=ira_trade_rows,
    ) == (3, 3)

    assert _import_cash_rows(
        db_session,
        account_id=taxable_id,
        broker="B1",
        rows=taxable_cash_rows,
    ) == 3
    assert _import_cash_rows(
        db_session,
        account_id=ira_id,
        broker="B1",
        rows=ira_cash_rows,
    ) == 3

    assert _import_trade_rows(
        db_session,
        account_id=taxable_id,
        broker="B1",
        source_file="taxable-trades-reimport.csv",
        file_sig="sig-taxable-trades",
        rows=taxable_trade_rows,
    ) == (0, 0)
    assert _import_trade_rows(
        db_session,
        account_id=ira_id,
        broker="B1",
        source_file="ira-trades-reimport.csv",
        file_sig="sig-ira-trades",
        rows=ira_trade_rows,
    ) == (0, 0)
    assert _import_cash_rows(
        db_session,
        account_id=taxable_id,
        broker="B1",
        rows=taxable_cash_rows,
    ) == 0
    assert _import_cash_rows(
        db_session,
        account_id=ira_id,
        broker="B1",
        rows=ira_cash_rows,
    ) == 0

    assert db_session.scalar(select(func.count()).select_from(TradeRaw)) == 7
    assert db_session.scalar(select(func.count()).select_from(TradeNormalized)) == 7
    assert db_session.scalar(select(func.count()).select_from(CashActivity)) == 6

    stats = recompute_pnl(db_session)
    assert stats["realized_rows"] == 3
    assert isclose(float(stats["unmatched_close_quantity"]), 0.0, rel_tol=0.0, abs_tol=1e-9)

    realized_by_account = dict(
        db_session.execute(
            select(PnlRealized.account_id, func.sum(PnlRealized.pnl)).group_by(PnlRealized.account_id)
        ).all()
    )
    assert isclose(float(realized_by_account[taxable_id]), -75.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(realized_by_account[ira_id]), 20.0, rel_tol=0.0, abs_tol=1e-9)

    risks = detect_wash_sale_risks(db_session, account_id=taxable_id)
    assert len(risks) == 1
    risk = risks[0]
    assert risk["symbol"] == "AAPL"
    assert risk["buy_account_id"] == ira_id
    assert risk["cross_account"]
    assert risk["ira_replacement"]
    assert int(risk["days_from_sale"]) == 10
    assert isclose(
        float(risk["allocated_replacement_quantity_equiv"]),
        3.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )

    report = generate_tax_year_report(db_session, tax_year=2025, account_id=taxable_id)
    summary = report["summary"]
    assert summary["rows"] == 2
    assert isclose(float(summary["total_proceeds"]), 1175.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(summary["total_cost_basis"]), 1250.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(summary["total_gain_or_loss_raw"]), -75.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(summary["total_gain_or_loss"]), -45.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(
        float(summary["total_wash_sale_disallowed_broker"]),
        0.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(
        float(summary["total_wash_sale_disallowed_irs"]),
        30.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(
        float(summary["wash_sale_mode_difference"]),
        30.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert bool(summary["math_check_raw"])
    assert bool(summary["math_check_adjusted"])

    detail_rows = {row["symbol"]: row for row in report["detail_rows"]}
    assert isclose(
        float(detail_rows["AAPL"]["wash_sale_disallowed_irs"]),
        30.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(float(detail_rows["AAPL"]["gain_or_loss"]), -70.0, rel_tol=0.0, abs_tol=1e-9)
    assert detail_rows["AAPL"]["adjustment_codes"] == "W"

    validation = validate_tax_report_summary(report)
    assert validation["ok"], validation

    assert isclose(net_contributions(db_session), 5550.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(
        net_contributions(db_session, account_id=taxable_id),
        4900.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(net_contributions(db_session, account_id=ira_id), 650.0, rel_tol=0.0, abs_tol=1e-9)

    realized_calendar = {
        row["close_date"]: float(row["pnl"]) for row in daily_realized_pnl(db_session)
    }
    assert realized_calendar == {date(2025, 1, 10): -100.0, date(2025, 1, 12): 45.0}

    app_totals = tax_report_totals(report["detail_rows"])
    comparison = compare_totals(
        app_totals,
        {
            "total_proceeds": 1175.0,
            "total_cost_basis": 1250.0,
            "total_gain_or_loss": -75.0,
            "short_term_gain_or_loss": -75.0,
            "long_term_gain_or_loss": 0.0,
            "total_wash_sale_disallowed": 0.0,
        },
    )
    assert isclose(float(comparison["total_proceeds"]["delta"]), 0.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(comparison["total_cost_basis"]["delta"]), 0.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(
        float(comparison["total_gain_or_loss"]["delta"]),
        30.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
    assert isclose(
        float(comparison["total_wash_sale_disallowed"]["delta"]),
        30.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    )
