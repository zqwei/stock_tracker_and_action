from __future__ import annotations

from collections import defaultdict
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import CashActivity, PnlRealized


def net_contributions(session: Session, account_id: str | None = None) -> float:
    stmt = select(CashActivity).where(CashActivity.is_external.is_(True))
    if account_id:
        stmt = stmt.where(CashActivity.account_id == account_id)

    total = 0.0
    for row in session.scalars(stmt):
        if row.activity_type.value == "DEPOSIT":
            total += float(row.amount)
        else:
            total -= float(row.amount)
    return total


def contributions_by_month(
    session: Session, account_id: str | None = None
) -> list[dict[str, str | float]]:
    stmt = select(CashActivity).where(CashActivity.is_external.is_(True))
    if account_id:
        stmt = stmt.where(CashActivity.account_id == account_id)

    buckets: dict[str, float] = defaultdict(float)
    for row in session.scalars(stmt):
        key = row.posted_at.strftime("%Y-%m")
        sign = 1.0 if row.activity_type.value == "DEPOSIT" else -1.0
        buckets[key] += sign * float(row.amount)

    return [
        {"month": month, "net_contribution": amount}
        for month, amount in sorted(buckets.items())
    ]


def daily_realized_pnl(
    session: Session, account_id: str | None = None
) -> list[dict[str, date | float]]:
    stmt = select(PnlRealized)
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)

    buckets: dict[date, float] = defaultdict(float)
    for row in session.scalars(stmt):
        buckets[row.close_date] += float(row.pnl)
    return [{"close_date": d, "pnl": pnl} for d, pnl in sorted(buckets.items())]


def realized_by_symbol(
    session: Session, account_id: str | None = None
) -> list[dict[str, str | float]]:
    stmt = select(PnlRealized)
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)

    buckets: dict[tuple[str, str], float] = defaultdict(float)
    for row in session.scalars(stmt):
        key = (row.symbol, row.instrument_type.value)
        buckets[key] += float(row.pnl)
    return [
        {"symbol": symbol, "instrument_type": instrument, "realized_pnl": pnl}
        for (symbol, instrument), pnl in sorted(buckets.items())
    ]
