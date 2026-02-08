"""Contribution calculations from cash activity rows."""

from __future__ import annotations

from dataclasses import dataclass

from portfolio_assistant.db.models import CashActivity
from portfolio_assistant.utils.money import round_money


@dataclass(slots=True)
class ContributionSummary:
    net_total: float
    by_month: list[dict[str, float | str]]
    by_account: list[dict[str, float | str]]


def _signed_amount(activity: CashActivity) -> float:
    if activity.type.upper() == "DEPOSIT":
        return activity.amount
    if activity.type.upper() == "WITHDRAWAL":
        return -activity.amount
    return 0.0


def compute_contributions(cash_rows: list[CashActivity]) -> ContributionSummary:
    by_month: dict[str, float] = {}
    by_account: dict[str, float] = {}

    net = 0.0
    for row in cash_rows:
        if not row.is_external:
            continue
        signed = _signed_amount(row)
        net += signed

        month_key = row.posted_at.strftime("%Y-%m")
        by_month[month_key] = by_month.get(month_key, 0.0) + signed
        by_account[row.account_id] = by_account.get(row.account_id, 0.0) + signed

    month_rows = [
        {"month": month, "net": round_money(value)}
        for month, value in sorted(by_month.items())
    ]
    account_rows = [
        {"account_id": account_id, "net": round_money(value)}
        for account_id, value in sorted(by_account.items())
    ]

    return ContributionSummary(
        net_total=round_money(net),
        by_month=month_rows,
        by_account=account_rows,
    )
