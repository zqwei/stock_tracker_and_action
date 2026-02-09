from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

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


def tax_report_totals(detail_rows: list[dict[str, Any]]) -> dict[str, float]:
    totals = {
        "total_proceeds": 0.0,
        "total_cost_basis": 0.0,
        "total_gain_or_loss": 0.0,
        "total_gain_or_loss_raw": 0.0,
        "short_term_gain_or_loss": 0.0,
        "long_term_gain_or_loss": 0.0,
        "unknown_term_gain_or_loss": 0.0,
        "total_wash_sale_disallowed": 0.0,
    }

    for row in detail_rows:
        proceeds = float(row.get("proceeds", 0.0) or 0.0)
        cost_basis = float(row.get("cost_basis", row.get("basis", 0.0)) or 0.0)
        gain_or_loss = float(row.get("gain_or_loss", 0.0) or 0.0)
        raw_gain_or_loss = float(row.get("raw_gain_or_loss", gain_or_loss) or gain_or_loss)
        wash_disallowed = float(row.get("wash_sale_disallowed", 0.0) or 0.0)
        term = str(row.get("term", "UNKNOWN") or "UNKNOWN").upper()

        totals["total_proceeds"] += proceeds
        totals["total_cost_basis"] += cost_basis
        totals["total_gain_or_loss"] += gain_or_loss
        totals["total_gain_or_loss_raw"] += raw_gain_or_loss
        totals["total_wash_sale_disallowed"] += wash_disallowed

        if term == "SHORT":
            totals["short_term_gain_or_loss"] += gain_or_loss
        elif term == "LONG":
            totals["long_term_gain_or_loss"] += gain_or_loss
        else:
            totals["unknown_term_gain_or_loss"] += gain_or_loss

    return totals


def validate_tax_report_summary(report: dict[str, Any], tolerance: float = 1e-6) -> dict[str, Any]:
    detail = report.get("detail_rows") or []
    summary = report.get("summary") or {}
    recomputed = tax_report_totals(detail)

    checks: dict[str, dict[str, float | bool]] = {}
    for key, recomputed_value in recomputed.items():
        summary_value = float(summary.get(key, 0.0) or 0.0)
        delta = summary_value - recomputed_value
        checks[key] = {
            "summary": summary_value,
            "recomputed": recomputed_value,
            "delta": delta,
            "ok": abs(delta) <= tolerance,
        }

    checks["rows"] = {
        "summary": float(summary.get("rows", 0.0) or 0.0),
        "recomputed": float(len(detail)),
        "delta": float(summary.get("rows", 0.0) or 0.0) - float(len(detail)),
        "ok": int(summary.get("rows", 0) or 0) == len(detail),
    }

    return {
        "ok": all(bool(result["ok"]) for result in checks.values()),
        "checks": checks,
    }


def compare_totals(
    app_totals: dict[str, float], broker_totals: dict[str, float]
) -> dict[str, dict[str, float]]:
    keys = [
        "total_proceeds",
        "total_cost_basis",
        "total_gain_or_loss",
        "short_term_gain_or_loss",
        "long_term_gain_or_loss",
        "total_wash_sale_disallowed",
    ]
    comparison: dict[str, dict[str, float]] = {}
    for key in keys:
        app_value = float(app_totals.get(key, 0.0) or 0.0)
        broker_value = float(broker_totals.get(key, 0.0) or 0.0)
        comparison[key] = {
            "app": app_value,
            "broker": broker_value,
            "delta": app_value - broker_value,
        }
    return comparison
