from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import CashActivity, PnlRealized


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return float(text)
        except ValueError:
            return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
        "total_wash_sale_disallowed_broker": 0.0,
        "total_wash_sale_disallowed_irs": 0.0,
        "wash_sale_mode_difference": 0.0,
    }

    for row in detail_rows:
        proceeds = _as_float(row.get("proceeds"), 0.0)

        cost_basis_value = row.get("cost_basis")
        if cost_basis_value is None:
            cost_basis_value = row.get("basis")
        cost_basis = _as_float(cost_basis_value, 0.0)

        gain_or_loss = _as_float(row.get("gain_or_loss"), 0.0)

        raw_gain_or_loss_value = row.get("raw_gain_or_loss")
        if raw_gain_or_loss_value is None:
            raw_gain_or_loss_value = gain_or_loss
        raw_gain_or_loss = _as_float(raw_gain_or_loss_value, gain_or_loss)

        wash_disallowed = _as_float(row.get("wash_sale_disallowed"), 0.0)
        wash_disallowed_broker = _as_float(
            row.get("wash_sale_disallowed_broker"),
            wash_disallowed,
        )
        wash_disallowed_irs = _as_float(
            row.get("wash_sale_disallowed_irs"),
            wash_disallowed,
        )
        term = str(row.get("term", "UNKNOWN") or "UNKNOWN").upper()

        totals["total_proceeds"] += proceeds
        totals["total_cost_basis"] += cost_basis
        totals["total_gain_or_loss"] += gain_or_loss
        totals["total_gain_or_loss_raw"] += raw_gain_or_loss
        totals["total_wash_sale_disallowed"] += wash_disallowed
        totals["total_wash_sale_disallowed_broker"] += wash_disallowed_broker
        totals["total_wash_sale_disallowed_irs"] += wash_disallowed_irs

        if term == "SHORT":
            totals["short_term_gain_or_loss"] += gain_or_loss
        elif term == "LONG":
            totals["long_term_gain_or_loss"] += gain_or_loss
        else:
            totals["unknown_term_gain_or_loss"] += gain_or_loss

    totals["wash_sale_mode_difference"] = (
        totals["total_wash_sale_disallowed_irs"]
        - totals["total_wash_sale_disallowed_broker"]
    )
    return totals


def validate_tax_report_summary(report: dict[str, Any], tolerance: float = 1e-6) -> dict[str, Any]:
    detail = report.get("detail_rows") or []
    summary = report.get("summary") or {}
    recomputed = tax_report_totals(detail)

    checks: dict[str, dict[str, float | bool]] = {}
    for key, recomputed_value in recomputed.items():
        summary_value = _as_float(summary.get(key), 0.0)
        delta = summary_value - recomputed_value
        checks[key] = {
            "summary": summary_value,
            "recomputed": recomputed_value,
            "delta": delta,
            "ok": abs(delta) <= tolerance,
        }

    recomputed_math_raw = (
        abs(
            (recomputed["total_proceeds"] - recomputed["total_cost_basis"])
            - recomputed["total_gain_or_loss_raw"]
        )
        <= tolerance
    )
    recomputed_math_adjusted = (
        abs(
            (recomputed["total_gain_or_loss_raw"] + recomputed["total_wash_sale_disallowed_irs"])
            - recomputed["total_gain_or_loss"]
        )
        <= tolerance
    )
    summary_math_raw = bool(summary.get("math_check_raw", recomputed_math_raw))
    summary_math_adjusted = bool(
        summary.get("math_check_adjusted", recomputed_math_adjusted)
    )
    checks["math_check_raw"] = {
        "summary": summary_math_raw,
        "recomputed": recomputed_math_raw,
        "delta": 0.0 if summary_math_raw == recomputed_math_raw else 1.0,
        "ok": summary_math_raw == recomputed_math_raw,
    }
    checks["math_check_adjusted"] = {
        "summary": summary_math_adjusted,
        "recomputed": recomputed_math_adjusted,
        "delta": 0.0 if summary_math_adjusted == recomputed_math_adjusted else 1.0,
        "ok": summary_math_adjusted == recomputed_math_adjusted,
    }

    checks["rows"] = {
        "summary": _as_float(summary.get("rows"), 0.0),
        "recomputed": float(len(detail)),
        "delta": _as_float(summary.get("rows"), 0.0) - float(len(detail)),
        "ok": int(_as_float(summary.get("rows"), 0.0)) == len(detail),
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
