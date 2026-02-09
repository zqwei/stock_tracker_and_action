from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.wash_sale import estimate_wash_sale_disallowance
from portfolio_assistant.db.models import PnlRealized

DATE_FROM_NOTES_RE = re.compile(r"from\s+(\d{4}-\d{2}-\d{2})")


def _parse_date_acquired(notes: str | None) -> date | None:
    if not notes:
        return None
    match = DATE_FROM_NOTES_RE.search(notes)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _holding_term(acquired: date | None, sold: date) -> str:
    if acquired is None:
        return "UNKNOWN"
    days_held = (sold - acquired).days
    return "LONG" if days_held > 365 else "SHORT"


def generate_tax_year_report(
    session: Session, tax_year: int, account_id: str | None = None
) -> dict[str, Any]:
    start = date(tax_year, 1, 1)
    end = date(tax_year, 12, 31)

    stmt = select(PnlRealized).where(
        PnlRealized.close_date >= start,
        PnlRealized.close_date <= end,
    )
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)
    stmt = stmt.order_by(PnlRealized.close_date.asc(), PnlRealized.symbol.asc(), PnlRealized.id.asc())

    records = list(session.scalars(stmt).all())
    broker_wash = estimate_wash_sale_disallowance(
        session,
        account_id=account_id,
        mode="broker",
        sale_start=start,
        sale_end=end,
    )
    irs_wash = estimate_wash_sale_disallowance(
        session,
        account_id=account_id,
        mode="irs",
        sale_start=start,
        sale_end=end,
    )
    broker_adjustments = broker_wash["sale_adjustments"]
    irs_adjustments = irs_wash["sale_adjustments"]

    rows = []
    total_raw_gain_loss = 0.0
    total_adjusted_gain_loss = 0.0
    total_proceeds = 0.0
    total_cost_basis = 0.0
    total_wash_broker = 0.0
    total_wash_irs = 0.0
    st_total = 0.0
    lt_total = 0.0
    unknown_term_total = 0.0

    for record in records:
        raw_gain_loss = float(record.pnl)
        proceeds = float(record.proceeds)
        cost_basis = float(record.cost_basis)
        wash_broker = float(broker_adjustments.get(record.id, 0.0))
        wash_irs = float(irs_adjustments.get(record.id, 0.0))
        adjusted_gain_loss = raw_gain_loss + wash_irs

        acquired_date = _parse_date_acquired(record.notes)
        holding_term = _holding_term(acquired_date, record.close_date)

        rows.append(
            {
                "description": record.symbol,
                "date_acquired": acquired_date.isoformat() if acquired_date else None,
                "date_sold": record.close_date.isoformat(),
                "symbol": record.symbol,
                "instrument_type": record.instrument_type.value,
                "term": holding_term,
                "quantity": float(record.quantity),
                "proceeds": proceeds,
                "basis": cost_basis,
                "cost_basis": cost_basis,
                "adjustment_codes": "W" if wash_irs > 0 else "",
                "adjustment_amount": wash_irs,
                "wash_sale_disallowed": wash_irs,
                "wash_sale_disallowed_broker": wash_broker,
                "wash_sale_disallowed_irs": wash_irs,
                "raw_gain_or_loss": raw_gain_loss,
                "gain_or_loss": adjusted_gain_loss,
            }
        )

        total_raw_gain_loss += raw_gain_loss
        total_adjusted_gain_loss += adjusted_gain_loss
        total_proceeds += proceeds
        total_cost_basis += cost_basis
        total_wash_broker += wash_broker
        total_wash_irs += wash_irs

        if holding_term == "SHORT":
            st_total += adjusted_gain_loss
        elif holding_term == "LONG":
            lt_total += adjusted_gain_loss
        else:
            unknown_term_total += adjusted_gain_loss

    summary = {
        "tax_year": tax_year,
        "rows": len(rows),
        "total_proceeds": total_proceeds,
        "total_cost_basis": total_cost_basis,
        "total_gain_or_loss": total_adjusted_gain_loss,
        "total_gain_or_loss_raw": total_raw_gain_loss,
        "short_term_gain_or_loss": st_total,
        "long_term_gain_or_loss": lt_total,
        "unknown_term_gain_or_loss": unknown_term_total,
        "total_wash_sale_disallowed": total_wash_irs,
        "total_wash_sale_disallowed_broker": total_wash_broker,
        "total_wash_sale_disallowed_irs": total_wash_irs,
        "wash_sale_mode_difference": total_wash_irs - total_wash_broker,
        "math_check_raw": abs((total_proceeds - total_cost_basis) - total_raw_gain_loss) <= 1e-6,
        "math_check_adjusted": abs(
            (total_raw_gain_loss + total_wash_irs) - total_adjusted_gain_loss
        )
        <= 1e-6,
        "math_check_wash_broker": abs(
            total_wash_broker - float(broker_wash["total_disallowed_loss"])
        )
        <= 1e-6,
        "math_check_wash_irs": abs(
            total_wash_irs - float(irs_wash["total_disallowed_loss"])
        )
        <= 1e-6,
    }

    return {
        "summary": summary,
        "detail_rows": rows,
        "wash_sale_summary": {
            "broker": {
                "total_disallowed_loss": broker_wash["total_disallowed_loss"],
                "sales": broker_wash["sales"],
            },
            "irs": {
                "total_disallowed_loss": irs_wash["total_disallowed_loss"],
                "sales": irs_wash["sales"],
            },
        },
    }
