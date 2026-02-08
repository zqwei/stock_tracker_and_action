from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import PnlRealized


def generate_tax_year_report(
    session: Session, tax_year: int, account_id: str | None = None
) -> dict:
    start = date(tax_year, 1, 1)
    end = date(tax_year, 12, 31)
    stmt = select(PnlRealized).where(
        PnlRealized.close_date >= start,
        PnlRealized.close_date <= end,
    )
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)
    stmt = stmt.order_by(PnlRealized.close_date.asc(), PnlRealized.symbol.asc())

    rows = []
    total_gain_loss = 0.0
    total_proceeds = 0.0
    total_cost_basis = 0.0

    for record in session.scalars(stmt):
        pnl = float(record.pnl)
        proceeds = float(record.proceeds)
        cost_basis = float(record.cost_basis)
        rows.append(
            {
                "date_sold": record.close_date.isoformat(),
                "symbol": record.symbol,
                "instrument_type": record.instrument_type.value,
                "quantity": float(record.quantity),
                "proceeds": proceeds,
                "cost_basis": cost_basis,
                "wash_sale_disallowed": 0.0,
                "gain_or_loss": pnl,
            }
        )
        total_gain_loss += pnl
        total_proceeds += proceeds
        total_cost_basis += cost_basis

    summary = {
        "tax_year": tax_year,
        "rows": len(rows),
        "total_proceeds": total_proceeds,
        "total_cost_basis": total_cost_basis,
        "total_gain_or_loss": total_gain_loss,
    }
    return {"summary": summary, "detail_rows": rows}
