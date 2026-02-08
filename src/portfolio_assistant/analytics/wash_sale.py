from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import Account, PnlRealized, TradeNormalized


def detect_wash_sale_risks(
    session: Session, account_id: str | None = None, window_days: int = 30
) -> list[dict]:
    accounts = {acc.id: acc for acc in session.scalars(select(Account)).all()}

    sale_stmt = (
        select(PnlRealized)
        .join(Account, Account.id == PnlRealized.account_id)
        .where(Account.account_type == "TAXABLE", PnlRealized.pnl < 0)
        .order_by(PnlRealized.close_date.desc())
    )
    if account_id:
        sale_stmt = sale_stmt.where(PnlRealized.account_id == account_id)

    risks: list[dict] = []

    for sale in session.scalars(sale_stmt):
        start_dt = datetime.combine(
            sale.close_date - timedelta(days=window_days), datetime.min.time()
        )
        end_dt = datetime.combine(
            sale.close_date + timedelta(days=window_days), datetime.max.time()
        )

        buy_stmt = (
            select(TradeNormalized)
            .where(
                and_(
                    TradeNormalized.executed_at >= start_dt,
                    TradeNormalized.executed_at <= end_dt,
                    TradeNormalized.side.in_(["BUY", "BTO"]),
                    or_(
                        TradeNormalized.symbol == sale.symbol,
                        TradeNormalized.underlying == sale.symbol,
                    ),
                )
            )
            .order_by(TradeNormalized.executed_at.asc())
        )
        matching_buys = list(session.scalars(buy_stmt).all())
        if not matching_buys:
            continue

        sale_account = accounts.get(sale.account_id)
        for buy in matching_buys:
            buy_account = accounts.get(buy.account_id)
            days = (buy.executed_at.date() - sale.close_date).days
            risks.append(
                {
                    "symbol": sale.symbol,
                    "sale_account_id": sale.account_id,
                    "sale_account_label": sale_account.account_label if sale_account else "",
                    "sale_date": sale.close_date.isoformat(),
                    "sale_loss": float(sale.pnl),
                    "buy_account_id": buy.account_id,
                    "buy_account_label": buy_account.account_label if buy_account else "",
                    "buy_date": buy.executed_at.date().isoformat(),
                    "days_from_sale": days,
                    "buy_side": buy.side.value if hasattr(buy.side, "value") else buy.side,
                    "buy_quantity": float(buy.quantity),
                    "buy_price": float(buy.price),
                    "buy_trade_id": buy.trade_id,
                    "buy_trade_row_id": buy.id,
                }
            )

    return risks
