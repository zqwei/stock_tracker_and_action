"""Basic wash-sale risk detection across accounts."""

from __future__ import annotations

from datetime import timedelta

from portfolio_assistant.db.models import AccountType, RealizedPnLRow, Trade, TradeSide, WashSaleRiskRow


def detect_wash_sale_risks(
    realized_rows: list[RealizedPnLRow],
    trades: list[Trade],
    window_days: int = 30,
) -> list[WashSaleRiskRow]:
    risks: list[WashSaleRiskRow] = []
    buys = [
        trade
        for trade in trades
        if trade.side in {TradeSide.BUY, TradeSide.BTO} and trade.symbol
    ]

    for row in realized_rows:
        if row.account_type != AccountType.TAXABLE:
            continue
        if row.realized_pnl >= 0:
            continue

        start = row.closed_at - timedelta(days=window_days)
        end = row.closed_at + timedelta(days=window_days)

        for buy in buys:
            if buy.executed_at < start or buy.executed_at > end:
                continue
            if buy.symbol.upper() != row.symbol.upper():
                continue

            risks.append(
                WashSaleRiskRow(
                    symbol=row.symbol,
                    loss_sale_date=row.closed_at,
                    replacement_buy_date=buy.executed_at,
                    sale_account_id=row.account_id,
                    replacement_account_id=buy.account_id,
                    sale_account_type=row.account_type,
                    replacement_account_type=buy.account_type,
                    loss_amount=abs(row.realized_pnl),
                    notes=(
                        "Replacement buy within +/-30 days of taxable loss sale. "
                        "Review for potential wash-sale treatment."
                    ),
                    sale_trade_id=row.close_trade_id,
                    replacement_trade_id=buy.trade_id,
                )
            )

    return sorted(risks, key=lambda item: (item.loss_sale_date, item.replacement_buy_date, item.symbol))


def apply_wash_sale_flags(
    realized_rows: list[RealizedPnLRow],
    risks: list[WashSaleRiskRow],
) -> list[RealizedPnLRow]:
    """Mark loss-sale rows that have at least one detected replacement buy."""
    flagged = {(risk.symbol, risk.sale_account_id, risk.loss_sale_date) for risk in risks}
    for row in realized_rows:
        if (row.symbol, row.account_id, row.closed_at) in flagged:
            row.is_wash_sale = True
    return realized_rows
