"""Portfolio P&L computations built on top of FIFO lot matching."""

from __future__ import annotations

from dataclasses import dataclass

from portfolio_assistant.analytics.lots import FIFOLotEngine
from portfolio_assistant.db.models import OpenPositionRow, RealizedPnLRow, Trade
from portfolio_assistant.utils.money import round_money


@dataclass(slots=True)
class PnLComputation:
    realized: list[RealizedPnLRow]
    open_positions: list[OpenPositionRow]


def compute_realized_and_open_positions(
    trades: list[Trade],
    latest_quotes: dict[str, float] | None = None,
) -> PnLComputation:
    engine = FIFOLotEngine()
    realized = engine.process_trades(trades)

    quotes = latest_quotes or {}
    aggregate: dict[tuple[str, str, str], dict[str, float | None | str]] = {}

    for lot in engine.iter_open_lots():
        key = (lot.account_id, lot.symbol, lot.instrument_type.value)
        signed_qty = lot.quantity_remaining if lot.direction == "LONG" else -lot.quantity_remaining
        signed_cost = signed_qty * lot.unit_price * lot.multiplier

        row = aggregate.setdefault(
            key,
            {
                "symbol": lot.symbol,
                "account_id": lot.account_id,
                "account_type": lot.account_type,
                "instrument_type": lot.instrument_type,
                "signed_qty": 0.0,
                "signed_cost": 0.0,
            },
        )
        row["signed_qty"] = float(row["signed_qty"]) + signed_qty
        row["signed_cost"] = float(row["signed_cost"]) + signed_cost

    open_rows: list[OpenPositionRow] = []
    for row in aggregate.values():
        signed_qty = float(row["signed_qty"])
        if signed_qty == 0:
            continue

        signed_cost = float(row["signed_cost"])
        avg_cost = abs(signed_cost / signed_qty)
        mark = quotes.get(str(row["symbol"]))
        market_value = round_money(mark * signed_qty) if mark is not None else None
        unrealized = round_money(market_value - signed_cost) if market_value is not None else None

        open_rows.append(
            OpenPositionRow(
                symbol=str(row["symbol"]),
                account_id=str(row["account_id"]),
                account_type=row["account_type"],
                instrument_type=row["instrument_type"],
                quantity=round_money(signed_qty),
                average_cost=round_money(avg_cost),
                mark_price=mark,
                market_value=market_value,
                unrealized_pnl=unrealized,
            )
        )

    return PnLComputation(realized=realized, open_positions=open_rows)
