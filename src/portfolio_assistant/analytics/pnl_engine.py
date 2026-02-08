from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.lots import Lot, consume_fifo
from portfolio_assistant.db.models import PnlRealized, PositionOpen, PriceCache, TradeNormalized


def _option_key(trade: TradeNormalized) -> tuple[str, str]:
    symbol = (trade.underlying or trade.symbol or "").upper()
    if trade.option_symbol_raw:
        return symbol, trade.option_symbol_raw

    exp = trade.expiration.strftime("%Y-%m-%d") if trade.expiration else "UNKNOWN"
    strike = f"{trade.strike}" if trade.strike is not None else "UNKNOWN"
    cp = trade.call_put.value if trade.call_put is not None else "?"
    synthetic = f"{symbol}|{exp}|{strike}|{cp}"
    return symbol, synthetic


def _latest_price(session: Session, symbol: str) -> float | None:
    stmt = (
        select(PriceCache.close)
        .where(PriceCache.symbol == symbol)
        .order_by(PriceCache.as_of.desc())
        .limit(1)
    )
    return session.scalar(stmt)


def recompute_pnl(session: Session, account_id: str | None = None) -> dict[str, int]:
    if account_id:
        session.execute(delete(PnlRealized).where(PnlRealized.account_id == account_id))
        session.execute(delete(PositionOpen).where(PositionOpen.account_id == account_id))
    else:
        session.execute(delete(PnlRealized))
        session.execute(delete(PositionOpen))

    trade_stmt = select(TradeNormalized).order_by(
        TradeNormalized.executed_at, TradeNormalized.id
    )
    if account_id:
        trade_stmt = trade_stmt.where(TradeNormalized.account_id == account_id)
    trades = list(session.scalars(trade_stmt).all())

    stock_lots: dict[tuple[str, str], deque[Lot]] = defaultdict(deque)
    option_long_lots: dict[tuple[str, str, str], deque[Lot]] = defaultdict(deque)
    option_short_lots: dict[tuple[str, str, str], deque[Lot]] = defaultdict(deque)

    realized_rows = 0

    for trade in trades:
        qty = abs(float(trade.quantity or 0.0))
        if qty <= 0:
            continue

        side = trade.side.value if hasattr(trade.side, "value") else str(trade.side)
        side = side.upper()
        price = float(trade.price or 0.0)
        fees = float(trade.fees or 0.0)
        mult = int(trade.multiplier or 1)
        instrument = (
            trade.instrument_type.value
            if hasattr(trade.instrument_type, "value")
            else str(trade.instrument_type)
        )
        instrument = instrument.upper()

        if instrument == "OPTION" and side == "BUY":
            side = "BTO"
        elif instrument == "OPTION" and side == "SELL":
            side = "STC"

        if instrument == "STOCK" and side in {"BTO", "BTC"}:
            side = "BUY"
        elif instrument == "STOCK" and side in {"STO", "STC"}:
            side = "SELL"

        symbol = (trade.symbol or trade.underlying or "").upper()
        close_date = trade.executed_at.date()

        if instrument == "STOCK":
            key = (trade.account_id, symbol)
            if side == "BUY":
                unit_price = ((qty * price) + fees) / qty
                stock_lots[key].append(
                    Lot(
                        account_id=trade.account_id,
                        symbol=symbol,
                        quantity=qty,
                        unit_price=unit_price,
                        opened_at=trade.executed_at,
                        instrument_type="STOCK",
                        multiplier=1,
                    )
                )
                continue

            if side == "SELL":
                consumed = consume_fifo(stock_lots[key], qty)
                for lot, take in consumed:
                    fee_alloc = fees * (take / qty)
                    proceeds = (take * price) - fee_alloc
                    cost_basis = take * lot.unit_price
                    pnl = proceeds - cost_basis
                    session.add(
                        PnlRealized(
                            account_id=trade.account_id,
                            symbol=symbol,
                            instrument_type="STOCK",
                            close_date=close_date,
                            quantity=take,
                            proceeds=proceeds,
                            cost_basis=cost_basis,
                            fees=fee_alloc,
                            pnl=pnl,
                            notes=f"FIFO close from {lot.opened_at.date().isoformat()}",
                        )
                    )
                    realized_rows += 1
                continue

        if instrument == "OPTION":
            option_symbol, option_key = _option_key(trade)
            opt_key = (trade.account_id, option_symbol, option_key)

            if side == "BTO":
                unit_price = ((qty * mult * price) + fees) / (qty * mult)
                option_long_lots[opt_key].append(
                    Lot(
                        account_id=trade.account_id,
                        symbol=option_symbol,
                        quantity=qty,
                        unit_price=unit_price,
                        opened_at=trade.executed_at,
                        instrument_type="OPTION",
                        option_symbol_raw=option_key,
                        multiplier=mult,
                    )
                )
                continue

            if side == "STO":
                unit_credit = ((qty * mult * price) - fees) / (qty * mult)
                option_short_lots[opt_key].append(
                    Lot(
                        account_id=trade.account_id,
                        symbol=option_symbol,
                        quantity=qty,
                        unit_price=unit_credit,
                        opened_at=trade.executed_at,
                        instrument_type="OPTION",
                        option_symbol_raw=option_key,
                        multiplier=mult,
                    )
                )
                continue

            if side == "STC":
                consumed = consume_fifo(option_long_lots[opt_key], qty)
                for lot, take in consumed:
                    fee_alloc = fees * (take / qty)
                    proceeds = (take * mult * price) - fee_alloc
                    cost_basis = take * mult * lot.unit_price
                    pnl = proceeds - cost_basis
                    session.add(
                        PnlRealized(
                            account_id=trade.account_id,
                            symbol=option_symbol,
                            instrument_type="OPTION",
                            close_date=close_date,
                            quantity=take,
                            proceeds=proceeds,
                            cost_basis=cost_basis,
                            fees=fee_alloc,
                            pnl=pnl,
                            notes=f"{option_key} long close",
                        )
                    )
                    realized_rows += 1
                continue

            if side == "BTC":
                consumed = consume_fifo(option_short_lots[opt_key], qty)
                for lot, take in consumed:
                    fee_alloc = fees * (take / qty)
                    open_credit = take * mult * lot.unit_price
                    close_debit = (take * mult * price) + fee_alloc
                    pnl = open_credit - close_debit
                    session.add(
                        PnlRealized(
                            account_id=trade.account_id,
                            symbol=option_symbol,
                            instrument_type="OPTION",
                            close_date=close_date,
                            quantity=take,
                            proceeds=open_credit,
                            cost_basis=close_debit,
                            fees=fee_alloc,
                            pnl=pnl,
                            notes=f"{option_key} short close",
                        )
                    )
                    realized_rows += 1
                continue

    open_rows = 0
    as_of = datetime.utcnow()

    for (acc_id, symbol), lots in stock_lots.items():
        qty = sum(lot.quantity for lot in lots)
        if qty <= 1e-12:
            continue
        total_cost = sum(lot.quantity * lot.unit_price for lot in lots)
        avg_cost = total_cost / qty
        last_price = _latest_price(session, symbol) or avg_cost
        market_value = qty * last_price
        unrealized_pnl = (last_price - avg_cost) * qty
        session.add(
            PositionOpen(
                account_id=acc_id,
                instrument_type="STOCK",
                symbol=symbol,
                option_symbol_raw=None,
                quantity=qty,
                avg_cost=avg_cost,
                last_price=last_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                as_of=as_of,
            )
        )
        open_rows += 1

    for (acc_id, symbol, option_key), lots in option_long_lots.items():
        qty = sum(lot.quantity for lot in lots)
        if qty <= 1e-12:
            continue
        multiplier = lots[0].multiplier if lots else 100
        total_cost = sum(lot.quantity * lot.unit_price for lot in lots)
        avg_cost = total_cost / qty
        last_price = _latest_price(session, option_key) or avg_cost
        market_value = qty * multiplier * last_price
        unrealized_pnl = (last_price - avg_cost) * qty * multiplier
        session.add(
            PositionOpen(
                account_id=acc_id,
                instrument_type="OPTION",
                symbol=symbol,
                option_symbol_raw=option_key,
                quantity=qty,
                avg_cost=avg_cost,
                last_price=last_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                as_of=as_of,
            )
        )
        open_rows += 1

    for (acc_id, symbol, option_key), lots in option_short_lots.items():
        qty = sum(lot.quantity for lot in lots)
        if qty <= 1e-12:
            continue
        multiplier = lots[0].multiplier if lots else 100
        avg_credit = sum(lot.quantity * lot.unit_price for lot in lots) / qty
        last_price = _latest_price(session, option_key) or avg_credit
        signed_qty = -qty
        market_value = signed_qty * multiplier * last_price
        unrealized_pnl = (avg_credit - last_price) * qty * multiplier
        session.add(
            PositionOpen(
                account_id=acc_id,
                instrument_type="OPTION",
                symbol=symbol,
                option_symbol_raw=option_key,
                quantity=signed_qty,
                avg_cost=avg_credit,
                last_price=last_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                as_of=as_of,
            )
        )
        open_rows += 1

    return {"realized_rows": realized_rows, "open_rows": open_rows}
