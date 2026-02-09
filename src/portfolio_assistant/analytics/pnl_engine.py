from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime
import re
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.lots import LOT_EPSILON, Lot, consume_fifo_with_remainder
from portfolio_assistant.db.models import PnlRealized, PositionOpen, PriceCache, TradeNormalized

OPTION_OCC_RE = re.compile(r"^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")
OPTION_SIMPLE_RE = re.compile(
    r"^([A-Z.\-]{1,10})\s+(\d{4}-\d{2}-\d{2})\s+(\d+(?:\.\d+)?)\s*([CP])$"
)
SIDE_ALIASES = {
    "B": "BUY",
    "S": "SELL",
    "BUY TO OPEN": "BTO",
    "SELL TO OPEN": "STO",
    "BUY TO CLOSE": "BTC",
    "SELL TO CLOSE": "STC",
}


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _normalize_symbol(value: str | None) -> str:
    return (value or "").strip().upper()


def _format_strike(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    strike = float(value)
    if strike.is_integer():
        return str(int(strike))
    return f"{strike:.8f}".rstrip("0").rstrip(".")


def _parse_option_symbol_raw(raw: str | None) -> tuple[str, str, str, str] | None:
    if not raw:
        return None

    canonical = " ".join(str(raw).upper().split())
    m_occ = OPTION_OCC_RE.match(canonical)
    if m_occ:
        underlying, yy, mm, dd, cp, strike_raw = m_occ.groups()
        strike = int(strike_raw) / 1000.0
        return underlying, f"20{yy}-{mm}-{dd}", _format_strike(strike), cp

    m_simple = OPTION_SIMPLE_RE.match(canonical)
    if m_simple:
        underlying, exp, strike_text, cp = m_simple.groups()
        return underlying, exp, _format_strike(float(strike_text)), cp

    return None


def _normalize_trade_side(instrument: str, side: str) -> str:
    normalized = SIDE_ALIASES.get(side, side)
    if instrument == "STOCK" and normalized in {"BTO", "BTC"}:
        return "BUY"
    if instrument == "STOCK" and normalized in {"STO", "STC"}:
        return "SELL"
    return normalized


def _option_key(trade: TradeNormalized) -> tuple[str, str]:
    symbol = _normalize_symbol(trade.underlying or trade.symbol)
    exp = trade.expiration.strftime("%Y-%m-%d") if trade.expiration else None
    cp = _enum_value(trade.call_put).upper() if trade.call_put is not None else None
    strike = _format_strike(trade.strike)
    raw_canonical = (
        " ".join(str(trade.option_symbol_raw).upper().split())
        if trade.option_symbol_raw
        else None
    )
    raw_parts = _parse_option_symbol_raw(raw_canonical)
    if raw_parts is not None:
        raw_symbol, raw_exp, raw_strike, raw_cp = raw_parts
        raw_structured = f"{raw_symbol}|{raw_exp}|{raw_strike}|{raw_cp}"
    else:
        raw_structured = None

    if exp and cp and trade.strike is not None:
        return symbol, f"{symbol}|{exp}|{strike}|{cp}"

    if exp and cp and raw_structured:
        expected_prefix = f"{symbol}|{exp}|"
        if raw_structured.startswith(expected_prefix):
            return symbol, raw_structured

    if raw_structured:
        resolved_symbol = symbol or raw_parts[0]
        return resolved_symbol, raw_structured

    if raw_canonical:
        return symbol, raw_canonical

    return symbol, f"{symbol}|UNKNOWN|{strike}|{cp or '?'}"


def _latest_price(session: Session, symbol: str) -> float | None:
    stmt = (
        select(PriceCache.close)
        .where(PriceCache.symbol == symbol)
        .order_by(PriceCache.as_of.desc())
        .limit(1)
    )
    return session.scalar(stmt)


def recompute_pnl(session: Session, account_id: str | None = None) -> dict[str, int | float]:
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

    stock_long_lots: dict[tuple[str, str], deque[Lot]] = defaultdict(deque)
    stock_short_lots: dict[tuple[str, str], deque[Lot]] = defaultdict(deque)
    option_long_lots: dict[tuple[str, str, str], deque[Lot]] = defaultdict(deque)
    option_short_lots: dict[tuple[str, str, str], deque[Lot]] = defaultdict(deque)

    realized_rows = 0
    unmatched_close_quantity = 0.0

    def _record_realized(
        *,
        trade: TradeNormalized,
        symbol: str,
        instrument_type: str,
        close_date,
        quantity: float,
        proceeds: float,
        cost_basis: float,
        fees: float,
        notes: str,
    ) -> None:
        nonlocal realized_rows
        pnl = proceeds - cost_basis
        session.add(
            PnlRealized(
                account_id=trade.account_id,
                symbol=symbol,
                instrument_type=instrument_type,
                close_date=close_date,
                quantity=quantity,
                proceeds=proceeds,
                cost_basis=cost_basis,
                fees=fees,
                pnl=pnl,
                notes=notes,
            )
        )
        realized_rows += 1

    for trade in trades:
        qty = abs(float(trade.quantity or 0.0))
        if qty <= 0:
            continue

        price = float(trade.price or 0.0)
        fees = float(trade.fees or 0.0)
        mult = int(trade.multiplier or 100)
        if mult <= 0:
            mult = 100
        instrument = _enum_value(trade.instrument_type).upper()
        side = _normalize_trade_side(instrument, _enum_value(trade.side).upper())

        symbol = _normalize_symbol(trade.symbol or trade.underlying)
        if not symbol:
            continue
        close_date = trade.executed_at.date()

        if instrument == "STOCK":
            key = (trade.account_id, symbol)

            if side == "BUY":
                consumed, remaining = consume_fifo_with_remainder(stock_short_lots[key], qty)
                for lot, take in consumed:
                    fee_alloc = fees * (take / qty)
                    open_credit = take * lot.unit_price
                    close_debit = (take * price) + fee_alloc
                    _record_realized(
                        trade=trade,
                        symbol=symbol,
                        instrument_type="STOCK",
                        close_date=close_date,
                        quantity=take,
                        proceeds=open_credit,
                        cost_basis=close_debit,
                        fees=fee_alloc,
                        notes=f"FIFO short cover from {lot.opened_at.date().isoformat()}",
                    )

                if remaining > LOT_EPSILON:
                    fee_alloc = fees * (remaining / qty)
                    unit_price = ((remaining * price) + fee_alloc) / remaining
                    stock_long_lots[key].append(
                        Lot(
                            account_id=trade.account_id,
                            symbol=symbol,
                            quantity=remaining,
                            unit_price=unit_price,
                            opened_at=trade.executed_at,
                            instrument_type="STOCK",
                            multiplier=1,
                        )
                    )
                continue

            if side == "SELL":
                consumed, remaining = consume_fifo_with_remainder(stock_long_lots[key], qty)
                for lot, take in consumed:
                    fee_alloc = fees * (take / qty)
                    proceeds = (take * price) - fee_alloc
                    cost_basis = take * lot.unit_price
                    _record_realized(
                        trade=trade,
                        symbol=symbol,
                        instrument_type="STOCK",
                        close_date=close_date,
                        quantity=take,
                        proceeds=proceeds,
                        cost_basis=cost_basis,
                        fees=fee_alloc,
                        notes=f"FIFO close from {lot.opened_at.date().isoformat()}",
                    )

                if remaining > LOT_EPSILON:
                    fee_alloc = fees * (remaining / qty)
                    unit_credit = ((remaining * price) - fee_alloc) / remaining
                    stock_short_lots[key].append(
                        Lot(
                            account_id=trade.account_id,
                            symbol=symbol,
                            quantity=remaining,
                            unit_price=unit_credit,
                            opened_at=trade.executed_at,
                            instrument_type="STOCK",
                            multiplier=1,
                        )
                    )
                continue

            unmatched_close_quantity += qty
            continue

        if instrument != "OPTION":
            continue

        option_symbol, option_contract = _option_key(trade)
        opt_key = (trade.account_id, option_symbol, option_contract)

        def _open_option_long(open_qty: float) -> None:
            if open_qty <= LOT_EPSILON:
                return
            fee_alloc = fees * (open_qty / qty)
            unit_price = ((open_qty * mult * price) + fee_alloc) / (open_qty * mult)
            option_long_lots[opt_key].append(
                Lot(
                    account_id=trade.account_id,
                    symbol=option_symbol,
                    quantity=open_qty,
                    unit_price=unit_price,
                    opened_at=trade.executed_at,
                    instrument_type="OPTION",
                    option_symbol_raw=option_contract,
                    multiplier=mult,
                )
            )

        def _open_option_short(open_qty: float) -> None:
            if open_qty <= LOT_EPSILON:
                return
            fee_alloc = fees * (open_qty / qty)
            unit_credit = ((open_qty * mult * price) - fee_alloc) / (open_qty * mult)
            option_short_lots[opt_key].append(
                Lot(
                    account_id=trade.account_id,
                    symbol=option_symbol,
                    quantity=open_qty,
                    unit_price=unit_credit,
                    opened_at=trade.executed_at,
                    instrument_type="OPTION",
                    option_symbol_raw=option_contract,
                    multiplier=mult,
                )
            )

        def _close_option_long(close_qty: float) -> float:
            consumed, remaining = consume_fifo_with_remainder(option_long_lots[opt_key], close_qty)
            for lot, take in consumed:
                fee_alloc = fees * (take / qty)
                proceeds = (take * lot.multiplier * price) - fee_alloc
                cost_basis = take * lot.multiplier * lot.unit_price
                _record_realized(
                    trade=trade,
                    symbol=option_symbol,
                    instrument_type="OPTION",
                    close_date=close_date,
                    quantity=take,
                    proceeds=proceeds,
                    cost_basis=cost_basis,
                    fees=fee_alloc,
                    notes=(
                        f"{option_contract} long close from "
                        f"{lot.opened_at.date().isoformat()}"
                    ),
                )
            return remaining

        def _close_option_short(close_qty: float) -> float:
            consumed, remaining = consume_fifo_with_remainder(option_short_lots[opt_key], close_qty)
            for lot, take in consumed:
                fee_alloc = fees * (take / qty)
                open_credit = take * lot.multiplier * lot.unit_price
                close_debit = (take * lot.multiplier * price) + fee_alloc
                _record_realized(
                    trade=trade,
                    symbol=option_symbol,
                    instrument_type="OPTION",
                    close_date=close_date,
                    quantity=take,
                    proceeds=open_credit,
                    cost_basis=close_debit,
                    fees=fee_alloc,
                    notes=(
                        f"{option_contract} short close from "
                        f"{lot.opened_at.date().isoformat()}"
                    ),
                )
            return remaining

        if side == "BUY":
            remaining = _close_option_short(qty)
            _open_option_long(remaining)
            continue

        if side == "SELL":
            remaining = _close_option_long(qty)
            _open_option_short(remaining)
            continue

        if side == "BTO":
            _open_option_long(qty)
            continue

        if side == "STO":
            _open_option_short(qty)
            continue

        if side == "STC":
            remaining = _close_option_long(qty)
            if remaining > LOT_EPSILON:
                unmatched_close_quantity += remaining
            continue

        if side == "BTC":
            remaining = _close_option_short(qty)
            if remaining > LOT_EPSILON:
                unmatched_close_quantity += remaining
            continue

        unmatched_close_quantity += qty

    open_rows = 0
    as_of = datetime.utcnow()

    all_stock_keys = set(stock_long_lots) | set(stock_short_lots)
    for acc_id, symbol in sorted(all_stock_keys):
        long_lots = stock_long_lots[(acc_id, symbol)]
        short_lots = stock_short_lots[(acc_id, symbol)]
        long_qty = sum(lot.quantity for lot in long_lots)
        short_qty = sum(lot.quantity for lot in short_lots)
        net_qty = long_qty - short_qty
        if abs(net_qty) <= LOT_EPSILON:
            continue

        long_cost = sum(lot.quantity * lot.unit_price for lot in long_lots)
        short_credit = sum(lot.quantity * lot.unit_price for lot in short_lots)
        net_cost = long_cost - short_credit
        avg_cost = net_cost / net_qty
        last_price = _latest_price(session, symbol) or avg_cost
        market_value = net_qty * last_price
        unrealized_pnl = (last_price - avg_cost) * net_qty
        session.add(
            PositionOpen(
                account_id=acc_id,
                instrument_type="STOCK",
                symbol=symbol,
                option_symbol_raw=None,
                quantity=net_qty,
                avg_cost=avg_cost,
                last_price=last_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                as_of=as_of,
            )
        )
        open_rows += 1

    all_option_keys = set(option_long_lots) | set(option_short_lots)
    for acc_id, symbol, option_contract in sorted(all_option_keys):
        long_lots = option_long_lots[(acc_id, symbol, option_contract)]
        short_lots = option_short_lots[(acc_id, symbol, option_contract)]
        multiplier = (
            next((lot.multiplier for lot in long_lots if lot.multiplier > 0), 0)
            or next((lot.multiplier for lot in short_lots if lot.multiplier > 0), 0)
            or 100
        )

        long_share_qty = sum(lot.quantity * lot.multiplier for lot in long_lots)
        short_share_qty = sum(lot.quantity * lot.multiplier for lot in short_lots)
        net_share_qty = long_share_qty - short_share_qty
        if abs(net_share_qty) <= LOT_EPSILON:
            continue

        long_cost = sum(lot.quantity * lot.multiplier * lot.unit_price for lot in long_lots)
        short_credit = sum(
            lot.quantity * lot.multiplier * lot.unit_price for lot in short_lots
        )
        net_cost = long_cost - short_credit
        avg_cost = net_cost / net_share_qty
        last_price = _latest_price(session, option_contract) or avg_cost
        market_value = net_share_qty * last_price
        unrealized_pnl = (last_price - avg_cost) * net_share_qty
        quantity_contracts = net_share_qty / multiplier
        session.add(
            PositionOpen(
                account_id=acc_id,
                instrument_type="OPTION",
                symbol=symbol,
                option_symbol_raw=option_contract,
                quantity=quantity_contracts,
                avg_cost=avg_cost,
                last_price=last_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                as_of=as_of,
            )
        )
        open_rows += 1

    return {
        "realized_rows": realized_rows,
        "open_rows": open_rows,
        "unmatched_close_quantity": unmatched_close_quantity,
    }
