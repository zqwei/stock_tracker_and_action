"""FIFO lot matching engine for stocks and options."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from portfolio_assistant.db.models import AccountType, InstrumentType, RealizedPnLRow, Trade, TradeSide
from portfolio_assistant.utils.money import round_money


@dataclass(slots=True)
class OpenLot:
    symbol: str
    account_id: str
    account_type: AccountType
    instrument_type: InstrumentType
    opened_at: datetime
    quantity_remaining: float
    unit_price: float
    multiplier: int
    direction: str  # LONG or SHORT
    fee_per_unit: float


class FIFOLotEngine:
    def __init__(self) -> None:
        self._open_lots: dict[tuple[str, str, InstrumentType, str], list[OpenLot]] = {}
        self.realized_rows: list[RealizedPnLRow] = []

    @staticmethod
    def _direction_for_open(trade: Trade) -> str | None:
        if trade.instrument_type == InstrumentType.STOCK and trade.side == TradeSide.BUY:
            return "LONG"
        if trade.instrument_type == InstrumentType.OPTION and trade.side == TradeSide.BTO:
            return "LONG"
        if trade.instrument_type == InstrumentType.OPTION and trade.side == TradeSide.STO:
            return "SHORT"
        return None

    @staticmethod
    def _direction_for_close(trade: Trade) -> str | None:
        if trade.instrument_type == InstrumentType.STOCK and trade.side == TradeSide.SELL:
            return "LONG"
        if trade.instrument_type == InstrumentType.OPTION and trade.side == TradeSide.STC:
            return "LONG"
        if trade.instrument_type == InstrumentType.OPTION and trade.side == TradeSide.BTC:
            return "SHORT"
        return None

    @staticmethod
    def _key(trade: Trade, direction: str) -> tuple[str, str, InstrumentType, str]:
        return (trade.account_id, trade.contract_symbol(), trade.instrument_type, direction)

    def process_trades(self, trades: list[Trade]) -> list[RealizedPnLRow]:
        for trade in sorted(trades, key=lambda item: item.executed_at):
            self.process_trade(trade)
        return self.realized_rows

    def process_trade(self, trade: Trade) -> None:
        open_direction = self._direction_for_open(trade)
        if open_direction:
            self._open_trade(trade, open_direction)
            return

        close_direction = self._direction_for_close(trade)
        if close_direction:
            self._close_trade(trade, close_direction)
            return

        raise ValueError(f"Unsupported side for lot engine: {trade.side}")

    def _open_trade(self, trade: Trade, direction: str) -> None:
        key = self._key(trade, direction)
        lots = self._open_lots.setdefault(key, [])
        fee_per_unit = (trade.fees / trade.quantity) if trade.quantity else 0.0
        lots.append(
            OpenLot(
                symbol=trade.contract_symbol(),
                account_id=trade.account_id,
                account_type=trade.account_type,
                instrument_type=trade.instrument_type,
                opened_at=trade.executed_at,
                quantity_remaining=trade.quantity,
                unit_price=trade.price,
                multiplier=trade.multiplier,
                direction=direction,
                fee_per_unit=fee_per_unit,
            )
        )

    def _close_trade(self, trade: Trade, direction: str) -> None:
        key = self._key(trade, direction)
        queue = self._open_lots.get(key, [])
        if not queue:
            raise ValueError(
                f"Attempted to close {trade.contract_symbol()} in {trade.account_id} with no open lots"
            )

        remaining = trade.quantity
        close_fee_per_unit = (trade.fees / trade.quantity) if trade.quantity else 0.0

        while remaining > 0 and queue:
            lot = queue[0]
            matched_qty = min(remaining, lot.quantity_remaining)
            open_fee = lot.fee_per_unit * matched_qty
            close_fee = close_fee_per_unit * matched_qty
            total_fee = open_fee + close_fee

            if direction == "LONG":
                proceeds = matched_qty * trade.price * lot.multiplier
                cost_basis = matched_qty * lot.unit_price * lot.multiplier
                realized = proceeds - cost_basis - total_fee
            else:
                proceeds = matched_qty * lot.unit_price * lot.multiplier
                cost_basis = matched_qty * trade.price * lot.multiplier
                realized = proceeds - cost_basis - total_fee

            self.realized_rows.append(
                RealizedPnLRow(
                    symbol=trade.contract_symbol(),
                    account_id=trade.account_id,
                    account_type=trade.account_type,
                    instrument_type=trade.instrument_type,
                    opened_at=lot.opened_at,
                    closed_at=trade.executed_at,
                    quantity=round_money(matched_qty),
                    proceeds=round_money(proceeds),
                    cost_basis=round_money(cost_basis),
                    fees=round_money(total_fee),
                    realized_pnl=round_money(realized),
                    holding_days=max((trade.executed_at.date() - lot.opened_at.date()).days, 0),
                    close_trade_id=trade.trade_id,
                )
            )

            lot.quantity_remaining = round_money(lot.quantity_remaining - matched_qty)
            remaining = round_money(remaining - matched_qty)

            if lot.quantity_remaining <= 0:
                queue.pop(0)

        if remaining > 0:
            raise ValueError(
                f"Close quantity {trade.quantity} exceeds open lots for {trade.contract_symbol()}"
            )

    def iter_open_lots(self) -> list[OpenLot]:
        lots: list[OpenLot] = []
        for queue in self._open_lots.values():
            lots.extend([lot for lot in queue if lot.quantity_remaining > 0])
        return lots
