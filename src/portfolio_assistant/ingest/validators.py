"""Ingestion validation helpers."""

from __future__ import annotations

from portfolio_assistant.db.models import InstrumentType, Trade, TradeSide


def normalize_instrument_type(raw: str | None) -> InstrumentType:
    if not raw:
        return InstrumentType.STOCK
    token = raw.strip().upper()
    if token in {"STOCK", "EQUITY"}:
        return InstrumentType.STOCK
    if token in {"OPTION", "OPTIONS"}:
        return InstrumentType.OPTION
    return InstrumentType.STOCK


def normalize_side(raw: str) -> TradeSide:
    token = raw.strip().upper().replace(" ", "")
    aliases = {
        "BOT": "BUY",
        "SLD": "SELL",
        "BUYTOOPEN": "BTO",
        "SELLTOOPEN": "STO",
        "BUYTOCLOSE": "BTC",
        "SELLTOCLOSE": "STC",
    }
    token = aliases.get(token, token)
    try:
        return TradeSide(token)
    except ValueError as exc:
        raise ValueError(f"Unsupported side: {raw}") from exc


def validate_trade(trade: Trade) -> list[str]:
    errors: list[str] = []
    if trade.quantity <= 0:
        errors.append("quantity must be positive")
    if trade.price < 0:
        errors.append("price cannot be negative")
    if trade.instrument_type == InstrumentType.OPTION:
        if not trade.option_symbol_raw and (not trade.underlying or trade.strike is None):
            errors.append("option trade missing contract details")
    return errors


def validate_trades(trades: list[Trade]) -> list[tuple[int, str]]:
    issues: list[tuple[int, str]] = []
    for idx, trade in enumerate(trades):
        for err in validate_trade(trade):
            issues.append((idx, err))
    return issues
