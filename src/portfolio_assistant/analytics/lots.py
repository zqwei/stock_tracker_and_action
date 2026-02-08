from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar

T = TypeVar("T")


@dataclass
class Lot:
    account_id: str
    symbol: str
    quantity: float
    unit_price: float
    opened_at: datetime
    instrument_type: str = "STOCK"
    option_symbol_raw: str | None = None
    multiplier: int = 1


def consume_fifo(lots: deque[Lot], quantity: float) -> list[tuple[Lot, float]]:
    """Consume quantity from lots in FIFO order and return (lot, consumed_qty) chunks."""
    remaining = quantity
    consumed: list[tuple[Lot, float]] = []

    while remaining > 1e-12 and lots:
        head = lots[0]
        take = min(head.quantity, remaining)
        consumed.append((head, take))
        head.quantity -= take
        remaining -= take
        if head.quantity <= 1e-12:
            lots.popleft()

    return consumed
