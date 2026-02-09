from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar

T = TypeVar("T")
LOT_EPSILON = 1e-12


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


def consume_fifo_with_remainder(
    lots: deque[Lot], quantity: float
) -> tuple[list[tuple[Lot, float]], float]:
    """Consume quantity from lots in FIFO order and return consumed chunks + remaining qty."""
    if quantity < 0:
        raise ValueError("quantity must be non-negative")

    remaining = quantity
    consumed: list[tuple[Lot, float]] = []

    while remaining > LOT_EPSILON and lots:
        head = lots[0]
        take = min(head.quantity, remaining)
        consumed.append((head, take))
        head.quantity -= take
        remaining -= take
        if head.quantity <= LOT_EPSILON:
            lots.popleft()

    return consumed, remaining


def consume_fifo(lots: deque[Lot], quantity: float) -> list[tuple[Lot, float]]:
    """Backward-compatible wrapper that returns consumed chunks only."""
    consumed, _ = consume_fifo_with_remainder(lots, quantity)
    return consumed
