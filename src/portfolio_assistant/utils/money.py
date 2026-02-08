"""Money helpers for deterministic rounding and parsing."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


CENT = Decimal("0.01")


def to_decimal(value: float | int | str | Decimal | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def round_money(value: float | int | str | Decimal | None) -> float:
    return float(to_decimal(value).quantize(CENT, rounding=ROUND_HALF_UP))


def signed_cash_inflow(side: str, gross_notional: float, fees: float = 0.0) -> float:
    """Return signed cash amount where positive means cash into account."""
    side_upper = side.upper()
    if side_upper in {"SELL", "STC", "STO"}:
        return round_money(gross_notional - fees)
    if side_upper in {"BUY", "BTO", "BTC"}:
        return round_money(-(gross_notional + fees))
    raise ValueError(f"Unsupported side: {side}")
