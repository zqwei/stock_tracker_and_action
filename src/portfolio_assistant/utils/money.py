from __future__ import annotations

from typing import Any


def safe_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def format_money(value: Any, *, signed: bool = True, precision: int = 2) -> str:
    amount = safe_float(value)
    sign = "+" if signed and amount > 0 else ""
    return f"{sign}{amount:,.{precision}f}"


def format_percent(
    value: Any | None,
    *,
    signed: bool = True,
    precision: int = 2,
    ratio: bool = True,
) -> str:
    if value is None:
        return "n/a"
    pct = safe_float(value) * 100.0 if ratio else safe_float(value)
    sign = "+" if signed and pct > 0 else ""
    return f"{sign}{pct:.{precision}f}%"
