"""Deterministic risk checks used by briefing and dashboards."""

from __future__ import annotations

from portfolio_assistant.db.models import OpenPositionRow


def concentration_risk(open_positions: list[OpenPositionRow], threshold: float = 0.35) -> list[str]:
    valued = [p for p in open_positions if p.market_value is not None]
    total = sum(abs(p.market_value or 0.0) for p in valued)
    if total <= 0:
        return []

    warnings: list[str] = []
    for position in valued:
        weight = abs((position.market_value or 0.0) / total)
        if weight >= threshold:
            warnings.append(
                f"{position.symbol} is {weight:.1%} of marked portfolio value, above {threshold:.0%} threshold"
            )
    return warnings
