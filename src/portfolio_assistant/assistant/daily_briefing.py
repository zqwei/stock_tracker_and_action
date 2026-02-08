"""Generate a deterministic daily briefing from computed portfolio state."""

from __future__ import annotations

from portfolio_assistant.analytics.risk_checks import concentration_risk
from portfolio_assistant.db.models import OpenPositionRow


DISCLAIMER = "Educational only, not financial or tax advice."


def build_daily_briefing(open_positions: list[OpenPositionRow]) -> dict[str, list[str] | str]:
    risks = concentration_risk(open_positions)
    highlights = [
        f"Open positions tracked: {len(open_positions)}",
        f"Concentration warnings: {len(risks)}",
    ]
    actions = risks if risks else ["No concentration alerts triggered."]
    return {
        "highlights": highlights,
        "actions": actions,
        "disclaimer": DISCLAIMER,
    }
