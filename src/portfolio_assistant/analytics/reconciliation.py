"""Helpers to reconcile local tax totals against broker exports."""

from __future__ import annotations

from dataclasses import dataclass

from portfolio_assistant.utils.money import round_money


@dataclass(slots=True)
class ReconciliationRow:
    metric: str
    local_value: float
    broker_value: float
    difference: float
    within_tolerance: bool


def reconcile_totals(
    local_totals: dict[str, float],
    broker_totals: dict[str, float],
    tolerance: float = 1.0,
) -> list[ReconciliationRow]:
    rows: list[ReconciliationRow] = []
    all_metrics = sorted(set(local_totals) | set(broker_totals))

    for metric in all_metrics:
        local = round_money(local_totals.get(metric, 0.0))
        broker = round_money(broker_totals.get(metric, 0.0))
        diff = round_money(local - broker)
        rows.append(
            ReconciliationRow(
                metric=metric,
                local_value=local,
                broker_value=broker,
                difference=diff,
                within_tolerance=abs(diff) <= tolerance,
            )
        )

    return rows
