from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.wash_sale import detect_wash_sale_risks
from portfolio_assistant.db.models import CashActivity, PositionOpen


@dataclass(frozen=True)
class RiskCheck:
    key: str
    severity: str
    title: str
    detail: str
    recommendation: str
    metrics: dict[str, Any]


_SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}


def _rank(check: RiskCheck) -> tuple[int, str]:
    return (_SEVERITY_ORDER.get(check.severity, 3), check.key)


def run_deterministic_risk_checks(
    session: Session,
    *,
    account_id: str | None = None,
    concentration_threshold: float = 0.40,
    unrealized_loss_threshold: float = 1000.0,
) -> list[dict[str, Any]]:
    checks: list[RiskCheck] = []

    unknown_tag_stmt = select(func.count()).select_from(CashActivity).where(
        CashActivity.is_external.is_(None)
    )
    if account_id:
        unknown_tag_stmt = unknown_tag_stmt.where(CashActivity.account_id == account_id)
    unknown_external_tags = int(session.scalar(unknown_tag_stmt) or 0)
    if unknown_external_tags > 0:
        checks.append(
            RiskCheck(
                key="cash_external_tagging",
                severity="medium",
                title="Cash rows need external/internal tagging",
                detail=(
                    f"{unknown_external_tags} cash activity row(s) are still untagged, "
                    "which can distort contribution and return metrics."
                ),
                recommendation=(
                    "Review Import Cash / Data Quality and classify each row as external "
                    "or internal transfer."
                ),
                metrics={"unknown_external_rows": unknown_external_tags},
            )
        )

    wash_risks = detect_wash_sale_risks(session, account_id=account_id, window_days=30)
    if wash_risks:
        ira_count = sum(1 for row in wash_risks if bool(row.get("ira_replacement")))
        cross_count = sum(1 for row in wash_risks if bool(row.get("cross_account")))
        severity = "high" if ira_count > 0 else "medium"
        checks.append(
            RiskCheck(
                key="wash_sale_replacements",
                severity=severity,
                title="Potential wash-sale replacement matches",
                detail=(
                    f"{len(wash_risks)} replacement match(es) detected, "
                    f"{cross_count} cross-account and {ira_count} IRA-related."
                ),
                recommendation=(
                    "Review the Wash Sale Risk page and reconcile trades before tax filing. "
                    "Use informational flags only, not tax advice."
                ),
                metrics={
                    "matches": len(wash_risks),
                    "cross_account_matches": cross_count,
                    "ira_matches": ira_count,
                },
            )
        )

    position_stmt = select(PositionOpen)
    if account_id:
        position_stmt = position_stmt.where(PositionOpen.account_id == account_id)
    positions = list(session.scalars(position_stmt).all())

    symbol_mv: dict[str, float] = {}
    for position in positions:
        if position.market_value is None:
            continue
        symbol_mv[position.symbol] = symbol_mv.get(position.symbol, 0.0) + abs(
            float(position.market_value)
        )

    total_mv = sum(symbol_mv.values())
    if total_mv > 0.0 and symbol_mv:
        top_symbol, top_value = max(symbol_mv.items(), key=lambda item: item[1])
        concentration = top_value / total_mv
        if concentration >= concentration_threshold:
            severity = "high" if concentration >= 0.60 else "medium"
            checks.append(
                RiskCheck(
                    key="position_concentration",
                    severity=severity,
                    title="Single-symbol concentration is elevated",
                    detail=(
                        f"{top_symbol} is {concentration:.1%} of tracked open market value "
                        f"(threshold {concentration_threshold:.0%})."
                    ),
                    recommendation=(
                        "Review sizing and downside impact for concentrated positions "
                        "before adding new risk."
                    ),
                    metrics={
                        "top_symbol": top_symbol,
                        "top_symbol_market_value": top_value,
                        "total_market_value": total_mv,
                        "concentration_ratio": concentration,
                    },
                )
            )

    max_loss_symbol: str | None = None
    max_loss_value = 0.0
    stale_price_count = 0
    for position in positions:
        unrealized = float(position.unrealized_pnl or 0.0)
        if unrealized < max_loss_value:
            max_loss_value = unrealized
            max_loss_symbol = position.symbol
        if position.last_price is None:
            stale_price_count += 1

    if max_loss_symbol and abs(max_loss_value) >= unrealized_loss_threshold:
        checks.append(
            RiskCheck(
                key="large_unrealized_loss",
                severity="medium",
                title="Large unrealized loss position",
                detail=(
                    f"{max_loss_symbol} unrealized loss is {max_loss_value:,.2f}, "
                    f"exceeding threshold {unrealized_loss_threshold:,.2f}."
                ),
                recommendation=(
                    "Re-check thesis, event risk, and sizing. Avoid forced actions; "
                    "this is an informational flag only."
                ),
                metrics={
                    "symbol": max_loss_symbol,
                    "unrealized_pnl": max_loss_value,
                    "threshold": unrealized_loss_threshold,
                },
            )
        )

    if stale_price_count > 0:
        checks.append(
            RiskCheck(
                key="missing_prices",
                severity="low",
                title="Some open positions lack latest price",
                detail=(
                    f"{stale_price_count} open position(s) are missing `last_price`, "
                    "so unrealized totals may be incomplete."
                ),
                recommendation=(
                    "Refresh or import recent prices before relying on unrealized P&L."
                ),
                metrics={"positions_missing_price": stale_price_count},
            )
        )

    return [
        {
            "key": check.key,
            "severity": check.severity,
            "title": check.title,
            "detail": check.detail,
            "recommendation": check.recommendation,
            "metrics": check.metrics,
        }
        for check in sorted(checks, key=_rank)
    ]
