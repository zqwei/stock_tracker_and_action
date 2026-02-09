from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import OpenAI
from sqlalchemy import case, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.risk_checks import run_deterministic_risk_checks
from portfolio_assistant.db.models import CashActivity, PnlRealized, PositionOpen
from portfolio_assistant.config.paths import PRIVATE_DIR
from portfolio_assistant.assistant.ask_gpt import (
    build_openai_client,
    extract_response_sources,
    extract_response_text,
)


@dataclass(frozen=True)
class DailyBriefingResult:
    payload: dict[str, Any]
    artifact_path: Path
    gpt_summary: str | None
    gpt_sources: list[dict[str, str]]


def briefing_storage_dir(base_dir: Path | None = None) -> Path:
    root = base_dir or (PRIVATE_DIR / "briefings")
    root.mkdir(parents=True, exist_ok=True)
    return root


def list_briefing_artifacts(*, base_dir: Path | None = None, limit: int = 20) -> list[Path]:
    root = briefing_storage_dir(base_dir)
    files = sorted(
        [path for path in root.glob("*.json") if path.is_file()],
        key=lambda path: path.name,
        reverse=True,
    )
    return files[: max(limit, 0)]


def load_briefing_artifact(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _portfolio_snapshot(session: Session, account_id: str | None) -> dict[str, Any]:
    realized_stmt = select(func.coalesce(func.sum(PnlRealized.pnl), 0.0))
    unrealized_stmt = select(func.coalesce(func.sum(PositionOpen.unrealized_pnl), 0.0))
    positions_stmt = select(func.count()).select_from(PositionOpen)
    contributions_stmt = select(
        func.coalesce(
            func.sum(
                case(
                    (CashActivity.activity_type == "DEPOSIT", CashActivity.amount),
                    else_=-CashActivity.amount,
                )
            ),
            0.0,
        )
    ).where(CashActivity.is_external.is_(True))

    if account_id:
        realized_stmt = realized_stmt.where(PnlRealized.account_id == account_id)
        unrealized_stmt = unrealized_stmt.where(PositionOpen.account_id == account_id)
        positions_stmt = positions_stmt.where(PositionOpen.account_id == account_id)
        contributions_stmt = contributions_stmt.where(CashActivity.account_id == account_id)

    realized_total = float(session.scalar(realized_stmt) or 0.0)
    unrealized_total = float(session.scalar(unrealized_stmt) or 0.0)
    net_contributions = float(session.scalar(contributions_stmt) or 0.0)
    open_positions = int(session.scalar(positions_stmt) or 0)
    return {
        "realized_total": realized_total,
        "unrealized_total": unrealized_total,
        "total_pnl": realized_total + unrealized_total,
        "net_contributions": net_contributions,
        "open_positions": open_positions,
    }


def _top_realized_rows(
    session: Session, account_id: str | None, *, limit: int = 6
) -> list[dict[str, Any]]:
    stmt = (
        select(
            PnlRealized.symbol,
            PnlRealized.instrument_type,
            func.sum(PnlRealized.pnl).label("realized_pnl"),
        )
        .group_by(PnlRealized.symbol, PnlRealized.instrument_type)
        .order_by(func.abs(func.sum(PnlRealized.pnl)).desc(), PnlRealized.symbol.asc())
        .limit(limit)
    )
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)

    rows = list(session.execute(stmt).all())
    return [
        {
            "symbol": symbol,
            "instrument_type": instrument_type.value
            if hasattr(instrument_type, "value")
            else str(instrument_type),
            "realized_pnl": float(realized_pnl or 0.0),
        }
        for symbol, instrument_type, realized_pnl in rows
    ]


def _top_unrealized_rows(
    session: Session, account_id: str | None, *, limit: int = 6
) -> list[dict[str, Any]]:
    stmt = select(PositionOpen).order_by(
        func.abs(func.coalesce(PositionOpen.unrealized_pnl, 0.0)).desc(),
        PositionOpen.id.desc(),
    )
    if account_id:
        stmt = stmt.where(PositionOpen.account_id == account_id)

    rows = list(session.scalars(stmt.limit(limit)).all())
    return [
        {
            "symbol": row.symbol,
            "instrument_type": row.instrument_type.value
            if hasattr(row.instrument_type, "value")
            else str(row.instrument_type),
            "quantity": float(row.quantity),
            "market_value": float(row.market_value) if row.market_value is not None else None,
            "unrealized_pnl": float(row.unrealized_pnl)
            if row.unrealized_pnl is not None
            else None,
        }
        for row in rows
    ]


def _default_protective_actions(checks: list[dict[str, Any]]) -> list[str]:
    actions: list[str] = []
    if any(check.get("key") == "wash_sale_replacements" for check in checks):
        actions.append(
            "Review wash-sale matches and replacement lots before making tax-sensitive decisions."
        )
    if any(check.get("key") == "position_concentration" for check in checks):
        actions.append(
            "Stress-test downside for concentrated symbols and cap new exposure until risk is acceptable."
        )
    if any(check.get("key") == "cash_external_tagging" for check in checks):
        actions.append(
            "Finish external/internal tagging for cash rows before interpreting return metrics."
        )
    if any(check.get("key") == "large_unrealized_loss" for check in checks):
        actions.append(
            "Re-check thesis, event risk, and sizing on largest unrealized loss positions."
        )
    if any(check.get("key") == "missing_prices" for check in checks):
        actions.append("Refresh or import latest prices before relying on unrealized totals.")

    if not actions:
        actions.append(
            "No deterministic high-severity flags today; continue normal monitoring and sizing discipline."
        )
    return actions


def _briefing_instructions() -> str:
    return (
        "You are producing a concise portfolio risk briefing.\n"
        "Guardrails:\n"
        "- Educational only, not financial/tax advice.\n"
        "- No guaranteed outcomes.\n"
        "- No auto-trading instructions.\n"
        "- Do not request or store brokerage credentials.\n"
        "Use the provided JSON context and return a short summary plus risk-focused actions."
    )


def _generate_gpt_summary(
    *,
    payload: dict[str, Any],
    model: str,
    enable_web_context: bool,
    client: OpenAI | None,
) -> tuple[str, list[dict[str, str]]]:
    local_client = client or build_openai_client()
    request: dict[str, Any] = {
        "model": model,
        "instructions": _briefing_instructions(),
        "input": (
            "Daily briefing context JSON:\n"
            f"{json.dumps(payload, sort_keys=True)}\n\n"
            "Write:\n"
            "1) One paragraph summary.\n"
            "2) Up to 5 protective actions.\n"
            "3) Explicitly state that this is educational, not advice."
        ),
    }
    if enable_web_context:
        request["tools"] = [{"type": "web_search_preview"}]

    response = local_client.responses.create(**request)
    summary = extract_response_text(response).strip()
    sources = extract_response_sources(response)
    return (summary or "No GPT summary returned."), sources


def generate_daily_briefing(
    engine: Engine,
    *,
    model: str,
    account_id: str | None = None,
    include_gpt_summary: bool = False,
    enable_web_context: bool = False,
    output_dir: Path | None = None,
    as_of: datetime | None = None,
    client: OpenAI | None = None,
) -> DailyBriefingResult:
    generated_at = as_of or datetime.utcnow()

    with Session(engine) as session:
        snapshot = _portfolio_snapshot(session, account_id=account_id)
        checks = run_deterministic_risk_checks(session, account_id=account_id)
        top_realized = _top_realized_rows(session, account_id=account_id)
        top_unrealized = _top_unrealized_rows(session, account_id=account_id)

    protective_actions = _default_protective_actions(checks)
    payload: dict[str, Any] = {
        "generated_at": generated_at.isoformat(),
        "account_scope": account_id or "ALL_ACCOUNTS",
        "guardrails": {
            "credentials_storage": "forbidden",
            "auto_trading": "forbidden",
            "advice_scope": "educational_only",
        },
        "snapshot": snapshot,
        "risk_checks": checks,
        "top_realized": top_realized,
        "top_unrealized": top_unrealized,
        "protective_actions": protective_actions,
    }

    gpt_summary: str | None = None
    gpt_sources: list[dict[str, str]] = []
    gpt_error: str | None = None
    if include_gpt_summary:
        try:
            gpt_summary, gpt_sources = _generate_gpt_summary(
                payload=payload,
                model=model,
                enable_web_context=enable_web_context,
                client=client,
            )
        except Exception as exc:
            gpt_error = str(exc)

    if gpt_summary:
        payload["gpt_summary"] = gpt_summary
    if gpt_sources:
        payload["gpt_sources"] = gpt_sources
    if gpt_error:
        payload["gpt_error"] = gpt_error

    scope = account_id or "all_accounts"
    safe_scope = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in scope)
    filename = f"{generated_at.strftime('%Y%m%dT%H%M%SZ')}_{safe_scope}.json"
    artifact_path = briefing_storage_dir(output_dir) / filename
    with artifact_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)

    return DailyBriefingResult(
        payload=payload,
        artifact_path=artifact_path,
        gpt_summary=gpt_summary,
        gpt_sources=gpt_sources,
    )
