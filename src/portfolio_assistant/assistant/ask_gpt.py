from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, TYPE_CHECKING

from sqlalchemy import case, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.wash_sale import detect_wash_sale_risks
from portfolio_assistant.db.models import (
    Account,
    CashActivity,
    PnlRealized,
    PositionOpen,
    TradeNormalized,
)

if TYPE_CHECKING:
    from openai import OpenAI

MAX_TOOL_ROWS = 200
DEFAULT_TOOL_ROWS = 50
MAX_TOOL_CALL_STEPS = 8


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    if hasattr(value, "value"):  # enum-like
        return value.value
    return value


def _as_dict(item: Any) -> dict[str, Any]:
    if item is None:
        return {}
    if isinstance(item, dict):
        return item
    if hasattr(item, "model_dump"):
        try:
            dumped = item.model_dump()
            if isinstance(dumped, dict):
                return dumped
        except Exception:
            pass
    if hasattr(item, "__dict__"):
        return dict(item.__dict__)
    return {}


def _normalize_limit(value: Any, *, default: int = DEFAULT_TOOL_ROWS) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed <= 0:
        return default
    return min(parsed, MAX_TOOL_ROWS)


def _normalize_symbol(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _tool_list_accounts(session: Session, arguments: dict[str, Any]) -> dict[str, Any]:
    limit = _normalize_limit(arguments.get("limit"), default=MAX_TOOL_ROWS)
    rows = list(
        session.scalars(select(Account).order_by(Account.broker, Account.account_label).limit(limit)).all()
    )
    payload = [
        {
            "account_id": row.id,
            "broker": row.broker,
            "account_label": row.account_label,
            "account_type": _enum_value(row.account_type),
            "created_at": row.created_at,
        }
        for row in rows
    ]
    return {"count": len(payload), "rows": _to_jsonable(payload)}


def _tool_get_portfolio_overview(session: Session, arguments: dict[str, Any]) -> dict[str, Any]:
    account_id = arguments.get("account_id")
    realized_stmt = select(func.coalesce(func.sum(PnlRealized.pnl), 0.0))
    unrealized_stmt = select(func.coalesce(func.sum(PositionOpen.unrealized_pnl), 0.0))
    open_positions_stmt = select(func.count()).select_from(PositionOpen)
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
        open_positions_stmt = open_positions_stmt.where(PositionOpen.account_id == account_id)
        contributions_stmt = contributions_stmt.where(CashActivity.account_id == account_id)

    realized_total = float(session.scalar(realized_stmt) or 0.0)
    unrealized_total = float(session.scalar(unrealized_stmt) or 0.0)
    contributions_total = float(session.scalar(contributions_stmt) or 0.0)
    open_positions = int(session.scalar(open_positions_stmt) or 0)

    return {
        "account_id": account_id,
        "realized_total": realized_total,
        "unrealized_total": unrealized_total,
        "total_pnl": realized_total + unrealized_total,
        "net_contributions": contributions_total,
        "open_positions": open_positions,
    }


def _tool_get_open_positions(session: Session, arguments: dict[str, Any]) -> dict[str, Any]:
    account_id = arguments.get("account_id")
    limit = _normalize_limit(arguments.get("limit"))

    stmt = select(PositionOpen).order_by(
        func.abs(func.coalesce(PositionOpen.unrealized_pnl, 0.0)).desc(),
        PositionOpen.id.desc(),
    )
    if account_id:
        stmt = stmt.where(PositionOpen.account_id == account_id)
    rows = list(session.scalars(stmt.limit(limit)).all())

    payload = [
        {
            "account_id": row.account_id,
            "symbol": row.symbol,
            "instrument_type": _enum_value(row.instrument_type),
            "option_symbol_raw": row.option_symbol_raw,
            "quantity": float(row.quantity),
            "avg_cost": float(row.avg_cost),
            "last_price": float(row.last_price) if row.last_price is not None else None,
            "market_value": float(row.market_value) if row.market_value is not None else None,
            "unrealized_pnl": float(row.unrealized_pnl)
            if row.unrealized_pnl is not None
            else None,
            "as_of": row.as_of,
        }
        for row in rows
    ]
    return {"count": len(payload), "rows": _to_jsonable(payload)}


def _tool_get_recent_trades(session: Session, arguments: dict[str, Any]) -> dict[str, Any]:
    account_id = arguments.get("account_id")
    symbol = _normalize_symbol(arguments.get("symbol"))
    limit = _normalize_limit(arguments.get("limit"))

    stmt = select(TradeNormalized).order_by(TradeNormalized.executed_at.desc(), TradeNormalized.id.desc())
    if account_id:
        stmt = stmt.where(TradeNormalized.account_id == account_id)
    if symbol:
        stmt = stmt.where(func.upper(TradeNormalized.symbol) == symbol)

    rows = list(session.scalars(stmt.limit(limit)).all())
    payload = [
        {
            "row_id": row.id,
            "account_id": row.account_id,
            "broker": row.broker,
            "trade_id": row.trade_id,
            "executed_at": row.executed_at,
            "instrument_type": _enum_value(row.instrument_type),
            "symbol": row.symbol,
            "underlying": row.underlying,
            "option_symbol_raw": row.option_symbol_raw,
            "side": _enum_value(row.side),
            "quantity": float(row.quantity),
            "price": float(row.price),
            "fees": float(row.fees),
            "net_amount": float(row.net_amount) if row.net_amount is not None else None,
            "currency": row.currency,
        }
        for row in rows
    ]
    return {"count": len(payload), "rows": _to_jsonable(payload)}


def _tool_get_realized_pnl_by_symbol(session: Session, arguments: dict[str, Any]) -> dict[str, Any]:
    account_id = arguments.get("account_id")
    limit = _normalize_limit(arguments.get("limit"))

    stmt = (
        select(
            PnlRealized.symbol,
            PnlRealized.instrument_type,
            func.sum(PnlRealized.pnl).label("realized_pnl"),
        )
        .group_by(PnlRealized.symbol, PnlRealized.instrument_type)
        .order_by(func.abs(func.sum(PnlRealized.pnl)).desc(), PnlRealized.symbol.asc())
    )
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)

    rows = list(session.execute(stmt.limit(limit)).all())
    payload = [
        {
            "symbol": symbol,
            "instrument_type": _enum_value(instrument_type),
            "realized_pnl": float(realized_pnl or 0.0),
        }
        for symbol, instrument_type, realized_pnl in rows
    ]
    return {"count": len(payload), "rows": _to_jsonable(payload)}


def _tool_get_recent_cash_activity(session: Session, arguments: dict[str, Any]) -> dict[str, Any]:
    account_id = arguments.get("account_id")
    limit = _normalize_limit(arguments.get("limit"))

    stmt = select(CashActivity).order_by(CashActivity.posted_at.desc(), CashActivity.id.desc())
    if account_id:
        stmt = stmt.where(CashActivity.account_id == account_id)

    rows = list(session.scalars(stmt.limit(limit)).all())
    payload = [
        {
            "row_id": row.id,
            "account_id": row.account_id,
            "broker": row.broker,
            "posted_at": row.posted_at,
            "activity_type": _enum_value(row.activity_type),
            "amount": float(row.amount),
            "description": row.description,
            "source": row.source,
            "is_external": row.is_external,
        }
        for row in rows
    ]
    return {"count": len(payload), "rows": _to_jsonable(payload)}


def _tool_get_wash_sale_risks(session: Session, arguments: dict[str, Any]) -> dict[str, Any]:
    account_id = arguments.get("account_id")
    limit = _normalize_limit(arguments.get("limit"))
    risks = detect_wash_sale_risks(session, account_id=account_id, window_days=30)
    rows = sorted(
        risks,
        key=lambda row: (
            str(row.get("sale_date", "")),
            str(row.get("symbol", "")),
            int(row.get("days_from_sale", 0)),
        ),
        reverse=True,
    )[:limit]
    return {"count": len(rows), "rows": _to_jsonable(rows)}


READ_ONLY_TOOLS: dict[str, dict[str, Any]] = {
    "list_accounts": {
        "description": "List known brokerage accounts with labels and account types.",
        "parameters": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOOL_ROWS,
                    "description": "Max rows to return.",
                }
            },
            "additionalProperties": False,
        },
        "handler": _tool_list_accounts,
    },
    "get_portfolio_overview": {
        "description": "Return aggregate totals (realized, unrealized, total P&L, contributions, open positions).",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Optional account UUID scope."}
            },
            "additionalProperties": False,
        },
        "handler": _tool_get_portfolio_overview,
    },
    "get_open_positions": {
        "description": "Return current open positions ordered by absolute unrealized P&L.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Optional account UUID scope."},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOOL_ROWS,
                    "description": "Max rows to return.",
                },
            },
            "additionalProperties": False,
        },
        "handler": _tool_get_open_positions,
    },
    "get_recent_trades": {
        "description": "Return most recent normalized trades, optionally filtered by account and ticker symbol.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Optional account UUID scope."},
                "symbol": {"type": "string", "description": "Optional ticker filter (e.g. AAPL)."},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOOL_ROWS,
                    "description": "Max rows to return.",
                },
            },
            "additionalProperties": False,
        },
        "handler": _tool_get_recent_trades,
    },
    "get_realized_pnl_by_symbol": {
        "description": "Aggregate realized P&L by symbol and instrument type.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Optional account UUID scope."},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOOL_ROWS,
                    "description": "Max rows to return.",
                },
            },
            "additionalProperties": False,
        },
        "handler": _tool_get_realized_pnl_by_symbol,
    },
    "get_recent_cash_activity": {
        "description": "Return recent cash activity rows with external/internal tags.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Optional account UUID scope."},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOOL_ROWS,
                    "description": "Max rows to return.",
                },
            },
            "additionalProperties": False,
        },
        "handler": _tool_get_recent_cash_activity,
    },
    "get_wash_sale_risks": {
        "description": "Return potential wash-sale replacement matches (+/-30 days, informational).",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Optional account UUID scope."},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOOL_ROWS,
                    "description": "Max rows to return.",
                },
            },
            "additionalProperties": False,
        },
        "handler": _tool_get_wash_sale_risks,
    },
}


def build_read_only_tool_specs() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "name": name,
            "description": meta["description"],
            "parameters": meta["parameters"],
            "strict": True,
        }
        for name, meta in READ_ONLY_TOOLS.items()
    ]


def build_openai_client() -> Any:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError(
            "openai package is not installed. Install `openai` to enable Ask GPT."
        ) from exc
    return OpenAI(api_key=api_key)


def dispatch_read_only_tool(
    engine: Engine,
    *,
    name: str,
    arguments: dict[str, Any] | None = None,
    account_scope_id: str | None = None,
) -> dict[str, Any]:
    if name not in READ_ONLY_TOOLS:
        raise ValueError(f"Unsupported tool '{name}'.")

    payload = dict(arguments or {})
    if account_scope_id:
        payload["account_id"] = account_scope_id

    handler = READ_ONLY_TOOLS[name]["handler"]
    with Session(engine) as session:
        result = handler(session, payload)
    return _to_jsonable(result)


def _extract_function_calls(response: Any) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    for item in getattr(response, "output", []) or []:
        payload = _as_dict(item)
        if payload.get("type") != "function_call":
            continue
        calls.append(
            {
                "call_id": payload.get("call_id") or payload.get("id"),
                "name": payload.get("name"),
                "arguments": payload.get("arguments", "{}"),
            }
        )
    return calls


def extract_response_text(response: Any) -> str:
    text = str(getattr(response, "output_text", "") or "").strip()
    if text:
        return text

    snippets: list[str] = []
    for item in getattr(response, "output", []) or []:
        payload = _as_dict(item)
        if payload.get("type") != "message":
            continue
        content_items = payload.get("content") or []
        for content in content_items:
            content_payload = _as_dict(content)
            if content_payload.get("type") not in {"output_text", "text"}:
                continue
            value = str(content_payload.get("text", "") or "").strip()
            if value:
                snippets.append(value)
    return "\n\n".join(snippets).strip()


def extract_response_sources(response: Any) -> list[dict[str, str]]:
    seen: set[str] = set()
    rows: list[dict[str, str]] = []

    for item in getattr(response, "output", []) or []:
        payload = _as_dict(item)
        if payload.get("type") != "message":
            continue

        for content in payload.get("content") or []:
            content_payload = _as_dict(content)
            for annotation in content_payload.get("annotations") or []:
                annotation_payload = _as_dict(annotation)
                ann_type = str(annotation_payload.get("type", "")).lower()
                if "citation" not in ann_type and "source" not in ann_type:
                    continue

                url = str(annotation_payload.get("url", "") or "").strip()
                title = str(annotation_payload.get("title", "") or "").strip()
                if not url and not title:
                    continue
                key = url or title
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "title": title or url,
                        "url": url,
                    }
                )
    return rows


def _parse_tool_arguments(raw_arguments: Any) -> dict[str, Any]:
    if isinstance(raw_arguments, dict):
        return raw_arguments
    if raw_arguments is None:
        return {}
    if not isinstance(raw_arguments, str):
        return {}
    text = raw_arguments.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _ask_gpt_instructions(account_scope_id: str | None) -> str:
    scope_text = (
        f"Only analyze account_id={account_scope_id} when querying tools."
        if account_scope_id
        else "You may use all accounts."
    )
    return (
        "You are a portfolio analytics assistant for a local-first app.\n"
        "Hard guardrails:\n"
        "- Never request, infer, or store brokerage credentials.\n"
        "- Never instruct auto-trading or claim guaranteed returns.\n"
        "- Educational analysis only; not financial/tax advice.\n"
        "- Use only available read-only tools for portfolio data.\n"
        f"- {scope_text}\n"
        "When web sources are available, cite concrete sources for external claims."
    )


@dataclass(frozen=True)
class AskGptResult:
    answer: str
    model: str
    web_enabled: bool
    tool_events: list[dict[str, Any]]
    sources: list[dict[str, str]]


def ask_portfolio_question(
    *,
    engine: Engine,
    question: str,
    model: str,
    account_scope_id: str | None = None,
    web_enabled: bool = False,
    client: Any | None = None,
) -> AskGptResult:
    query = str(question or "").strip()
    if not query:
        raise ValueError("question is required")

    local_client = client or build_openai_client()

    tools = build_read_only_tool_specs()
    if web_enabled:
        tools.append({"type": "web_search_preview"})

    response = local_client.responses.create(
        model=model,
        tools=tools,
        instructions=_ask_gpt_instructions(account_scope_id),
        input=query,
    )

    tool_events: list[dict[str, Any]] = []
    for _ in range(MAX_TOOL_CALL_STEPS):
        pending_calls = _extract_function_calls(response)
        if not pending_calls:
            break

        follow_up_items: list[dict[str, str]] = []
        for call in pending_calls:
            name = str(call.get("name") or "")
            call_id = str(call.get("call_id") or "")
            if not name or not call_id:
                continue

            arguments = _parse_tool_arguments(call.get("arguments"))
            try:
                result = dispatch_read_only_tool(
                    engine,
                    name=name,
                    arguments=arguments,
                    account_scope_id=account_scope_id,
                )
            except Exception as exc:
                result = {"error": str(exc)}

            row_count = 0
            if isinstance(result, dict) and "count" in result:
                try:
                    row_count = int(result.get("count") or 0)
                except (TypeError, ValueError):
                    row_count = 0

            tool_events.append(
                {
                    "tool": name,
                    "arguments": _to_jsonable(arguments),
                    "row_count": row_count,
                    "errored": "error" in result,
                }
            )
            follow_up_items.append(
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": json.dumps(_to_jsonable(result)),
                }
            )

        if not follow_up_items:
            break

        response = local_client.responses.create(
            model=model,
            tools=tools,
            instructions=_ask_gpt_instructions(account_scope_id),
            previous_response_id=response.id,
            input=follow_up_items,
        )

    answer = extract_response_text(response)
    if not answer:
        answer = "No response text returned."

    return AskGptResult(
        answer=answer,
        model=model,
        web_enabled=web_enabled,
        tool_events=tool_events,
        sources=extract_response_sources(response),
    )
