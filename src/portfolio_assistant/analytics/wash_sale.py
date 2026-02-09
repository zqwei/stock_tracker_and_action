from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Literal

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import Account, PnlRealized, TradeNormalized

EPSILON = 1e-12
IRA_ACCOUNT_TYPES = {"TRAD_IRA", "ROTH_IRA"}


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _normalize_symbol(value: str | None) -> str:
    return (value or "").strip().upper()


def _sale_share_equivalent(sale: PnlRealized) -> float:
    instrument = _enum_value(sale.instrument_type).upper()
    multiplier = 100 if instrument == "OPTION" else 1
    return abs(float(sale.quantity or 0.0)) * multiplier


def _trade_share_equivalent(trade: TradeNormalized) -> float:
    instrument = _enum_value(trade.instrument_type).upper()
    qty = abs(float(trade.quantity or 0.0))
    multiplier = int(trade.multiplier or 1)
    if multiplier <= 0:
        multiplier = 1
    if instrument == "OPTION":
        return qty * multiplier
    return qty


def _is_replacement_acquisition(trade: TradeNormalized) -> bool:
    side = _enum_value(trade.side).upper()
    instrument = _enum_value(trade.instrument_type).upper()

    if side == "BTO":
        return True
    if instrument == "STOCK" and side == "BUY":
        return True
    if instrument == "OPTION" and side == "BUY":
        return True
    return False


def _loss_sales(
    session: Session,
    account_id: str | None,
    sale_start: date | None,
    sale_end: date | None,
) -> list[PnlRealized]:
    stmt = (
        select(PnlRealized)
        .join(Account, Account.id == PnlRealized.account_id)
        .where(Account.account_type == "TAXABLE", PnlRealized.pnl < 0)
        .order_by(PnlRealized.close_date.asc(), PnlRealized.id.asc())
    )
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)
    if sale_start:
        stmt = stmt.where(PnlRealized.close_date >= sale_start)
    if sale_end:
        stmt = stmt.where(PnlRealized.close_date <= sale_end)
    return list(session.scalars(stmt).all())


def _candidate_replacements(
    session: Session,
    *,
    sale_symbol: str,
    sale_account_id: str,
    start_dt: datetime,
    end_dt: datetime,
    mode: Literal["broker", "irs"],
) -> list[TradeNormalized]:
    stmt = (
        select(TradeNormalized)
        .where(
            TradeNormalized.executed_at >= start_dt,
            TradeNormalized.executed_at <= end_dt,
            TradeNormalized.quantity > 0,
            or_(
                func.upper(TradeNormalized.symbol) == sale_symbol,
                func.upper(TradeNormalized.underlying) == sale_symbol,
            ),
        )
        .order_by(TradeNormalized.executed_at.asc(), TradeNormalized.id.asc())
    )
    if mode == "broker":
        stmt = stmt.where(TradeNormalized.account_id == sale_account_id)

    return [
        trade for trade in session.scalars(stmt).all() if _is_replacement_acquisition(trade)
    ]


def estimate_wash_sale_disallowance(
    session: Session,
    account_id: str | None = None,
    mode: Literal["broker", "irs"] = "irs",
    window_days: int = 30,
    sale_start: date | None = None,
    sale_end: date | None = None,
) -> dict[str, Any]:
    if mode not in {"broker", "irs"}:
        raise ValueError("mode must be 'broker' or 'irs'")
    if window_days < 0:
        raise ValueError("window_days must be non-negative")

    accounts = {acc.id: acc for acc in session.scalars(select(Account)).all()}
    trade_capacity_by_row: dict[int, float] = {}
    sales_out: list[dict[str, Any]] = []

    for sale in _loss_sales(
        session,
        account_id=account_id,
        sale_start=sale_start,
        sale_end=sale_end,
    ):
        sale_symbol = _normalize_symbol(sale.symbol)
        if not sale_symbol:
            continue

        sale_qty_equiv = _sale_share_equivalent(sale)
        if sale_qty_equiv <= EPSILON:
            continue

        sale_loss = abs(float(sale.pnl))
        loss_per_share_equiv = sale_loss / sale_qty_equiv
        remaining_qty_equiv = sale_qty_equiv
        start_dt = datetime.combine(
            sale.close_date - timedelta(days=window_days), datetime.min.time()
        )
        end_dt = datetime.combine(
            sale.close_date + timedelta(days=window_days), datetime.max.time()
        )

        matches: list[dict[str, Any]] = []
        for trade in _candidate_replacements(
            session,
            sale_symbol=sale_symbol,
            sale_account_id=sale.account_id,
            start_dt=start_dt,
            end_dt=end_dt,
            mode=mode,
        ):
            trade_qty_equiv = _trade_share_equivalent(trade)
            if trade_qty_equiv <= EPSILON:
                continue

            available_qty_equiv = trade_capacity_by_row.setdefault(trade.id, trade_qty_equiv)
            if available_qty_equiv <= EPSILON:
                continue

            allocated_qty_equiv = min(remaining_qty_equiv, available_qty_equiv)
            if allocated_qty_equiv <= EPSILON:
                continue

            trade_capacity_by_row[trade.id] = available_qty_equiv - allocated_qty_equiv
            remaining_qty_equiv -= allocated_qty_equiv

            buy_account = accounts.get(trade.account_id)
            buy_account_type = (
                _enum_value(buy_account.account_type).upper() if buy_account else ""
            )
            days_from_sale = (trade.executed_at.date() - sale.close_date).days
            buy_side = _enum_value(trade.side).upper()
            buy_instrument = _enum_value(trade.instrument_type).upper()

            matches.append(
                {
                    "buy_account_id": trade.account_id,
                    "buy_account_label": (
                        buy_account.account_label if buy_account is not None else ""
                    ),
                    "buy_account_type": buy_account_type,
                    "buy_trade_row_id": trade.id,
                    "buy_trade_id": trade.trade_id,
                    "buy_date": trade.executed_at.date().isoformat(),
                    "buy_side": buy_side,
                    "buy_instrument_type": buy_instrument,
                    "buy_quantity": float(trade.quantity),
                    "buy_quantity_equiv": trade_qty_equiv,
                    "buy_price": float(trade.price),
                    "days_from_sale": days_from_sale,
                    "is_boundary_day": abs(days_from_sale) == window_days,
                    "cross_account": trade.account_id != sale.account_id,
                    "ira_replacement": buy_account_type in IRA_ACCOUNT_TYPES,
                    "allocated_replacement_quantity_equiv": allocated_qty_equiv,
                }
            )

            if remaining_qty_equiv <= EPSILON:
                break

        matched_qty_equiv = sale_qty_equiv - remaining_qty_equiv
        if matched_qty_equiv <= EPSILON:
            continue

        disallowed_loss = matched_qty_equiv * loss_per_share_equiv
        sale_account = accounts.get(sale.account_id)
        sales_out.append(
            {
                "sale_row_id": sale.id,
                "sale_account_id": sale.account_id,
                "sale_account_label": (
                    sale_account.account_label if sale_account is not None else ""
                ),
                "sale_account_type": (
                    _enum_value(sale_account.account_type).upper()
                    if sale_account is not None
                    else ""
                ),
                "symbol": sale_symbol,
                "sale_date": sale.close_date.isoformat(),
                "sale_loss": float(sale.pnl),
                "sale_quantity": float(sale.quantity),
                "sale_quantity_equiv": sale_qty_equiv,
                "loss_per_share_equiv": loss_per_share_equiv,
                "matched_replacement_quantity_equiv": matched_qty_equiv,
                "unmatched_replacement_quantity_equiv": remaining_qty_equiv,
                "disallowed_loss": disallowed_loss,
                "has_ira_replacement": any(m["ira_replacement"] for m in matches),
                "matches": matches,
            }
        )

    adjustments = {row["sale_row_id"]: row["disallowed_loss"] for row in sales_out}
    total_disallowed = float(sum(adjustments.values()))
    return {
        "mode": mode,
        "window_days": window_days,
        "sales": sales_out,
        "sale_adjustments": adjustments,
        "total_disallowed_loss": total_disallowed,
    }


def detect_wash_sale_risks(
    session: Session, account_id: str | None = None, window_days: int = 30
) -> list[dict[str, Any]]:
    analysis = estimate_wash_sale_disallowance(
        session,
        account_id=account_id,
        mode="irs",
        window_days=window_days,
    )

    risks: list[dict[str, Any]] = []
    for sale in analysis["sales"]:
        for match in sale["matches"]:
            risks.append(
                {
                    "symbol": sale["symbol"],
                    "sale_account_id": sale["sale_account_id"],
                    "sale_account_label": sale["sale_account_label"],
                    "sale_date": sale["sale_date"],
                    "sale_loss": sale["sale_loss"],
                    "sale_quantity": sale["sale_quantity"],
                    "sale_quantity_equiv": sale["sale_quantity_equiv"],
                    "buy_account_id": match["buy_account_id"],
                    "buy_account_label": match["buy_account_label"],
                    "buy_account_type": match["buy_account_type"],
                    "buy_date": match["buy_date"],
                    "days_from_sale": match["days_from_sale"],
                    "buy_side": match["buy_side"],
                    "buy_instrument_type": match["buy_instrument_type"],
                    "buy_quantity": match["buy_quantity"],
                    "buy_quantity_equiv": match["buy_quantity_equiv"],
                    "buy_price": match["buy_price"],
                    "buy_trade_id": match["buy_trade_id"],
                    "buy_trade_row_id": match["buy_trade_row_id"],
                    "cross_account": match["cross_account"],
                    "ira_replacement": match["ira_replacement"],
                    "is_boundary_day": match["is_boundary_day"],
                    "allocated_replacement_quantity_equiv": (
                        match["allocated_replacement_quantity_equiv"]
                    ),
                }
            )

    return risks
