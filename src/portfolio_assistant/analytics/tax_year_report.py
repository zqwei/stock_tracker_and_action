from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import re
from datetime import date, datetime, time
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.analytics.reconciliation import build_broker_vs_irs_reconciliation
from portfolio_assistant.analytics.wash_sale import estimate_wash_sale_disallowance
from portfolio_assistant.db.models import Account, PnlRealized, TradeNormalized

DATE_FROM_NOTES_RE = re.compile(r"from\s+(\d{4}-\d{2}-\d{2})")
SIDE_ALIASES = {
    "B": "BUY",
    "S": "SELL",
    "BUY TO OPEN": "BTO",
    "SELL TO OPEN": "STO",
    "BUY TO CLOSE": "BTC",
    "SELL TO CLOSE": "STC",
}
SNAPSHOT_EPSILON = 1e-12


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _normalize_symbol(value: str | None) -> str:
    return (value or "").strip().upper()


def _format_strike(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    strike = float(value)
    if strike.is_integer():
        return str(int(strike))
    return f"{strike:.8f}".rstrip("0").rstrip(".")


def _option_contract_label(trade: TradeNormalized) -> str:
    if trade.option_symbol_raw:
        return " ".join(str(trade.option_symbol_raw).upper().split())

    symbol = _normalize_symbol(trade.underlying or trade.symbol)
    exp = trade.expiration.strftime("%Y-%m-%d") if trade.expiration else "UNKNOWN"
    cp = _enum_value(trade.call_put).upper() if trade.call_put is not None else "?"
    strike = _format_strike(trade.strike)
    return f"{symbol}|{exp}|{strike}|{cp}"


def _normalize_trade_side(instrument: str, side: str) -> str:
    normalized = SIDE_ALIASES.get(side, side)
    if instrument == "STOCK" and normalized in {"BTO", "BTC"}:
        return "BUY"
    if instrument == "STOCK" and normalized in {"STO", "STC"}:
        return "SELL"
    return normalized


def _parse_date_acquired(notes: str | None) -> date | None:
    if not notes:
        return None
    match = DATE_FROM_NOTES_RE.search(notes)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _holding_term(acquired: date | None, sold: date) -> str:
    if acquired is None:
        return "UNKNOWN"
    days_held = (sold - acquired).days
    return "LONG" if days_held > 365 else "SHORT"


@dataclass
class _SnapshotLot:
    account_id: str
    account_label: str
    account_type: str
    symbol: str
    instrument_type: str
    option_symbol_raw: str | None
    opened_at: datetime
    source_trade_row_id: int
    multiplier: int
    quantity_equiv: float
    raw_unit_basis: float
    adjusted_unit_basis: float
    wash_unit_adjustment: float
    position_side: str


def _consume_open_lots(lots: deque[_SnapshotLot], quantity_equiv: float) -> float:
    remaining = quantity_equiv
    while remaining > SNAPSHOT_EPSILON and lots:
        head = lots[0]
        take = min(head.quantity_equiv, remaining)
        head.quantity_equiv -= take
        remaining -= take
        if head.quantity_equiv <= SNAPSHOT_EPSILON:
            lots.popleft()
    return remaining


def _build_trade_basis_adjustments(
    wash_analysis: dict[str, Any], cutoff_date: date
) -> dict[int, dict[str, float]]:
    by_trade: dict[int, dict[str, float]] = {}
    for entry in wash_analysis.get("adjustment_ledger") or []:
        if not bool(entry.get("basis_adjustment_applies")):
            continue

        buy_date_text = str(entry.get("buy_date") or "").strip()
        try:
            buy_date = datetime.strptime(buy_date_text, "%Y-%m-%d").date()
        except ValueError:
            continue
        if buy_date > cutoff_date:
            continue

        trade_row_id = int(entry.get("buy_trade_row_id") or 0)
        if trade_row_id <= 0:
            continue

        qty_equiv = float(entry.get("allocated_replacement_quantity_equiv") or 0.0)
        loss = float(entry.get("allocated_disallowed_loss") or 0.0)
        if qty_equiv <= SNAPSHOT_EPSILON or loss <= SNAPSHOT_EPSILON:
            continue

        bucket = by_trade.setdefault(
            trade_row_id,
            {
                "allocated_replacement_quantity_equiv": 0.0,
                "allocated_disallowed_loss": 0.0,
            },
        )
        bucket["allocated_replacement_quantity_equiv"] += qty_equiv
        bucket["allocated_disallowed_loss"] += loss

    out: dict[int, dict[str, float]] = {}
    for trade_row_id, values in by_trade.items():
        allocated_qty_equiv = float(values["allocated_replacement_quantity_equiv"])
        allocated_loss = float(values["allocated_disallowed_loss"])
        if allocated_qty_equiv <= SNAPSHOT_EPSILON:
            continue
        out[trade_row_id] = {
            "allocated_replacement_quantity_equiv": allocated_qty_equiv,
            "allocated_disallowed_loss": allocated_loss,
            "remaining_qty_equiv": allocated_qty_equiv,
            "unit_adjustment": allocated_loss / allocated_qty_equiv,
        }
    return out


def _open_lot_with_trade_adjustment(
    *,
    lots: deque[_SnapshotLot],
    trade: TradeNormalized,
    account: Account,
    symbol: str,
    instrument_type: str,
    option_symbol_raw: str | None,
    multiplier: int,
    quantity_equiv: float,
    unit_basis: float,
    position_side: str,
    trade_basis_adjustments: dict[int, dict[str, float]],
) -> None:
    if quantity_equiv <= SNAPSHOT_EPSILON:
        return

    account_type = _enum_value(account.account_type).upper()
    if position_side == "SHORT":
        lots.append(
            _SnapshotLot(
                account_id=trade.account_id,
                account_label=account.account_label,
                account_type=account_type,
                symbol=symbol,
                instrument_type=instrument_type,
                option_symbol_raw=option_symbol_raw,
                opened_at=trade.executed_at,
                source_trade_row_id=int(trade.id),
                multiplier=multiplier,
                quantity_equiv=quantity_equiv,
                raw_unit_basis=unit_basis,
                adjusted_unit_basis=unit_basis,
                wash_unit_adjustment=0.0,
                position_side="SHORT",
            )
        )
        return

    adjustment = trade_basis_adjustments.get(int(trade.id))
    adjusted_qty_equiv = 0.0
    wash_unit_adjustment = 0.0
    if adjustment and adjustment["remaining_qty_equiv"] > SNAPSHOT_EPSILON:
        adjusted_qty_equiv = min(quantity_equiv, adjustment["remaining_qty_equiv"])
        if adjusted_qty_equiv > SNAPSHOT_EPSILON:
            wash_unit_adjustment = adjustment["unit_adjustment"]
            adjustment["remaining_qty_equiv"] -= adjusted_qty_equiv

    if adjusted_qty_equiv > SNAPSHOT_EPSILON:
        lots.append(
            _SnapshotLot(
                account_id=trade.account_id,
                account_label=account.account_label,
                account_type=account_type,
                symbol=symbol,
                instrument_type=instrument_type,
                option_symbol_raw=option_symbol_raw,
                opened_at=trade.executed_at,
                source_trade_row_id=int(trade.id),
                multiplier=multiplier,
                quantity_equiv=adjusted_qty_equiv,
                raw_unit_basis=unit_basis,
                adjusted_unit_basis=unit_basis + wash_unit_adjustment,
                wash_unit_adjustment=wash_unit_adjustment,
                position_side="LONG",
            )
        )

    unadjusted_qty_equiv = quantity_equiv - adjusted_qty_equiv
    if unadjusted_qty_equiv > SNAPSHOT_EPSILON:
        lots.append(
            _SnapshotLot(
                account_id=trade.account_id,
                account_label=account.account_label,
                account_type=account_type,
                symbol=symbol,
                instrument_type=instrument_type,
                option_symbol_raw=option_symbol_raw,
                opened_at=trade.executed_at,
                source_trade_row_id=int(trade.id),
                multiplier=multiplier,
                quantity_equiv=unadjusted_qty_equiv,
                raw_unit_basis=unit_basis,
                adjusted_unit_basis=unit_basis,
                wash_unit_adjustment=0.0,
                position_side="LONG",
            )
        )


def year_end_lot_snapshot(
    session: Session,
    tax_year: int,
    account_id: str | None = None,
    wash_analysis: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    cutoff_date = date(tax_year, 12, 31)
    cutoff_dt = datetime.combine(cutoff_date, time.max)

    account_stmt = select(Account).where(Account.account_type == "TAXABLE")
    if account_id:
        account_stmt = account_stmt.where(Account.id == account_id)
    accounts = list(session.scalars(account_stmt).all())
    account_lookup = {account.id: account for account in accounts}
    if not account_lookup:
        return []

    if wash_analysis is None:
        wash_analysis = estimate_wash_sale_disallowance(
            session,
            mode="irs",
            sale_end=cutoff_date,
        )
    trade_basis_adjustments = _build_trade_basis_adjustments(wash_analysis, cutoff_date)

    trade_stmt = (
        select(TradeNormalized)
        .where(
            TradeNormalized.account_id.in_(list(account_lookup.keys())),
            TradeNormalized.executed_at <= cutoff_dt,
        )
        .order_by(TradeNormalized.executed_at.asc(), TradeNormalized.id.asc())
    )
    trades = list(session.scalars(trade_stmt).all())

    stock_long_lots: dict[tuple[str, str], deque[_SnapshotLot]] = defaultdict(deque)
    stock_short_lots: dict[tuple[str, str], deque[_SnapshotLot]] = defaultdict(deque)
    option_long_lots: dict[tuple[str, str, str], deque[_SnapshotLot]] = defaultdict(deque)
    option_short_lots: dict[tuple[str, str, str], deque[_SnapshotLot]] = defaultdict(deque)

    for trade in trades:
        account = account_lookup.get(trade.account_id)
        if account is None:
            continue

        instrument_type = _enum_value(trade.instrument_type).upper()
        side = _normalize_trade_side(instrument_type, _enum_value(trade.side).upper())
        qty = abs(float(trade.quantity or 0.0))
        if qty <= SNAPSHOT_EPSILON:
            continue

        symbol = _normalize_symbol(trade.symbol or trade.underlying)
        if not symbol:
            continue

        price = float(trade.price or 0.0)
        fees = float(trade.fees or 0.0)

        if instrument_type == "STOCK":
            key = (trade.account_id, symbol)
            qty_total = qty

            if side == "BUY":
                remaining_qty = _consume_open_lots(stock_short_lots[key], qty_total)
                if remaining_qty > SNAPSHOT_EPSILON:
                    fee_alloc = fees * (remaining_qty / qty_total)
                    unit_basis = ((remaining_qty * price) + fee_alloc) / remaining_qty
                    _open_lot_with_trade_adjustment(
                        lots=stock_long_lots[key],
                        trade=trade,
                        account=account,
                        symbol=symbol,
                        instrument_type="STOCK",
                        option_symbol_raw=None,
                        multiplier=1,
                        quantity_equiv=remaining_qty,
                        unit_basis=unit_basis,
                        position_side="LONG",
                        trade_basis_adjustments=trade_basis_adjustments,
                    )
                continue

            if side == "SELL":
                remaining_qty = _consume_open_lots(stock_long_lots[key], qty_total)
                if remaining_qty > SNAPSHOT_EPSILON:
                    fee_alloc = fees * (remaining_qty / qty_total)
                    unit_credit = ((remaining_qty * price) - fee_alloc) / remaining_qty
                    _open_lot_with_trade_adjustment(
                        lots=stock_short_lots[key],
                        trade=trade,
                        account=account,
                        symbol=symbol,
                        instrument_type="STOCK",
                        option_symbol_raw=None,
                        multiplier=1,
                        quantity_equiv=remaining_qty,
                        unit_basis=unit_credit,
                        position_side="SHORT",
                        trade_basis_adjustments=trade_basis_adjustments,
                    )
                continue

            continue

        if instrument_type != "OPTION":
            continue

        multiplier = int(trade.multiplier or 100)
        if multiplier <= 0:
            multiplier = 100

        option_contract = _option_contract_label(trade)
        key = (trade.account_id, symbol, option_contract)
        qty_total_contracts = qty

        def _open_option_long(open_contracts: float) -> None:
            if open_contracts <= SNAPSHOT_EPSILON:
                return
            open_equiv = open_contracts * multiplier
            fee_alloc = fees * (open_contracts / qty_total_contracts)
            unit_basis = ((open_equiv * price) + fee_alloc) / open_equiv
            _open_lot_with_trade_adjustment(
                lots=option_long_lots[key],
                trade=trade,
                account=account,
                symbol=symbol,
                instrument_type="OPTION",
                option_symbol_raw=option_contract,
                multiplier=multiplier,
                quantity_equiv=open_equiv,
                unit_basis=unit_basis,
                position_side="LONG",
                trade_basis_adjustments=trade_basis_adjustments,
            )

        def _open_option_short(open_contracts: float) -> None:
            if open_contracts <= SNAPSHOT_EPSILON:
                return
            open_equiv = open_contracts * multiplier
            fee_alloc = fees * (open_contracts / qty_total_contracts)
            unit_credit = ((open_equiv * price) - fee_alloc) / open_equiv
            _open_lot_with_trade_adjustment(
                lots=option_short_lots[key],
                trade=trade,
                account=account,
                symbol=symbol,
                instrument_type="OPTION",
                option_symbol_raw=option_contract,
                multiplier=multiplier,
                quantity_equiv=open_equiv,
                unit_basis=unit_credit,
                position_side="SHORT",
                trade_basis_adjustments=trade_basis_adjustments,
            )

        def _close_option_long(close_contracts: float) -> float:
            remaining_equiv = _consume_open_lots(option_long_lots[key], close_contracts * multiplier)
            return remaining_equiv / multiplier

        def _close_option_short(close_contracts: float) -> float:
            remaining_equiv = _consume_open_lots(
                option_short_lots[key], close_contracts * multiplier
            )
            return remaining_equiv / multiplier

        if side == "BUY":
            remaining_contracts = _close_option_short(qty_total_contracts)
            _open_option_long(remaining_contracts)
            continue

        if side == "SELL":
            remaining_contracts = _close_option_long(qty_total_contracts)
            _open_option_short(remaining_contracts)
            continue

        if side == "BTO":
            _open_option_long(qty_total_contracts)
            continue

        if side == "STO":
            _open_option_short(qty_total_contracts)
            continue

        if side == "STC":
            _close_option_long(qty_total_contracts)
            continue

        if side == "BTC":
            _close_option_short(qty_total_contracts)
            continue

    rows: list[dict[str, Any]] = []
    all_lot_buckets = [
        stock_long_lots.values(),
        stock_short_lots.values(),
        option_long_lots.values(),
        option_short_lots.values(),
    ]
    for buckets in all_lot_buckets:
        for lots in buckets:
            for lot in lots:
                if lot.quantity_equiv <= SNAPSHOT_EPSILON:
                    continue

                quantity = lot.quantity_equiv / lot.multiplier
                if lot.position_side == "SHORT":
                    quantity = -quantity

                opened_date = lot.opened_at.date()
                holding_period_days = (cutoff_date - opened_date).days
                term = "SHORT" if lot.position_side == "SHORT" else _holding_term(
                    opened_date, cutoff_date
                )
                raw_cost_basis = lot.quantity_equiv * lot.raw_unit_basis
                adjusted_cost_basis = lot.quantity_equiv * lot.adjusted_unit_basis
                wash_adjustment = adjusted_cost_basis - raw_cost_basis

                rows.append(
                    {
                        "account_id": lot.account_id,
                        "account_label": lot.account_label,
                        "account_type": lot.account_type,
                        "symbol": lot.symbol,
                        "instrument_type": lot.instrument_type,
                        "option_symbol_raw": lot.option_symbol_raw,
                        "position_side": lot.position_side,
                        "opened_at": lot.opened_at.date().isoformat(),
                        "holding_period_days": holding_period_days,
                        "term": term,
                        "quantity": quantity,
                        "quantity_equiv": lot.quantity_equiv,
                        "multiplier": lot.multiplier,
                        "raw_unit_basis": lot.raw_unit_basis,
                        "adjusted_unit_basis": lot.adjusted_unit_basis,
                        "raw_cost_basis": raw_cost_basis,
                        "adjusted_cost_basis": adjusted_cost_basis,
                        "wash_sale_basis_adjustment": wash_adjustment,
                        "source_trade_row_id": lot.source_trade_row_id,
                    }
                )

    rows.sort(
        key=lambda row: (
            str(row["account_label"]),
            str(row["symbol"]),
            str(row["instrument_type"]),
            str(row["position_side"]),
            str(row["opened_at"]),
            int(row["source_trade_row_id"]),
        )
    )
    return rows


def generate_tax_year_report(
    session: Session, tax_year: int, account_id: str | None = None
) -> dict[str, Any]:
    start = date(tax_year, 1, 1)
    end = date(tax_year, 12, 31)

    stmt = select(PnlRealized).where(
        PnlRealized.close_date >= start,
        PnlRealized.close_date <= end,
    )
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)
    stmt = stmt.order_by(PnlRealized.close_date.asc(), PnlRealized.symbol.asc(), PnlRealized.id.asc())

    records = list(session.scalars(stmt).all())
    broker_wash = estimate_wash_sale_disallowance(
        session,
        account_id=account_id,
        mode="broker",
        sale_start=start,
        sale_end=end,
    )
    irs_wash = estimate_wash_sale_disallowance(
        session,
        account_id=account_id,
        mode="irs",
        sale_start=start,
        sale_end=end,
    )
    snapshot_wash = estimate_wash_sale_disallowance(
        session,
        mode="irs",
        sale_end=end,
    )

    broker_adjustments = broker_wash["sale_adjustments"]
    irs_adjustments = irs_wash["sale_adjustments"]

    rows = []
    total_raw_gain_loss = 0.0
    total_adjusted_gain_loss = 0.0
    total_broker_gain_loss = 0.0
    total_proceeds = 0.0
    total_cost_basis = 0.0
    total_wash_broker = 0.0
    total_wash_irs = 0.0
    st_total = 0.0
    lt_total = 0.0
    unknown_term_total = 0.0

    for record in records:
        raw_gain_loss = float(record.pnl)
        proceeds = float(record.proceeds)
        cost_basis = float(record.cost_basis)
        wash_broker = float(broker_adjustments.get(record.id, 0.0))
        wash_irs = float(irs_adjustments.get(record.id, 0.0))
        broker_gain_loss = raw_gain_loss + wash_broker
        adjusted_gain_loss = raw_gain_loss + wash_irs

        acquired_date = _parse_date_acquired(record.notes)
        holding_term = _holding_term(acquired_date, record.close_date)

        rows.append(
            {
                "sale_row_id": int(record.id),
                "description": record.symbol,
                "date_acquired": acquired_date.isoformat() if acquired_date else None,
                "date_sold": record.close_date.isoformat(),
                "symbol": record.symbol,
                "instrument_type": record.instrument_type.value,
                "term": holding_term,
                "quantity": float(record.quantity),
                "proceeds": proceeds,
                "basis": cost_basis,
                "cost_basis": cost_basis,
                "adjustment_codes": "W" if wash_irs > 0 else "",
                "adjustment_amount": wash_irs,
                "wash_sale_disallowed": wash_irs,
                "wash_sale_disallowed_broker": wash_broker,
                "wash_sale_disallowed_irs": wash_irs,
                "raw_gain_or_loss": raw_gain_loss,
                "gain_or_loss_broker": broker_gain_loss,
                "gain_or_loss_irs": adjusted_gain_loss,
                "wash_sale_mode_gain_difference": adjusted_gain_loss - broker_gain_loss,
                "gain_or_loss": adjusted_gain_loss,
            }
        )

        total_raw_gain_loss += raw_gain_loss
        total_adjusted_gain_loss += adjusted_gain_loss
        total_broker_gain_loss += broker_gain_loss
        total_proceeds += proceeds
        total_cost_basis += cost_basis
        total_wash_broker += wash_broker
        total_wash_irs += wash_irs

        if holding_term == "SHORT":
            st_total += adjusted_gain_loss
        elif holding_term == "LONG":
            lt_total += adjusted_gain_loss
        else:
            unknown_term_total += adjusted_gain_loss

    year_end_snapshot = year_end_lot_snapshot(
        session,
        tax_year=tax_year,
        account_id=account_id,
        wash_analysis=snapshot_wash,
    )
    year_end_raw_basis_total = float(
        sum(float(row["raw_cost_basis"]) for row in year_end_snapshot)
    )
    year_end_adjusted_basis_total = float(
        sum(float(row["adjusted_cost_basis"]) for row in year_end_snapshot)
    )
    year_end_wash_adjustment_total = float(
        sum(float(row["wash_sale_basis_adjustment"]) for row in year_end_snapshot)
    )

    summary = {
        "tax_year": tax_year,
        "rows": len(rows),
        "total_proceeds": total_proceeds,
        "total_cost_basis": total_cost_basis,
        "total_gain_or_loss": total_adjusted_gain_loss,
        "total_gain_or_loss_broker": total_broker_gain_loss,
        "total_gain_or_loss_raw": total_raw_gain_loss,
        "short_term_gain_or_loss": st_total,
        "long_term_gain_or_loss": lt_total,
        "unknown_term_gain_or_loss": unknown_term_total,
        "total_wash_sale_disallowed": total_wash_irs,
        "total_wash_sale_disallowed_broker": total_wash_broker,
        "total_wash_sale_disallowed_irs": total_wash_irs,
        "wash_sale_mode_difference": total_wash_irs - total_wash_broker,
        "wash_sale_mode_gain_difference": total_adjusted_gain_loss - total_broker_gain_loss,
        "year_end_open_lot_count": len(year_end_snapshot),
        "year_end_raw_basis_total": year_end_raw_basis_total,
        "year_end_adjusted_basis_total": year_end_adjusted_basis_total,
        "year_end_wash_basis_adjustment_total": year_end_wash_adjustment_total,
        "math_check_raw": abs((total_proceeds - total_cost_basis) - total_raw_gain_loss) <= 1e-6,
        "math_check_adjusted": abs(
            (total_raw_gain_loss + total_wash_irs) - total_adjusted_gain_loss
        )
        <= 1e-6,
        "math_check_wash_broker": abs(
            total_wash_broker - float(broker_wash["total_disallowed_loss"])
        )
        <= 1e-6,
        "math_check_wash_irs": abs(
            total_wash_irs - float(irs_wash["total_disallowed_loss"])
        )
        <= 1e-6,
    }

    report = {
        "summary": summary,
        "detail_rows": rows,
        "year_end_lot_snapshot": year_end_snapshot,
        "wash_sale_summary": {
            "broker": {
                "total_disallowed_loss": broker_wash["total_disallowed_loss"],
                "total_deferred_loss_to_replacement_basis": (
                    broker_wash.get("total_deferred_loss_to_replacement_basis", 0.0)
                ),
                "total_permanently_disallowed_loss": (
                    broker_wash.get("total_permanently_disallowed_loss", 0.0)
                ),
                "sales": broker_wash["sales"],
                "adjustment_ledger": broker_wash.get("adjustment_ledger", []),
                "replacement_lot_adjustments": broker_wash.get(
                    "replacement_lot_adjustments", []
                ),
            },
            "irs": {
                "total_disallowed_loss": irs_wash["total_disallowed_loss"],
                "total_deferred_loss_to_replacement_basis": (
                    irs_wash.get("total_deferred_loss_to_replacement_basis", 0.0)
                ),
                "total_permanently_disallowed_loss": (
                    irs_wash.get("total_permanently_disallowed_loss", 0.0)
                ),
                "sales": irs_wash["sales"],
                "adjustment_ledger": irs_wash.get("adjustment_ledger", []),
                "replacement_lot_adjustments": irs_wash.get(
                    "replacement_lot_adjustments", []
                ),
            },
        },
    }
    report["broker_vs_irs_reconciliation"] = build_broker_vs_irs_reconciliation(report)
    return report
