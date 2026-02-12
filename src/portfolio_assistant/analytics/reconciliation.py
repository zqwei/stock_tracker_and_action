from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import CashActivity, PnlRealized

EPSILON = 1e-9
CORPORATE_ACTION_KEYWORDS = (
    "SPLIT",
    "REVERSE SPLIT",
    "MERGER",
    "SPIN",
    "SPINOFF",
    "REORG",
    "REORGANIZATION",
    "CUSIP",
    "SYMBOL CHANGE",
)


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return float(text)
        except ValueError:
            return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_symbol(value: Any) -> str:
    return _normalize_text(value).upper()


def _normalize_term(value: Any) -> str:
    term = _normalize_text(value).upper()
    if term in {"SHORT", "ST"}:
        return "SHORT"
    if term in {"LONG", "LT"}:
        return "LONG"
    if not term:
        return "UNKNOWN"
    return term


def _coerce_iso_date_text(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""

    if len(text) >= 10:
        candidate = text[:10]
        try:
            datetime.strptime(candidate, "%Y-%m-%d")
            return candidate
        except ValueError:
            pass

    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.date().isoformat()
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(text)
        return parsed.date().isoformat()
    except ValueError:
        return text


def _row_date_key(row: dict[str, Any]) -> str:
    return _coerce_iso_date_text(
        row.get("date_sold")
        or row.get("sale_date")
        or row.get("close_date")
        or row.get("date")
    )


def _row_symbol_key(row: dict[str, Any]) -> str:
    return _normalize_symbol(row.get("symbol") or row.get("description"))


def _row_cost_basis(row: dict[str, Any]) -> float:
    value = row.get("cost_basis")
    if value is None:
        value = row.get("basis")
    return _as_float(value, 0.0)


def _row_wash_broker(row: dict[str, Any]) -> float:
    wash = _as_float(row.get("wash_sale_disallowed"), 0.0)
    return _as_float(row.get("wash_sale_disallowed_broker"), wash)


def _row_wash_irs(row: dict[str, Any]) -> float:
    wash = _as_float(row.get("wash_sale_disallowed"), 0.0)
    return _as_float(row.get("wash_sale_disallowed_irs"), wash)


def _row_gain_raw(row: dict[str, Any]) -> float:
    gain_or_loss = _as_float(row.get("gain_or_loss"), 0.0)
    raw = row.get("raw_gain_or_loss")
    if raw is None:
        return gain_or_loss
    return _as_float(raw, gain_or_loss)


def _row_gain_broker(row: dict[str, Any]) -> float:
    value = row.get("gain_or_loss_broker")
    if value is not None:
        return _as_float(value, 0.0)
    return _row_gain_raw(row) + _row_wash_broker(row)


def _row_gain_irs(row: dict[str, Any]) -> float:
    value = row.get("gain_or_loss_irs")
    if value is not None:
        return _as_float(value, 0.0)
    return _row_gain_raw(row) + _row_wash_irs(row)


def net_contributions(session: Session, account_id: str | None = None) -> float:
    stmt = select(CashActivity).where(CashActivity.is_external.is_(True))
    if account_id:
        stmt = stmt.where(CashActivity.account_id == account_id)

    total = 0.0
    for row in session.scalars(stmt):
        if row.activity_type.value == "DEPOSIT":
            total += float(row.amount)
        else:
            total -= float(row.amount)
    return total


def contributions_by_month(
    session: Session, account_id: str | None = None
) -> list[dict[str, str | float]]:
    stmt = select(CashActivity).where(CashActivity.is_external.is_(True))
    if account_id:
        stmt = stmt.where(CashActivity.account_id == account_id)

    buckets: dict[str, float] = defaultdict(float)
    for row in session.scalars(stmt):
        key = row.posted_at.strftime("%Y-%m")
        sign = 1.0 if row.activity_type.value == "DEPOSIT" else -1.0
        buckets[key] += sign * float(row.amount)

    return [
        {"month": month, "net_contribution": amount}
        for month, amount in sorted(buckets.items())
    ]


def daily_realized_pnl(
    session: Session, account_id: str | None = None
) -> list[dict[str, date | float]]:
    stmt = select(PnlRealized)
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)

    buckets: dict[date, float] = defaultdict(float)
    for row in session.scalars(stmt):
        buckets[row.close_date] += float(row.pnl)
    return [{"close_date": d, "pnl": pnl} for d, pnl in sorted(buckets.items())]


def realized_by_symbol(
    session: Session, account_id: str | None = None
) -> list[dict[str, str | float]]:
    stmt = select(PnlRealized)
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)

    buckets: dict[tuple[str, str], float] = defaultdict(float)
    for row in session.scalars(stmt):
        key = (row.symbol, row.instrument_type.value)
        buckets[key] += float(row.pnl)
    return [
        {"symbol": symbol, "instrument_type": instrument, "realized_pnl": pnl}
        for (symbol, instrument), pnl in sorted(buckets.items())
    ]


def tax_report_totals(detail_rows: list[dict[str, Any]]) -> dict[str, float]:
    totals = {
        "total_proceeds": 0.0,
        "total_cost_basis": 0.0,
        "total_gain_or_loss": 0.0,
        "total_gain_or_loss_raw": 0.0,
        "short_term_gain_or_loss": 0.0,
        "long_term_gain_or_loss": 0.0,
        "unknown_term_gain_or_loss": 0.0,
        "total_wash_sale_disallowed": 0.0,
        "total_wash_sale_disallowed_broker": 0.0,
        "total_wash_sale_disallowed_irs": 0.0,
        "wash_sale_mode_difference": 0.0,
    }

    for row in detail_rows:
        proceeds = _as_float(row.get("proceeds"), 0.0)
        cost_basis = _row_cost_basis(row)
        gain_or_loss = _as_float(row.get("gain_or_loss"), 0.0)
        raw_gain_or_loss = _row_gain_raw(row)
        wash_disallowed = _as_float(row.get("wash_sale_disallowed"), 0.0)
        wash_disallowed_broker = _row_wash_broker(row)
        wash_disallowed_irs = _row_wash_irs(row)
        term = _normalize_term(row.get("term"))

        totals["total_proceeds"] += proceeds
        totals["total_cost_basis"] += cost_basis
        totals["total_gain_or_loss"] += gain_or_loss
        totals["total_gain_or_loss_raw"] += raw_gain_or_loss
        totals["total_wash_sale_disallowed"] += wash_disallowed
        totals["total_wash_sale_disallowed_broker"] += wash_disallowed_broker
        totals["total_wash_sale_disallowed_irs"] += wash_disallowed_irs

        if term == "SHORT":
            totals["short_term_gain_or_loss"] += gain_or_loss
        elif term == "LONG":
            totals["long_term_gain_or_loss"] += gain_or_loss
        else:
            totals["unknown_term_gain_or_loss"] += gain_or_loss

    totals["wash_sale_mode_difference"] = (
        totals["total_wash_sale_disallowed_irs"]
        - totals["total_wash_sale_disallowed_broker"]
    )
    return totals


def validate_tax_report_summary(report: dict[str, Any], tolerance: float = 1e-6) -> dict[str, Any]:
    detail = report.get("detail_rows") or []
    summary = report.get("summary") or {}
    recomputed = tax_report_totals(detail)

    checks: dict[str, dict[str, float | bool]] = {}
    for key, recomputed_value in recomputed.items():
        summary_value = _as_float(summary.get(key), 0.0)
        delta = summary_value - recomputed_value
        checks[key] = {
            "summary": summary_value,
            "recomputed": recomputed_value,
            "delta": delta,
            "ok": abs(delta) <= tolerance,
        }

    recomputed_math_raw = (
        abs(
            (recomputed["total_proceeds"] - recomputed["total_cost_basis"])
            - recomputed["total_gain_or_loss_raw"]
        )
        <= tolerance
    )
    recomputed_math_adjusted = (
        abs(
            (recomputed["total_gain_or_loss_raw"] + recomputed["total_wash_sale_disallowed_irs"])
            - recomputed["total_gain_or_loss"]
        )
        <= tolerance
    )
    summary_math_raw = bool(summary.get("math_check_raw", recomputed_math_raw))
    summary_math_adjusted = bool(
        summary.get("math_check_adjusted", recomputed_math_adjusted)
    )
    checks["math_check_raw"] = {
        "summary": summary_math_raw,
        "recomputed": recomputed_math_raw,
        "delta": 0.0 if summary_math_raw == recomputed_math_raw else 1.0,
        "ok": summary_math_raw == recomputed_math_raw,
    }
    checks["math_check_adjusted"] = {
        "summary": summary_math_adjusted,
        "recomputed": recomputed_math_adjusted,
        "delta": 0.0 if summary_math_adjusted == recomputed_math_adjusted else 1.0,
        "ok": summary_math_adjusted == recomputed_math_adjusted,
    }

    checks["rows"] = {
        "summary": _as_float(summary.get("rows"), 0.0),
        "recomputed": float(len(detail)),
        "delta": _as_float(summary.get("rows"), 0.0) - float(len(detail)),
        "ok": int(_as_float(summary.get("rows"), 0.0)) == len(detail),
    }

    return {
        "ok": all(bool(result["ok"]) for result in checks.values()),
        "checks": checks,
    }


def compare_totals(
    app_totals: dict[str, float], broker_totals: dict[str, float]
) -> dict[str, dict[str, float]]:
    keys = [
        "total_proceeds",
        "total_cost_basis",
        "total_gain_or_loss",
        "short_term_gain_or_loss",
        "long_term_gain_or_loss",
        "total_wash_sale_disallowed",
    ]
    comparison: dict[str, dict[str, float]] = {}
    for key in keys:
        app_value = float(app_totals.get(key, 0.0) or 0.0)
        broker_value = float(broker_totals.get(key, 0.0) or 0.0)
        comparison[key] = {
            "app": app_value,
            "broker": broker_value,
            "delta": app_value - broker_value,
        }
    return comparison


def _aggregate_rows(
    rows: list[dict[str, Any]], key_fn: Callable[[dict[str, Any]], str]
) -> dict[str, dict[str, float]]:
    buckets: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "proceeds": 0.0,
            "cost_basis": 0.0,
            "gain_or_loss": 0.0,
            "wash_sale_disallowed": 0.0,
            "count": 0.0,
        }
    )
    for row in rows:
        key = key_fn(row)
        bucket = buckets[key]
        bucket["proceeds"] += _as_float(row.get("proceeds"), 0.0)
        bucket["cost_basis"] += _row_cost_basis(row)
        bucket["gain_or_loss"] += _as_float(row.get("gain_or_loss"), 0.0)
        bucket["wash_sale_disallowed"] += _as_float(row.get("wash_sale_disallowed"), 0.0)
        bucket["count"] += 1.0
    return buckets


def _diff_from_aggregates(
    app_agg: dict[str, dict[str, float]],
    broker_agg: dict[str, dict[str, float]],
    key_name: str,
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    keys = sorted(set(app_agg) | set(broker_agg))
    for key in keys:
        app_bucket = app_agg.get(key) or {}
        broker_bucket = broker_agg.get(key) or {}
        app_proceeds = _as_float(app_bucket.get("proceeds"), 0.0)
        broker_proceeds = _as_float(broker_bucket.get("proceeds"), 0.0)
        app_basis = _as_float(app_bucket.get("cost_basis"), 0.0)
        broker_basis = _as_float(broker_bucket.get("cost_basis"), 0.0)
        app_gain = _as_float(app_bucket.get("gain_or_loss"), 0.0)
        broker_gain = _as_float(broker_bucket.get("gain_or_loss"), 0.0)
        app_wash = _as_float(app_bucket.get("wash_sale_disallowed"), 0.0)
        broker_wash = _as_float(broker_bucket.get("wash_sale_disallowed"), 0.0)

        rows.append(
            {
                key_name: key,
                "app_proceeds": app_proceeds,
                "broker_proceeds": broker_proceeds,
                "delta_proceeds": app_proceeds - broker_proceeds,
                "app_cost_basis": app_basis,
                "broker_cost_basis": broker_basis,
                "delta_cost_basis": app_basis - broker_basis,
                "app_gain_or_loss": app_gain,
                "broker_gain_or_loss": broker_gain,
                "delta_gain_or_loss": app_gain - broker_gain,
                "app_wash_sale_disallowed": app_wash,
                "broker_wash_sale_disallowed": broker_wash,
                "delta_wash_sale_disallowed": app_wash - broker_wash,
                "app_count": _as_float(app_bucket.get("count"), 0.0),
                "broker_count": _as_float(broker_bucket.get("count"), 0.0),
            }
        )
    return rows


def build_app_vs_broker_diff_tables(
    app_detail_rows: list[dict[str, Any]],
    broker_detail_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, float | str]]]:
    app_by_symbol = _aggregate_rows(app_detail_rows, _row_symbol_key)
    broker_by_symbol = _aggregate_rows(broker_detail_rows, _row_symbol_key)
    by_symbol = _diff_from_aggregates(app_by_symbol, broker_by_symbol, "symbol")

    app_by_sale_date = _aggregate_rows(app_detail_rows, _row_date_key)
    broker_by_sale_date = _aggregate_rows(broker_detail_rows, _row_date_key)
    by_sale_date = _diff_from_aggregates(app_by_sale_date, broker_by_sale_date, "sale_date")

    app_by_term = _aggregate_rows(app_detail_rows, lambda row: _normalize_term(row.get("term")))
    broker_by_term = _aggregate_rows(
        broker_detail_rows, lambda row: _normalize_term(row.get("term"))
    )
    by_term = _diff_from_aggregates(app_by_term, broker_by_term, "term")

    return {
        "by_symbol": by_symbol,
        "by_sale_date": by_sale_date,
        "by_term": by_term,
    }


def broker_vs_irs_diffs(detail_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_trade: list[dict[str, Any]] = []
    by_symbol: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "broker_gain_or_loss": 0.0,
            "irs_gain_or_loss": 0.0,
            "gain_or_loss_delta": 0.0,
            "broker_wash_sale_disallowed": 0.0,
            "irs_wash_sale_disallowed": 0.0,
            "wash_sale_disallowed_delta": 0.0,
            "count": 0.0,
        }
    )
    by_sale_date: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "broker_gain_or_loss": 0.0,
            "irs_gain_or_loss": 0.0,
            "gain_or_loss_delta": 0.0,
            "broker_wash_sale_disallowed": 0.0,
            "irs_wash_sale_disallowed": 0.0,
            "wash_sale_disallowed_delta": 0.0,
            "count": 0.0,
        }
    )
    by_term: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "broker_gain_or_loss": 0.0,
            "irs_gain_or_loss": 0.0,
            "gain_or_loss_delta": 0.0,
            "broker_wash_sale_disallowed": 0.0,
            "irs_wash_sale_disallowed": 0.0,
            "wash_sale_disallowed_delta": 0.0,
            "count": 0.0,
        }
    )

    for row in detail_rows:
        symbol = _row_symbol_key(row)
        sale_date = _row_date_key(row)
        term = _normalize_term(row.get("term"))

        broker_gain = _row_gain_broker(row)
        irs_gain = _row_gain_irs(row)
        gain_delta = irs_gain - broker_gain
        broker_wash = _row_wash_broker(row)
        irs_wash = _row_wash_irs(row)
        wash_delta = irs_wash - broker_wash

        by_trade.append(
            {
                "sale_row_id": int(_as_float(row.get("sale_row_id"), 0.0)),
                "symbol": symbol,
                "sale_date": sale_date,
                "term": term,
                "raw_gain_or_loss": _row_gain_raw(row),
                "broker_gain_or_loss": broker_gain,
                "irs_gain_or_loss": irs_gain,
                "gain_or_loss_delta": gain_delta,
                "broker_wash_sale_disallowed": broker_wash,
                "irs_wash_sale_disallowed": irs_wash,
                "wash_sale_disallowed_delta": wash_delta,
            }
        )

        for bucket in (by_symbol[symbol], by_sale_date[sale_date], by_term[term]):
            bucket["broker_gain_or_loss"] += broker_gain
            bucket["irs_gain_or_loss"] += irs_gain
            bucket["gain_or_loss_delta"] += gain_delta
            bucket["broker_wash_sale_disallowed"] += broker_wash
            bucket["irs_wash_sale_disallowed"] += irs_wash
            bucket["wash_sale_disallowed_delta"] += wash_delta
            bucket["count"] += 1.0

    by_trade.sort(key=lambda row: (str(row["sale_date"]), str(row["symbol"]), int(row["sale_row_id"])))

    def _flatten(
        label: str, buckets: dict[str, dict[str, float]]
    ) -> list[dict[str, float | str]]:
        rows: list[dict[str, float | str]] = []
        for key in sorted(buckets):
            bucket = buckets[key]
            rows.append(
                {
                    label: key,
                    "broker_gain_or_loss": float(bucket["broker_gain_or_loss"]),
                    "irs_gain_or_loss": float(bucket["irs_gain_or_loss"]),
                    "gain_or_loss_delta": float(bucket["gain_or_loss_delta"]),
                    "broker_wash_sale_disallowed": float(
                        bucket["broker_wash_sale_disallowed"]
                    ),
                    "irs_wash_sale_disallowed": float(bucket["irs_wash_sale_disallowed"]),
                    "wash_sale_disallowed_delta": float(bucket["wash_sale_disallowed_delta"]),
                    "count": int(bucket["count"]),
                }
            )
        return rows

    totals = {
        "broker_gain_or_loss": float(sum(row["broker_gain_or_loss"] for row in by_trade)),
        "irs_gain_or_loss": float(sum(row["irs_gain_or_loss"] for row in by_trade)),
        "gain_or_loss_delta": float(sum(row["gain_or_loss_delta"] for row in by_trade)),
        "broker_wash_sale_disallowed": float(
            sum(row["broker_wash_sale_disallowed"] for row in by_trade)
        ),
        "irs_wash_sale_disallowed": float(sum(row["irs_wash_sale_disallowed"] for row in by_trade)),
        "wash_sale_disallowed_delta": float(
            sum(row["wash_sale_disallowed_delta"] for row in by_trade)
        ),
        "rows": len(by_trade),
    }
    return {
        "by_trade": by_trade,
        "by_symbol": _flatten("symbol", by_symbol),
        "by_sale_date": _flatten("sale_date", by_sale_date),
        "by_term": _flatten("term", by_term),
        "totals": totals,
    }


def build_broker_vs_irs_diffs(detail_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return broker_vs_irs_diffs(detail_rows)


def _collect_irs_matches(wash_sale_summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    irs_sales = ((wash_sale_summary or {}).get("irs") or {}).get("sales") or []
    matches: list[dict[str, Any]] = []
    for sale in irs_sales:
        sale_date = _coerce_iso_date_text(sale.get("sale_date"))
        for match in sale.get("matches") or []:
            matches.append(
                {
                    "sale_row_id": int(_as_float(sale.get("sale_row_id"), 0.0)),
                    "symbol": _normalize_symbol(sale.get("symbol")),
                    "sale_date": sale_date,
                    "buy_date": _coerce_iso_date_text(match.get("buy_date")),
                    "days_from_sale": int(_as_float(match.get("days_from_sale"), 0.0)),
                    "cross_account": bool(match.get("cross_account")),
                    "ira_replacement": bool(match.get("ira_replacement")),
                    "buy_instrument_type": _normalize_text(
                        match.get("buy_instrument_type")
                    ).upper(),
                }
            )
    return matches


def _sample_symbols(
    evidence: list[dict[str, Any]], symbol_key: str = "symbol", limit: int = 3
) -> list[str]:
    seen: list[str] = []
    for row in evidence:
        symbol = _normalize_symbol(row.get(symbol_key))
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.append(symbol)
        if len(seen) >= limit:
            break
    return seen


def _sale_date_from_row(row: dict[str, Any]) -> date | None:
    sale_date_text = _row_date_key(row)
    if not sale_date_text:
        return None
    try:
        return datetime.strptime(sale_date_text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _build_checklist_rows(
    *,
    tax_year: int | None,
    mode_diffs: dict[str, Any],
    wash_sale_summary: dict[str, Any] | None,
    detail_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    irs_matches = _collect_irs_matches(wash_sale_summary)
    mode_totals = mode_diffs.get("totals") or {}
    gain_delta_abs = abs(_as_float(mode_totals.get("gain_or_loss_delta"), 0.0))
    wash_delta_abs = abs(_as_float(mode_totals.get("wash_sale_disallowed_delta"), 0.0))

    missing_boundary_evidence = []
    if tax_year is not None:
        for match in irs_matches:
            sale_date_text = _normalize_text(match.get("sale_date"))
            buy_date_text = _normalize_text(match.get("buy_date"))
            try:
                sale_date = datetime.strptime(sale_date_text, "%Y-%m-%d").date()
                buy_date = datetime.strptime(buy_date_text, "%Y-%m-%d").date()
            except ValueError:
                continue

            if sale_date.year != tax_year:
                continue
            if buy_date.year != sale_date.year:
                missing_boundary_evidence.append(
                    {
                        "sale_row_id": match["sale_row_id"],
                        "symbol": match["symbol"],
                        "sale_date": sale_date.isoformat(),
                        "buy_date": buy_date.isoformat(),
                    }
                )

    boundary_sale_evidence = []
    if tax_year is not None:
        for row in detail_rows:
            sale_date = _sale_date_from_row(row)
            if sale_date is None or sale_date.year != tax_year:
                continue
            if sale_date.month not in {1, 12}:
                continue
            boundary_sale_evidence.append(
                {
                    "sale_row_id": int(_as_float(row.get("sale_row_id"), 0.0)),
                    "symbol": _row_symbol_key(row),
                    "sale_date": sale_date.isoformat(),
                }
            )

    partial_replacement_evidence = []
    irs_sales = ((wash_sale_summary or {}).get("irs") or {}).get("sales") or []
    for sale in irs_sales:
        matched_qty = _as_float(sale.get("matched_replacement_quantity_equiv"), 0.0)
        sale_qty = _as_float(sale.get("sale_quantity_equiv"), 0.0)
        if sale_qty <= EPSILON:
            continue
        if matched_qty >= (sale_qty - EPSILON):
            continue
        partial_replacement_evidence.append(
            {
                "sale_row_id": int(_as_float(sale.get("sale_row_id"), 0.0)),
                "symbol": _normalize_symbol(sale.get("symbol")),
                "sale_date": _coerce_iso_date_text(sale.get("sale_date")),
                "matched_replacement_quantity_equiv": matched_qty,
                "sale_quantity_equiv": sale_qty,
            }
        )

    cross_account_evidence = [
        {
            "sale_row_id": row["sale_row_id"],
            "symbol": row["symbol"],
            "sale_date": row["sale_date"],
            "buy_date": row["buy_date"],
        }
        for row in irs_matches
        if bool(row.get("cross_account"))
    ]
    options_evidence = [
        {
            "sale_row_id": row["sale_row_id"],
            "symbol": row["symbol"],
            "sale_date": row["sale_date"],
            "buy_date": row["buy_date"],
            "buy_instrument_type": row["buy_instrument_type"],
        }
        for row in irs_matches
        if row.get("buy_instrument_type") == "OPTION"
    ]

    trade_diffs = mode_diffs.get("by_trade") or []
    lot_mismatch_evidence = [
        {
            "sale_row_id": row["sale_row_id"],
            "symbol": row["symbol"],
            "sale_date": row["sale_date"],
            "gain_or_loss_delta": row["gain_or_loss_delta"],
            "wash_sale_disallowed_delta": row["wash_sale_disallowed_delta"],
        }
        for row in trade_diffs
        if abs(_as_float(row.get("gain_or_loss_delta"), 0.0)) > EPSILON
        and abs(_as_float(row.get("wash_sale_disallowed_delta"), 0.0)) <= EPSILON
    ]

    missing_boundary_flag = bool(missing_boundary_evidence)
    if (
        not missing_boundary_flag
        and boundary_sale_evidence
        and wash_delta_abs > EPSILON
        and gain_delta_abs > EPSILON
    ):
        # If we have boundary-period sales and material wash/gain deltas without direct cross-year
        # evidence, warn that Y-1/Y+1 history may still be incomplete.
        missing_boundary_flag = True

    missing_boundary_symbols = _sample_symbols(
        missing_boundary_evidence or boundary_sale_evidence
    )
    cross_account_symbols = _sample_symbols(cross_account_evidence)
    options_symbols = _sample_symbols(options_evidence)
    lot_mismatch_symbols = _sample_symbols(lot_mismatch_evidence)

    corporate_action_evidence = []
    for row in detail_rows:
        description = _normalize_text(row.get("description") or row.get("symbol"))
        upper = description.upper()
        if any(keyword in upper for keyword in CORPORATE_ACTION_KEYWORDS):
            corporate_action_evidence.append(
                {
                    "sale_row_id": int(_as_float(row.get("sale_row_id"), 0.0)),
                    "description": description,
                }
            )

    checklist = [
        {
            "key": "missing_boundary_data",
            "title": "Missing boundary data?",
            "flag": missing_boundary_flag,
            "status": "YES" if missing_boundary_flag else "NO",
            "reason": (
                (
                    f"Detected {len(missing_boundary_evidence)} cross-year replacement link(s)"
                    f" across boundary sales (sample: {', '.join(missing_boundary_symbols) or 'n/a'})."
                )
                if missing_boundary_evidence
                else (
                    "Boundary-period sales plus material broker-vs-IRS deltas suggest"
                    " possible missing Y-1/Y+1 trade history."
                    if missing_boundary_flag
                    else "No boundary-data warning signals detected."
                )
            ),
            "evidence": missing_boundary_evidence,
            "signal_count": len(missing_boundary_evidence) or len(boundary_sale_evidence),
            "sample_symbols": missing_boundary_symbols,
            "links": ["mode_diffs.by_sale_date", "mode_diffs.by_trade"],
        },
        {
            "key": "cross_account_replacements_likely",
            "title": "Cross-account replacements likely?",
            "flag": bool(cross_account_evidence),
            "status": "YES" if cross_account_evidence else "NO",
            "reason": (
                (
                    f"Found {len(cross_account_evidence)} cross-account replacement link(s)"
                    f" (sample: {', '.join(cross_account_symbols) or 'n/a'})."
                )
                if cross_account_evidence
                else "No cross-account replacement matches detected."
            ),
            "evidence": cross_account_evidence,
            "signal_count": len(cross_account_evidence),
            "sample_symbols": cross_account_symbols,
            "links": ["mode_diffs.by_trade", "mode_diffs.by_symbol"],
        },
        {
            "key": "options_replacements_likely",
            "title": "Options replacements likely?",
            "flag": bool(options_evidence),
            "status": "YES" if options_evidence else "NO",
            "reason": (
                (
                    f"Found {len(options_evidence)} option replacement link(s)"
                    f" (sample: {', '.join(options_symbols) or 'n/a'})."
                )
                if options_evidence
                else "No option replacement matches detected."
            ),
            "evidence": options_evidence,
            "signal_count": len(options_evidence),
            "sample_symbols": options_symbols,
            "links": ["mode_diffs.by_trade"],
        },
        {
            "key": "lot_method_mismatch",
            "title": "Lot method mismatch?",
            "flag": bool(lot_mismatch_evidence),
            "status": "YES" if lot_mismatch_evidence else "NO",
            "reason": (
                (
                    f"Per-trade gain deltas exist without wash deltas"
                    f" on {len(lot_mismatch_evidence)} row(s)"
                    f" (sample: {', '.join(lot_mismatch_symbols) or 'n/a'})."
                )
                if lot_mismatch_evidence
                else (
                    "All mode differences are explained by wash-sale adjustments."
                    + (
                        f" Partial replacement patterns detected on {len(partial_replacement_evidence)}"
                        " sale(s)."
                        if partial_replacement_evidence
                        else ""
                    )
                )
            ),
            "evidence": lot_mismatch_evidence,
            "signal_count": len(lot_mismatch_evidence),
            "sample_symbols": lot_mismatch_symbols,
            "links": ["mode_diffs.by_trade"],
        },
        {
            "key": "corporate_actions_present",
            "title": "Corporate actions present?",
            "flag": bool(corporate_action_evidence),
            "status": "YES" if corporate_action_evidence else "NO",
            "reason": (
                f"Corporate-action keywords detected in {len(corporate_action_evidence)} row(s)."
                if corporate_action_evidence
                else "No corporate-action keywords detected in disposition descriptions."
            ),
            "evidence": corporate_action_evidence,
            "signal_count": len(corporate_action_evidence),
            "sample_symbols": _sample_symbols(corporate_action_evidence, symbol_key="description"),
            "links": ["mode_diffs.by_symbol", "mode_diffs.by_trade"],
        },
    ]
    return checklist


def build_reconciliation_checklist(
    report: dict[str, Any],
    mode_diffs: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    summary = report.get("summary") or {}
    tax_year = int(_as_float(summary.get("tax_year"), 0.0)) if summary.get("tax_year") else None
    detail_rows = report.get("detail_rows") or []
    if mode_diffs is None:
        mode_diffs = broker_vs_irs_diffs(detail_rows)
    wash_sale_summary = report.get("wash_sale_summary") or {}
    return _build_checklist_rows(
        tax_year=tax_year,
        mode_diffs=mode_diffs,
        wash_sale_summary=wash_sale_summary,
        detail_rows=detail_rows,
    )


def broker_vs_irs_checklist(
    report: dict[str, Any], mode_diffs: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    return build_reconciliation_checklist(report, mode_diffs=mode_diffs)


def build_broker_vs_irs_reconciliation(report: dict[str, Any]) -> dict[str, Any]:
    detail_rows = report.get("detail_rows") or []
    mode_diffs = broker_vs_irs_diffs(detail_rows)
    checklist = build_reconciliation_checklist(report, mode_diffs=mode_diffs)
    return {
        "mode_diffs": mode_diffs,
        "checklist": checklist,
    }
