from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from math import isfinite
from typing import Any

from dateutil.relativedelta import relativedelta
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import CashActivity, PriceCache, TradeNormalized
from portfolio_assistant.ingest.validators import compute_signed_trade_cash, normalize_side

BENCHMARK_SYMBOLS: tuple[str, ...] = ("DIA", "SPY", "QQQ")
WINDOW_LABELS: list[str] = ["Since inception", "1Y", "6M", "3M", "1M", "5D"]

WINDOW_DELTAS: dict[str, relativedelta] = {
    "1Y": relativedelta(years=1),
    "6M": relativedelta(months=6),
    "3M": relativedelta(months=3),
    "1M": relativedelta(months=1),
    "5D": relativedelta(days=5),
}

_EPSILON = 1e-9


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _end_of_day(day: date) -> datetime:
    return datetime.combine(day, time.max)


def _start_of_day(day: date) -> datetime:
    return datetime.combine(day, time.min)


def _cash_activity_signed_amount(row: CashActivity) -> float:
    activity = _enum_value(row.activity_type).upper()
    sign = 1.0 if activity == "DEPOSIT" else -1.0
    return sign * float(row.amount)


def _trade_cash_signed_amount(row: TradeNormalized) -> float:
    if row.net_amount is not None:
        return float(row.net_amount)

    side = normalize_side(_enum_value(row.side))
    instrument = _enum_value(row.instrument_type).upper()
    quantity = abs(float(row.quantity or 0.0))
    price = float(row.price or 0.0)
    fees = float(row.fees or 0.0)
    multiplier = int(row.multiplier or 1)
    if instrument != "OPTION":
        multiplier = 1
    elif multiplier <= 0:
        multiplier = 100
    return compute_signed_trade_cash(
        side=side,
        quantity=quantity,
        price=price,
        fees=fees,
        multiplier=multiplier,
    )


def _trade_position_units_delta(row: TradeNormalized) -> float:
    side = normalize_side(_enum_value(row.side))
    quantity = abs(float(row.quantity or 0.0))
    if quantity <= 0:
        return 0.0

    if side in {"BUY", "BTO", "BTC"}:
        sign = 1.0
    elif side in {"SELL", "STO", "STC"}:
        sign = -1.0
    else:
        return 0.0

    instrument = _enum_value(row.instrument_type).upper()
    multiplier = int(row.multiplier or 1)
    if instrument != "OPTION":
        multiplier = 1
    elif multiplier <= 0:
        multiplier = 100
    return sign * quantity * multiplier


def _valuation_symbol(row: TradeNormalized) -> str:
    instrument = _enum_value(row.instrument_type).upper()
    if instrument == "OPTION":
        raw = (row.option_symbol_raw or "").strip().upper()
        if raw:
            return raw
        fallback = (row.underlying or row.symbol or "").strip().upper()
        return fallback

    return (row.symbol or row.underlying or "").strip().upper()


def _latest_prices_for_symbols(
    session: Session,
    symbols: list[str],
    as_of_dt: datetime,
) -> dict[str, float]:
    cleaned = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not cleaned:
        return {}

    stmt = (
        select(PriceCache.symbol, PriceCache.close, PriceCache.as_of)
        .where(PriceCache.symbol.in_(cleaned), PriceCache.as_of <= as_of_dt)
        .order_by(PriceCache.symbol, PriceCache.as_of.desc())
    )

    latest: dict[str, float] = {}
    for symbol, close, _as_of in session.execute(stmt):
        if symbol not in latest:
            latest[str(symbol)] = float(close)
    return latest


def _price_on_or_before(session: Session, symbol: str, as_of_dt: datetime) -> float | None:
    stmt = (
        select(PriceCache.close)
        .where(PriceCache.symbol == symbol, PriceCache.as_of <= as_of_dt)
        .order_by(PriceCache.as_of.desc())
        .limit(1)
    )
    value = session.scalar(stmt)
    if value is None:
        return None
    return float(value)


def _price_between_after_start(
    session: Session, symbol: str, start_dt: datetime, end_dt: datetime
) -> float | None:
    stmt = (
        select(PriceCache.close)
        .where(
            PriceCache.symbol == symbol,
            PriceCache.as_of >= start_dt,
            PriceCache.as_of <= end_dt,
        )
        .order_by(PriceCache.as_of.asc())
        .limit(1)
    )
    value = session.scalar(stmt)
    if value is None:
        return None
    return float(value)


def _benchmark_return(
    session: Session,
    symbol: str,
    start_date: date,
    end_date: date,
) -> float | None:
    start_dt = _end_of_day(start_date)
    end_dt = _end_of_day(end_date)
    if end_dt < start_dt:
        return None

    start_price = _price_on_or_before(session, symbol, start_dt)
    if start_price is None:
        start_price = _price_between_after_start(session, symbol, _start_of_day(start_date), end_dt)

    end_price = _price_on_or_before(session, symbol, end_dt)

    if start_price is None or end_price is None or start_price <= 0:
        return None
    return (end_price / start_price) - 1.0


def _portfolio_snapshot(
    session: Session,
    *,
    as_of_date: date,
    account_id: str | None,
) -> dict[str, Any]:
    as_of_dt = _end_of_day(as_of_date)

    cash_stmt = select(CashActivity).where(CashActivity.posted_at <= as_of_dt)
    if account_id:
        cash_stmt = cash_stmt.where(CashActivity.account_id == account_id)

    trade_stmt = select(TradeNormalized).where(TradeNormalized.executed_at <= as_of_dt)
    if account_id:
        trade_stmt = trade_stmt.where(TradeNormalized.account_id == account_id)

    cash_balance = 0.0
    for row in session.scalars(cash_stmt):
        cash_balance += _cash_activity_signed_amount(row)

    units_by_symbol: dict[str, float] = defaultdict(float)
    for row in session.scalars(trade_stmt):
        cash_balance += _trade_cash_signed_amount(row)
        symbol = _valuation_symbol(row)
        if not symbol:
            continue
        units_by_symbol[symbol] += _trade_position_units_delta(row)

    active_symbols: dict[str, float] = {
        symbol: units
        for symbol, units in units_by_symbol.items()
        if abs(units) > _EPSILON
    }
    prices = _latest_prices_for_symbols(session, list(active_symbols), as_of_dt)

    holdings_value = 0.0
    missing_symbols: list[str] = []
    priced_symbols = 0
    for symbol, units in active_symbols.items():
        price = prices.get(symbol)
        if price is None:
            missing_symbols.append(symbol)
            continue
        holdings_value += units * price
        priced_symbols += 1

    return {
        "as_of_date": as_of_date,
        "cash_balance": cash_balance,
        "holdings_value": holdings_value,
        "equity": cash_balance + holdings_value,
        "missing_symbols": sorted(missing_symbols),
        "priced_symbols": priced_symbols,
        "position_symbols": len(active_symbols),
    }


def _xnpv(rate: float, cash_flows: list[tuple[date, float]]) -> float:
    if rate <= -0.999999999:
        raise ValueError("rate must be > -1")
    if not cash_flows:
        return 0.0

    anchor = cash_flows[0][0]
    total = 0.0
    for flow_date, amount in cash_flows:
        years = (flow_date - anchor).days / 365.0
        total += amount / ((1.0 + rate) ** years)
    return total


def _xirr(cash_flows: list[tuple[date, float]]) -> float | None:
    if len(cash_flows) < 2:
        return None

    cleaned = sorted(
        [
            (flow_date, float(amount))
            for flow_date, amount in cash_flows
            if abs(float(amount)) > _EPSILON
        ],
        key=lambda item: item[0],
    )
    if len(cleaned) < 2:
        return None

    has_positive = any(amount > 0 for _, amount in cleaned)
    has_negative = any(amount < 0 for _, amount in cleaned)
    if not has_positive or not has_negative:
        return None

    if cleaned[0][0] == cleaned[-1][0]:
        total = sum(amount for _, amount in cleaned)
        if abs(total) <= _EPSILON:
            return 0.0
        return None

    low = -0.9999
    high = 1.0

    try:
        f_low = _xnpv(low, cleaned)
    except Exception:
        return None

    try:
        f_high = _xnpv(high, cleaned)
    except Exception:
        f_high = float("nan")

    bracket_found = isfinite(f_high) and (f_low == 0.0 or f_low * f_high <= 0.0)
    attempts = 0
    while not bracket_found and attempts < 80:
        high = (high * 2.0) + 1.0
        attempts += 1
        try:
            f_high = _xnpv(high, cleaned)
        except Exception:
            continue
        bracket_found = isfinite(f_high) and (f_low == 0.0 or f_low * f_high <= 0.0)

    if not bracket_found:
        return None

    if abs(f_low) <= 1e-12:
        return low
    if abs(float(f_high)) <= 1e-12:
        return high

    for _ in range(200):
        mid = (low + high) / 2.0
        f_mid = _xnpv(mid, cleaned)
        if abs(f_mid) <= 1e-10:
            return mid
        if f_low * f_mid <= 0:
            high = mid
            f_high = f_mid
        else:
            low = mid
            f_low = f_mid

    return (low + high) / 2.0


def _latest_data_date(
    session: Session,
    account_id: str | None,
) -> date:
    cash_stmt = select(func.max(CashActivity.posted_at))
    trade_stmt = select(func.max(TradeNormalized.executed_at))
    if account_id:
        cash_stmt = cash_stmt.where(CashActivity.account_id == account_id)
        trade_stmt = trade_stmt.where(TradeNormalized.account_id == account_id)

    cash_latest = session.scalar(cash_stmt)
    trade_latest = session.scalar(trade_stmt)
    price_latest = session.scalar(select(func.max(PriceCache.as_of)))

    candidates = [
        value.date()
        for value in [cash_latest, trade_latest, price_latest]
        if isinstance(value, datetime)
    ]
    if not candidates:
        return date.today()
    return max(candidates)


def _inception_date(session: Session, account_id: str | None, fallback_end: date) -> date:
    external_stmt = select(func.min(CashActivity.posted_at)).where(
        CashActivity.is_external.is_(True),
        CashActivity.activity_type == "DEPOSIT",
    )
    cash_stmt = select(func.min(CashActivity.posted_at))
    trade_stmt = select(func.min(TradeNormalized.executed_at))

    if account_id:
        external_stmt = external_stmt.where(CashActivity.account_id == account_id)
        cash_stmt = cash_stmt.where(CashActivity.account_id == account_id)
        trade_stmt = trade_stmt.where(TradeNormalized.account_id == account_id)

    first_external = session.scalar(external_stmt)
    if isinstance(first_external, datetime):
        return first_external.date()

    candidates = [
        value.date()
        for value in [session.scalar(cash_stmt), session.scalar(trade_stmt)]
        if isinstance(value, datetime)
    ]
    if not candidates:
        return fallback_end
    return min(candidates)


def _resolve_window_start(
    session: Session,
    *,
    account_id: str | None,
    end_date: date,
    window: str,
) -> date:
    inception = _inception_date(session, account_id, fallback_end=end_date)
    if window == "Since inception":
        return inception

    delta = WINDOW_DELTAS.get(window)
    if delta is None:
        raise ValueError(f"Unsupported window: {window}")

    shifted = end_date - delta
    if shifted < inception:
        return inception
    return shifted


def compute_window_metrics(
    session: Session,
    *,
    account_id: str | None = None,
    window: str = "Since inception",
    as_of: date | datetime | None = None,
) -> dict[str, Any]:
    if window not in WINDOW_LABELS:
        raise ValueError(f"Unsupported window: {window}")

    if isinstance(as_of, datetime):
        end_date = as_of.date()
    elif isinstance(as_of, date):
        end_date = as_of
    else:
        end_date = _latest_data_date(session, account_id=account_id)

    start_date = _resolve_window_start(
        session,
        account_id=account_id,
        end_date=end_date,
        window=window,
    )
    if start_date > end_date:
        start_date = end_date

    start_anchor = start_date - timedelta(days=1)
    start_snapshot = _portfolio_snapshot(
        session,
        as_of_date=start_anchor,
        account_id=account_id,
    )
    end_snapshot = _portfolio_snapshot(
        session,
        as_of_date=end_date,
        account_id=account_id,
    )

    flow_stmt = select(CashActivity).where(
        CashActivity.is_external.is_(True),
        CashActivity.posted_at >= _start_of_day(start_date),
        CashActivity.posted_at <= _end_of_day(end_date),
    )
    if account_id:
        flow_stmt = flow_stmt.where(CashActivity.account_id == account_id)

    external_net_flow = 0.0
    investor_flows_by_day: dict[date, float] = defaultdict(float)
    flow_rows = list(session.scalars(flow_stmt).all())
    for row in flow_rows:
        signed = _cash_activity_signed_amount(row)
        external_net_flow += signed
        investor_flows_by_day[row.posted_at.date()] += -signed

    xirr_cash_flows: list[tuple[date, float]] = [(start_anchor, -float(start_snapshot["equity"]))]
    for flow_date in sorted(investor_flows_by_day):
        amount = investor_flows_by_day[flow_date]
        if abs(amount) > _EPSILON:
            xirr_cash_flows.append((flow_date, amount))
    xirr_cash_flows.append((end_date, float(end_snapshot["equity"])))

    xirr_annualized = _xirr(xirr_cash_flows)
    period_days = max((end_date - start_date).days, 1)
    portfolio_return = None
    if xirr_annualized is not None and xirr_annualized > -1.0:
        portfolio_return = (1.0 + xirr_annualized) ** (period_days / 365.0) - 1.0

    benchmark_returns: dict[str, float | None] = {}
    missing_benchmark_symbols: list[str] = []
    for symbol in BENCHMARK_SYMBOLS:
        value = _benchmark_return(
            session=session,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        benchmark_returns[symbol] = value
        if value is None:
            missing_benchmark_symbols.append(symbol)

    return {
        "window": window,
        "start_date": start_date,
        "end_date": end_date,
        "start_equity": float(start_snapshot["equity"]),
        "end_equity": float(end_snapshot["equity"]),
        "external_net_flow": external_net_flow,
        "xirr_annualized": xirr_annualized,
        "portfolio_return": portfolio_return,
        "benchmark_returns": benchmark_returns,
        "missing_benchmark_symbols": sorted(missing_benchmark_symbols),
        "missing_position_prices_start": list(start_snapshot["missing_symbols"]),
        "missing_position_prices_end": list(end_snapshot["missing_symbols"]),
        "position_symbols_start": int(start_snapshot["position_symbols"]),
        "position_symbols_end": int(end_snapshot["position_symbols"]),
        "external_cash_flow_rows": len(flow_rows),
    }


def compute_all_window_metrics(
    session: Session,
    *,
    account_id: str | None = None,
    as_of: date | datetime | None = None,
) -> list[dict[str, Any]]:
    return [
        compute_window_metrics(
            session,
            account_id=account_id,
            window=window,
            as_of=as_of,
        )
        for window in WINDOW_LABELS
    ]

