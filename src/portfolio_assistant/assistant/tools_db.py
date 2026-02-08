from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager

from sqlalchemy import case, delete, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from portfolio_assistant.db.migrate import build_engine
from portfolio_assistant.db.models import (
    Account,
    CashActivity,
    PnlRealized,
    PositionOpen,
    PriceCache,
    TradeNormalized,
    TradeRaw,
)


@contextmanager
def session_scope(engine: Engine):
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_engine() -> Engine:
    return build_engine()


def create_account(
    session: Session, broker: str, account_label: str, account_type: str
) -> Account:
    account = Account(
        broker=broker.strip(),
        account_label=account_label.strip(),
        account_type=account_type.strip(),
    )
    session.add(account)
    session.flush()
    return account


def list_accounts(session: Session) -> list[Account]:
    return list(
        session.scalars(select(Account).order_by(Account.broker, Account.account_label)).all()
    )


def insert_trade_import(
    session: Session,
    account_id: str,
    broker: str,
    source_file: str,
    file_sig: str,
    mapping_name: str,
    raw_rows: Iterable[dict],
    normalized_rows: Iterable[dict],
) -> tuple[int, int]:
    raw_count = 0
    for idx, row in enumerate(raw_rows):
        session.add(
            TradeRaw(
                account_id=account_id,
                broker=broker,
                source_file=source_file,
                file_signature=file_sig,
                row_index=idx,
                raw_payload=row,
                mapping_name=mapping_name,
            )
        )
        raw_count += 1

    normalized_count = 0
    for row in normalized_rows:
        session.add(TradeNormalized(**row))
        normalized_count += 1
    return raw_count, normalized_count


def insert_cash_activity(session: Session, rows: Iterable[dict]) -> int:
    count = 0
    for row in rows:
        session.add(CashActivity(**row))
        count += 1
    return count


def clear_derived_tables(session: Session) -> None:
    session.execute(delete(PnlRealized))
    session.execute(delete(PositionOpen))


def get_latest_price(session: Session, symbol: str) -> float | None:
    stmt = (
        select(PriceCache.close)
        .where(PriceCache.symbol == symbol)
        .order_by(PriceCache.as_of.desc())
        .limit(1)
    )
    return session.scalar(stmt)


def get_realized_totals(session: Session, account_id: str | None = None) -> dict[str, float]:
    stmt = select(func.coalesce(func.sum(PnlRealized.pnl), 0.0))
    if account_id:
        stmt = stmt.where(PnlRealized.account_id == account_id)
    realized = float(session.scalar(stmt) or 0.0)
    return {"realized_total": realized}


def get_unrealized_totals(session: Session, account_id: str | None = None) -> dict[str, float]:
    stmt = select(func.coalesce(func.sum(PositionOpen.unrealized_pnl), 0.0))
    if account_id:
        stmt = stmt.where(PositionOpen.account_id == account_id)
    unrealized = float(session.scalar(stmt) or 0.0)
    return {"unrealized_total": unrealized}


def get_net_contributions(session: Session, account_id: str | None = None) -> float:
    stmt = select(
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
        stmt = stmt.where(CashActivity.account_id == account_id)
    return float(session.scalar(stmt) or 0.0)
