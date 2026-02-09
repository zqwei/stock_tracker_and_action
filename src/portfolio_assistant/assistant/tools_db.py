from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager

from sqlalchemy import case, delete, func, insert, select
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
from portfolio_assistant.ingest.dedupe import cash_dedupe_key, raw_row_hash, trade_dedupe_key


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


def _chunked(rows: list[dict], batch_size: int) -> Iterable[list[dict]]:
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


def _filter_new_rows_by_key(
    session: Session,
    model,
    rows: list[dict],
    key_field: str,
    *,
    account_field: str = "account_id",
    query_chunk_size: int = 1000,
) -> list[dict]:
    if not rows:
        return []

    seen_in_batch: set[tuple[str, str]] = set()
    deduped_batch: list[dict] = []
    grouped_keys: dict[str, set[str]] = {}

    for row in rows:
        key_val = row.get(key_field)
        account_val = row.get(account_field)
        if key_val is None or account_val is None:
            deduped_batch.append(row)
            continue

        key_text = str(key_val)
        account_text = str(account_val)
        token = (account_text, key_text)
        if token in seen_in_batch:
            continue
        seen_in_batch.add(token)
        grouped_keys.setdefault(account_text, set()).add(key_text)
        deduped_batch.append(row)

    if not grouped_keys:
        return deduped_batch

    existing: dict[str, set[str]] = {}
    model_key = getattr(model, key_field)
    model_account = getattr(model, account_field)

    for account_id, keys in grouped_keys.items():
        existing_keys: set[str] = set()
        key_list = list(keys)
        for start in range(0, len(key_list), query_chunk_size):
            chunk = key_list[start : start + query_chunk_size]
            stmt = select(model_key).where(model_account == account_id, model_key.in_(chunk))
            existing_keys.update(str(value) for value in session.scalars(stmt).all() if value)
        existing[account_id] = existing_keys

    filtered: list[dict] = []
    for row in deduped_batch:
        key_val = row.get(key_field)
        account_val = row.get(account_field)
        if key_val is None or account_val is None:
            filtered.append(row)
            continue
        if str(key_val) in existing.get(str(account_val), set()):
            continue
        filtered.append(row)

    return filtered


def _bulk_insert(session: Session, model, rows: list[dict], batch_size: int = 2000) -> int:
    if not rows:
        return 0
    inserted = 0
    for chunk in _chunked(rows, batch_size=batch_size):
        session.execute(insert(model), chunk)
        inserted += len(chunk)
    return inserted


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
    raw_payloads = list(raw_rows)
    normalized_payloads = [dict(row) for row in normalized_rows]

    prepared_raw_rows: list[dict] = []
    for idx, payload in enumerate(raw_payloads):
        payload_dict = payload if isinstance(payload, dict) else {"raw": payload}
        prepared_raw_rows.append(
            {
                "account_id": account_id,
                "broker": broker,
                "source_file": source_file,
                "file_signature": file_sig,
                "row_index": idx,
                "row_hash": raw_row_hash(payload_dict),
                "raw_payload": payload_dict,
                "mapping_name": mapping_name,
            }
        )

    prepared_normalized_rows: list[dict] = []
    for row in normalized_payloads:
        normalized = dict(row)
        normalized["dedupe_key"] = normalized.get("dedupe_key") or trade_dedupe_key(normalized)
        prepared_normalized_rows.append(normalized)

    prepared_raw_rows = _filter_new_rows_by_key(
        session, TradeRaw, prepared_raw_rows, key_field="row_hash"
    )
    prepared_normalized_rows = _filter_new_rows_by_key(
        session, TradeNormalized, prepared_normalized_rows, key_field="dedupe_key"
    )

    raw_count = _bulk_insert(session, TradeRaw, prepared_raw_rows)
    normalized_count = _bulk_insert(session, TradeNormalized, prepared_normalized_rows)
    return raw_count, normalized_count


def insert_cash_activity(session: Session, rows: Iterable[dict]) -> int:
    prepared_rows: list[dict] = []
    for row in rows:
        payload = dict(row)
        payload["dedupe_key"] = payload.get("dedupe_key") or cash_dedupe_key(payload)
        prepared_rows.append(payload)

    prepared_rows = _filter_new_rows_by_key(
        session, CashActivity, prepared_rows, key_field="dedupe_key"
    )
    return _bulk_insert(session, CashActivity, prepared_rows)


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
