from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager

from sqlalchemy import case, delete, func, insert, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from portfolio_assistant.db.migrate import build_engine
from portfolio_assistant.db.models import (
    Account,
    CashActivity,
    PnlRealized,
    PositionOpen,
    PriceCache,
    ReconciliationRun,
    TradeNormalized,
    TradeRaw,
    WashSaleAdjustment,
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


def delete_account_if_empty(session: Session, account_id: str) -> tuple[bool, str]:
    account = session.get(Account, account_id)
    if account is None:
        return False, "Account not found."

    checks = {
        "trade imports": select(func.count()).select_from(TradeRaw).where(TradeRaw.account_id == account_id),
        "normalized trades": select(func.count())
        .select_from(TradeNormalized)
        .where(TradeNormalized.account_id == account_id),
        "cash rows": select(func.count())
        .select_from(CashActivity)
        .where(CashActivity.account_id == account_id),
        "realized rows": select(func.count())
        .select_from(PnlRealized)
        .where(PnlRealized.account_id == account_id),
        "open positions": select(func.count())
        .select_from(PositionOpen)
        .where(PositionOpen.account_id == account_id),
        "reconciliation runs": select(func.count())
        .select_from(ReconciliationRun)
        .where(ReconciliationRun.account_id == account_id),
        "wash-sale matches": select(func.count())
        .select_from(WashSaleAdjustment)
        .where(WashSaleAdjustment.replacement_account_id == account_id),
    }
    usage = {label: int(session.scalar(stmt) or 0) for label, stmt in checks.items()}
    blocking = {label: count for label, count in usage.items() if count > 0}
    if blocking:
        summary = ", ".join(f"{label}={count}" for label, count in blocking.items())
        return (
            False,
            "Cannot remove account because data exists. "
            f"Delete dependent rows first ({summary}).",
        )

    session.delete(account)
    session.flush()
    return True, f"Removed account '{account.broker} | {account.account_label}'."


def _chunked(rows: list[dict], batch_size: int) -> Iterable[list[dict]]:
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


def _dedupe_batch_rows_by_key(
    rows: list[dict],
    key_field: str,
    *,
    account_field: str = "account_id",
) -> list[dict]:
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for row in rows:
        key_val = row.get(key_field)
        account_val = row.get(account_field)
        if key_val is None or account_val is None:
            out.append(row)
            continue
        token = (str(account_val), str(key_val))
        if token in seen:
            continue
        seen.add(token)
        out.append(row)
    return out


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


def _bulk_insert_ignore_conflicts(
    session: Session,
    model,
    rows: list[dict],
    *,
    conflict_fields: tuple[str, ...],
    key_field: str,
    batch_size: int = 5000,
) -> int:
    if not rows:
        return 0

    bind = session.get_bind()
    dialect_name = bind.dialect.name if bind is not None else ""
    inserted = 0
    if dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(model).on_conflict_do_nothing(
            index_elements=list(conflict_fields)
        )
        for chunk in _chunked(rows, batch_size=batch_size):
            before = int(session.scalar(text("SELECT total_changes()")) or 0)
            session.execute(stmt, chunk)
            after = int(session.scalar(text("SELECT total_changes()")) or 0)
            inserted += max(after - before, 0)
        return inserted

    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as postgresql_insert

        for chunk in _chunked(rows, batch_size=batch_size):
            stmt = postgresql_insert(model).values(chunk).on_conflict_do_nothing(
                index_elements=list(conflict_fields)
            )
            result = session.execute(stmt)
            inserted += max(int(result.rowcount or 0), 0)
        return inserted

    filtered_rows = _filter_new_rows_by_key(session, model, rows, key_field=key_field)
    return _bulk_insert(session, model, filtered_rows, batch_size=batch_size)


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
    prepared_raw_rows: list[dict] = []
    for idx, payload in enumerate(raw_rows):
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
    for row in normalized_rows:
        normalized = dict(row)
        normalized["dedupe_key"] = normalized.get("dedupe_key") or trade_dedupe_key(normalized)
        prepared_normalized_rows.append(normalized)

    prepared_raw_rows = _dedupe_batch_rows_by_key(
        prepared_raw_rows,
        key_field="row_hash",
    )
    prepared_normalized_rows = _dedupe_batch_rows_by_key(
        prepared_normalized_rows,
        key_field="dedupe_key",
    )

    raw_count = _bulk_insert_ignore_conflicts(
        session,
        TradeRaw,
        prepared_raw_rows,
        conflict_fields=("account_id", "row_hash"),
        key_field="row_hash",
    )
    normalized_count = _bulk_insert_ignore_conflicts(
        session,
        TradeNormalized,
        prepared_normalized_rows,
        conflict_fields=("account_id", "dedupe_key"),
        key_field="dedupe_key",
    )
    return raw_count, normalized_count


def insert_cash_activity(session: Session, rows: Iterable[dict]) -> int:
    prepared_rows: list[dict] = []
    for row in rows:
        payload = dict(row)
        payload["dedupe_key"] = payload.get("dedupe_key") or cash_dedupe_key(payload)
        prepared_rows.append(payload)

    prepared_rows = _dedupe_batch_rows_by_key(
        prepared_rows,
        key_field="dedupe_key",
    )
    return _bulk_insert_ignore_conflicts(
        session,
        CashActivity,
        prepared_rows,
        conflict_fields=("account_id", "dedupe_key"),
        key_field="dedupe_key",
    )


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
