from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager
from time import perf_counter

from sqlalchemy import case, delete, func, insert, or_, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from portfolio_assistant.db.migrate import build_engine
from portfolio_assistant.db.models import (
    Account,
    CashActivity,
    PnlRealized,
    PositionOpen,
    PriceCache,
    ReconciliationArtifact,
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


def _account_dependency_rules(account_id: str) -> list[tuple[str, object]]:
    trade_ids = select(TradeNormalized.id).where(TradeNormalized.account_id == account_id)
    pnl_ids = select(PnlRealized.id).where(PnlRealized.account_id == account_id)
    reconciliation_run_ids = select(ReconciliationRun.id).where(
        ReconciliationRun.account_id == account_id
    )

    wash_sale_filter = or_(
        WashSaleAdjustment.replacement_account_id == account_id,
        WashSaleAdjustment.loss_trade_row_id.in_(trade_ids),
        WashSaleAdjustment.replacement_trade_row_id.in_(trade_ids),
        WashSaleAdjustment.loss_sale_row_id.in_(pnl_ids),
        WashSaleAdjustment.reconciliation_run_id.in_(reconciliation_run_ids),
    )

    return [
        ("trade imports", TradeRaw.account_id == account_id),
        ("normalized trades", TradeNormalized.account_id == account_id),
        ("cash rows", CashActivity.account_id == account_id),
        ("realized rows", PnlRealized.account_id == account_id),
        ("open positions", PositionOpen.account_id == account_id),
        ("reconciliation artifacts", ReconciliationArtifact.reconciliation_run_id.in_(reconciliation_run_ids)),
        ("reconciliation runs", ReconciliationRun.account_id == account_id),
        ("wash-sale matches", wash_sale_filter),
    ]


def _account_dependency_counts(session: Session, account_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for label, predicate in _account_dependency_rules(account_id):
        model = {
            "trade imports": TradeRaw,
            "normalized trades": TradeNormalized,
            "cash rows": CashActivity,
            "realized rows": PnlRealized,
            "open positions": PositionOpen,
            "reconciliation artifacts": ReconciliationArtifact,
            "reconciliation runs": ReconciliationRun,
            "wash-sale matches": WashSaleAdjustment,
        }[label]
        stmt = select(func.count()).select_from(model).where(predicate)
        counts[label] = int(session.scalar(stmt) or 0)
    return counts


def _format_dependency_counts(counts: dict[str, int]) -> str:
    ordered_labels = [
        "trade imports",
        "normalized trades",
        "cash rows",
        "realized rows",
        "open positions",
        "reconciliation artifacts",
        "reconciliation runs",
        "wash-sale matches",
    ]
    return ", ".join(f"{label}={counts.get(label, 0)}" for label in ordered_labels)


def _delete_account_dependencies(session: Session, account_id: str) -> None:
    trade_ids = select(TradeNormalized.id).where(TradeNormalized.account_id == account_id)
    pnl_ids = select(PnlRealized.id).where(PnlRealized.account_id == account_id)
    reconciliation_run_ids = select(ReconciliationRun.id).where(
        ReconciliationRun.account_id == account_id
    )

    session.execute(
        delete(WashSaleAdjustment).where(
            or_(
                WashSaleAdjustment.replacement_account_id == account_id,
                WashSaleAdjustment.loss_trade_row_id.in_(trade_ids),
                WashSaleAdjustment.replacement_trade_row_id.in_(trade_ids),
                WashSaleAdjustment.loss_sale_row_id.in_(pnl_ids),
                WashSaleAdjustment.reconciliation_run_id.in_(reconciliation_run_ids),
            )
        )
    )
    session.execute(
        delete(ReconciliationArtifact).where(
            ReconciliationArtifact.reconciliation_run_id.in_(reconciliation_run_ids)
        )
    )
    session.execute(delete(ReconciliationRun).where(ReconciliationRun.account_id == account_id))
    session.execute(delete(PositionOpen).where(PositionOpen.account_id == account_id))
    session.execute(delete(PnlRealized).where(PnlRealized.account_id == account_id))
    session.execute(delete(CashActivity).where(CashActivity.account_id == account_id))
    session.execute(delete(TradeNormalized).where(TradeNormalized.account_id == account_id))
    session.execute(delete(TradeRaw).where(TradeRaw.account_id == account_id))


def delete_account_if_empty(
    session: Session,
    account_id: str,
    *,
    force: bool = False,
) -> tuple[bool, str]:
    account = session.get(Account, account_id)
    if account is None:
        return False, "Account not found."

    usage_before = _account_dependency_counts(session, account_id)
    blocking = {label: count for label, count in usage_before.items() if count > 0}
    usage_summary = _format_dependency_counts(usage_before)
    if blocking:
        if force:
            _delete_account_dependencies(session, account_id)
            session.flush()
        else:
            return (
                False,
                "Cannot remove account because data exists. "
                "Delete dependent rows first or retry with force=True "
                f"({usage_summary}).",
            )

    if force and blocking:
        # Recompute after forced cleanup and fail-safe if anything still remains.
        usage_after = _account_dependency_counts(session, account_id)
        remaining = {label: count for label, count in usage_after.items() if count > 0}
        if remaining:
            summary = ", ".join(f"{label}={count}" for label, count in remaining.items())
            return (
                False,
                "Cannot force-remove account because dependent rows remain "
                f"after cleanup ({summary}).",
            )

    session.delete(account)
    session.flush()
    if force:
        return True, (
            f"Force removed account '{account.broker} | {account.account_label}'. "
            f"Deleted dependencies: {usage_summary}."
        )
    return True, (
        f"Removed account '{account.broker} | {account.account_label}'. "
        f"Dependencies at delete time: {usage_summary}."
    )


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
    *,
    perf_stats: dict[str, float | int] | None = None,
) -> tuple[int, int]:
    total_started = perf_counter() if perf_stats is not None else 0.0
    prepare_started = perf_counter() if perf_stats is not None else 0.0

    prepared_raw_rows: list[dict] = []
    input_raw_rows = 0
    for idx, payload in enumerate(raw_rows):
        input_raw_rows += 1
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
    input_normalized_rows = 0
    for row in normalized_rows:
        input_normalized_rows += 1
        normalized = dict(row)
        normalized["dedupe_key"] = normalized.get("dedupe_key") or trade_dedupe_key(normalized)
        prepared_normalized_rows.append(normalized)

    prepare_seconds = (perf_counter() - prepare_started) if perf_stats is not None else 0.0
    dedupe_started = perf_counter() if perf_stats is not None else 0.0

    prepared_raw_rows = _dedupe_batch_rows_by_key(
        prepared_raw_rows,
        key_field="row_hash",
    )
    prepared_normalized_rows = _dedupe_batch_rows_by_key(
        prepared_normalized_rows,
        key_field="dedupe_key",
    )

    dedupe_seconds = (perf_counter() - dedupe_started) if perf_stats is not None else 0.0
    insert_started = perf_counter() if perf_stats is not None else 0.0

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

    if perf_stats is not None:
        insert_seconds = perf_counter() - insert_started
        total_seconds = perf_counter() - total_started
        perf_stats.update(
            {
                "input_raw_rows": input_raw_rows,
                "input_normalized_rows": input_normalized_rows,
                "prepared_raw_rows": len(prepared_raw_rows),
                "prepared_normalized_rows": len(prepared_normalized_rows),
                "inserted_raw_rows": raw_count,
                "inserted_normalized_rows": normalized_count,
                "prepare_seconds": round(prepare_seconds, 6),
                "dedupe_seconds": round(dedupe_seconds, 6),
                "insert_seconds": round(insert_seconds, 6),
                "total_seconds": round(total_seconds, 6),
            }
        )
    return raw_count, normalized_count


def insert_cash_activity(
    session: Session,
    rows: Iterable[dict],
    *,
    perf_stats: dict[str, float | int] | None = None,
) -> int:
    total_started = perf_counter() if perf_stats is not None else 0.0
    prepare_started = perf_counter() if perf_stats is not None else 0.0

    prepared_rows: list[dict] = []
    input_rows = 0
    for row in rows:
        input_rows += 1
        payload = dict(row)
        payload["dedupe_key"] = payload.get("dedupe_key") or cash_dedupe_key(payload)
        prepared_rows.append(payload)

    prepare_seconds = (perf_counter() - prepare_started) if perf_stats is not None else 0.0
    dedupe_started = perf_counter() if perf_stats is not None else 0.0

    prepared_rows = _dedupe_batch_rows_by_key(
        prepared_rows,
        key_field="dedupe_key",
    )

    dedupe_seconds = (perf_counter() - dedupe_started) if perf_stats is not None else 0.0
    insert_started = perf_counter() if perf_stats is not None else 0.0

    inserted = _bulk_insert_ignore_conflicts(
        session,
        CashActivity,
        prepared_rows,
        conflict_fields=("account_id", "dedupe_key"),
        key_field="dedupe_key",
    )

    if perf_stats is not None:
        insert_seconds = perf_counter() - insert_started
        total_seconds = perf_counter() - total_started
        perf_stats.update(
            {
                "input_rows": input_rows,
                "prepared_rows": len(prepared_rows),
                "inserted_rows": inserted,
                "prepare_seconds": round(prepare_seconds, 6),
                "dedupe_seconds": round(dedupe_seconds, 6),
                "insert_seconds": round(insert_seconds, 6),
                "total_seconds": round(total_seconds, 6),
            }
        )
    return inserted


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
