from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from sqlalchemy import create_engine, event, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from portfolio_assistant.config.paths import ensure_data_dirs
from portfolio_assistant.config.settings import get_settings
from portfolio_assistant.db.models import Base, CashActivity, TradeNormalized, TradeRaw
from portfolio_assistant.ingest.dedupe import cash_dedupe_key, raw_row_hash, trade_dedupe_key


SQLITE_EXTRA_COLUMNS: dict[str, dict[str, str]] = {
    "trades_raw": {
        "row_hash": "VARCHAR(64)",
    },
    "trades_normalized": {
        "dedupe_key": "VARCHAR(96)",
    },
    "cash_activity": {
        "dedupe_key": "VARCHAR(96)",
    },
    "pnl_realized": {
        "disposal_label": "VARCHAR(256)",
        "security_id": "VARCHAR(32)",
        "acquired_date": "DATE",
        "disposal_term": "VARCHAR(16)",
        "close_trade_row_id": "INTEGER",
        "loss_trade_row_id": "INTEGER",
        "lot_method": "VARCHAR(32)",
        "adjustment_codes": "VARCHAR(16)",
        "adjustment_amount": "FLOAT",
        "wash_sale_disallowed": "FLOAT",
        "disposal_metadata": "JSON",
    },
}

SQLITE_EXTRA_INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_accounts_type_broker ON accounts (account_type, broker)",
    "CREATE INDEX IF NOT EXISTS ix_trades_raw_account_signature ON trades_raw (account_id, file_signature)",
    "CREATE INDEX IF NOT EXISTS ix_trades_raw_account_imported_at ON trades_raw (account_id, imported_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_raw_account_row_hash ON trades_raw (account_id, row_hash)",
    "CREATE INDEX IF NOT EXISTS ix_trades_norm_account_exec_id ON trades_normalized (account_id, executed_at, id)",
    "CREATE INDEX IF NOT EXISTS ix_trades_norm_exec_id ON trades_normalized (executed_at, id)",
    "CREATE INDEX IF NOT EXISTS ix_trades_norm_symbol_side_exec ON trades_normalized (symbol, side, executed_at)",
    "CREATE INDEX IF NOT EXISTS ix_trades_norm_underlying_side_exec ON trades_normalized (underlying, side, executed_at)",
    "CREATE INDEX IF NOT EXISTS ix_trades_norm_upper_symbol_exec ON trades_normalized (upper(symbol), executed_at, id)",
    "CREATE INDEX IF NOT EXISTS ix_trades_norm_upper_underlying_exec ON trades_normalized (upper(underlying), executed_at, id)",
    "CREATE INDEX IF NOT EXISTS ix_trades_norm_account_symbol_exec ON trades_normalized (account_id, symbol, executed_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_norm_account_dedupe ON trades_normalized (account_id, dedupe_key)",
    "CREATE INDEX IF NOT EXISTS ix_cash_activity_account_external_posted ON cash_activity (account_id, is_external, posted_at)",
    "CREATE INDEX IF NOT EXISTS ix_cash_activity_account_posted ON cash_activity (account_id, posted_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_cash_activity_account_dedupe ON cash_activity (account_id, dedupe_key)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_account_close ON pnl_realized (account_id, close_date)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_account_close_id ON pnl_realized (account_id, close_date, id)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_symbol_close ON pnl_realized (symbol, close_date)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_disposal_term_close ON pnl_realized (disposal_term, close_date)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_security_close ON pnl_realized (security_id, close_date)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_close_trade_row ON pnl_realized (close_trade_row_id)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_loss_trade_row ON pnl_realized (loss_trade_row_id)",
    "CREATE INDEX IF NOT EXISTS ix_pnl_realized_account_symbol_inst ON pnl_realized (account_id, symbol, instrument_type)",
    "CREATE INDEX IF NOT EXISTS ix_positions_open_account_asof ON positions_open (account_id, as_of)",
    "CREATE INDEX IF NOT EXISTS ix_positions_open_account_asof_id ON positions_open (account_id, as_of, id)",
    "CREATE INDEX IF NOT EXISTS ix_positions_open_account_symbol_inst ON positions_open (account_id, symbol, instrument_type)",
    "CREATE INDEX IF NOT EXISTS ix_reconciliation_runs_tax_year_created ON reconciliation_runs (tax_year, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_reconciliation_runs_account_tax_year ON reconciliation_runs (account_id, tax_year)",
    "CREATE INDEX IF NOT EXISTS ix_reconciliation_runs_status_created ON reconciliation_runs (status, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_reconciliation_artifacts_run_type ON reconciliation_artifacts (reconciliation_run_id, artifact_type)",
    "CREATE INDEX IF NOT EXISTS ix_reconciliation_artifacts_tax_year_type ON reconciliation_artifacts (tax_year, artifact_type)",
    "CREATE INDEX IF NOT EXISTS ix_wash_sale_adjustments_mode_tax_year ON wash_sale_adjustments (mode, tax_year)",
    "CREATE INDEX IF NOT EXISTS ix_wash_sale_adjustments_symbol_sale_date ON wash_sale_adjustments (sale_symbol, sale_date)",
    "CREATE INDEX IF NOT EXISTS ix_wash_sale_adjustments_replacement_account_exec ON wash_sale_adjustments (replacement_account_id, replacement_executed_at)",
    "CREATE INDEX IF NOT EXISTS ix_wash_sale_adjustments_reconciliation_run ON wash_sale_adjustments (reconciliation_run_id)",
    "CREATE INDEX IF NOT EXISTS ix_feed_sources_type_provider ON feed_sources (feed_type, provider)",
    "CREATE INDEX IF NOT EXISTS ix_feed_sources_symbol_type ON feed_sources (symbol, feed_type)",
    "CREATE INDEX IF NOT EXISTS ix_feed_sources_status_next_poll ON feed_sources (status, next_poll_after)",
    "CREATE INDEX IF NOT EXISTS ix_feed_ingest_runs_source_started ON feed_ingest_runs (feed_source_id, started_at)",
    "CREATE INDEX IF NOT EXISTS ix_feed_ingest_runs_status_started ON feed_ingest_runs (status, started_at)",
    "CREATE INDEX IF NOT EXISTS ix_feed_items_source_published ON feed_items (feed_source_id, published_at)",
    "CREATE INDEX IF NOT EXISTS ix_feed_items_symbol_published ON feed_items (symbol, published_at)",
]

SQLITE_REDUNDANT_INDEXES = [
    "DROP INDEX IF EXISTS ix_trades_raw_account_row_hash",
    "DROP INDEX IF EXISTS ix_trades_norm_account_dedupe",
    "DROP INDEX IF EXISTS ix_cash_activity_account_dedupe",
]

BACKFILL_LOOKUP_CHUNK_SIZE = 800


def _enable_sqlite_pragmas(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_connection, _connection_record):  # pragma: no cover - driver callback
        cursor = dbapi_connection.cursor()
        pragmas = (
            "PRAGMA foreign_keys=ON",
            "PRAGMA busy_timeout=5000",
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
        )
        for statement in pragmas:
            try:
                cursor.execute(statement)
            except Exception:
                # Keep startup resilient if a pragma is unsupported by a specific SQLite mode.
                continue
        cursor.close()


def _ensure_sqlite_columns(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as conn:
        for table_name, columns in SQLITE_EXTRA_COLUMNS.items():
            existing = {col["name"] for col in inspect(conn).get_columns(table_name)}
            for column_name, sql_type in columns.items():
                if column_name in existing:
                    continue
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}"  # noqa: S608
                    )
                )


def _ensure_sqlite_indexes(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        for statement in SQLITE_EXTRA_INDEXES:
            conn.execute(text(statement))


def _drop_sqlite_redundant_indexes(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        for statement in SQLITE_REDUNDANT_INDEXES:
            conn.execute(text(statement))


def _backfill_nullable_unique_key(
    session: Session,
    *,
    model,
    key_attr: str,
    compute_key: Callable[[object], str],
    batch_size: int = 2000,
    lookup_chunk_size: int = BACKFILL_LOOKUP_CHUNK_SIZE,
) -> None:
    model_id = getattr(model, "id")
    model_key = getattr(model, key_attr)
    model_account = getattr(model, "account_id")

    while True:
        rows = list(
            session.scalars(
                select(model)
                .where(model_key.is_(None))
                .order_by(model_id.asc())
                .limit(batch_size)
            ).all()
        )
        if not rows:
            break

        keep_rows: list[tuple[object, str, str]] = []
        seen_tokens: set[tuple[str, str]] = set()
        for row in rows:
            key_value = str(compute_key(row))
            account_value = str(getattr(row, "account_id"))
            token = (account_value, key_value)
            if token in seen_tokens:
                session.delete(row)
                continue
            seen_tokens.add(token)
            keep_rows.append((row, account_value, key_value))

        grouped_keys: dict[str, set[str]] = {}
        for _row, account_value, key_value in keep_rows:
            grouped_keys.setdefault(account_value, set()).add(key_value)

        existing_by_token: dict[tuple[str, str], int] = {}
        for account_value, key_values in grouped_keys.items():
            key_list = list(key_values)
            for start in range(0, len(key_list), lookup_chunk_size):
                chunk = key_list[start : start + lookup_chunk_size]
                stmt = (
                    select(model_account, model_key, model_id)
                    .where(
                        model_account == account_value,
                        model_key.in_(chunk),
                        model_key.is_not(None),
                    )
                    .order_by(model_id.asc())
                )
                for account_id, key_val, row_id in session.execute(stmt):
                    token = (str(account_id), str(key_val))
                    row_id_int = int(row_id)
                    existing_id = existing_by_token.get(token)
                    if existing_id is None or row_id_int < existing_id:
                        existing_by_token[token] = row_id_int

        for row, account_value, key_value in keep_rows:
            token = (account_value, key_value)
            existing_id = existing_by_token.get(token)
            row_id = int(getattr(row, "id"))
            if existing_id is None:
                setattr(row, key_attr, key_value)
                continue
            if existing_id < row_id:
                session.delete(row)
                continue
            if existing_id > row_id:
                existing_row = session.get(model, existing_id)
                if existing_row is not None:
                    session.delete(existing_row)
                setattr(row, key_attr, key_value)
                continue
            setattr(row, key_attr, key_value)

        session.commit()


def _trade_raw_row_hash(row: TradeRaw) -> str:
    payload = row.raw_payload if isinstance(row.raw_payload, dict) else {"raw": row.raw_payload}
    return raw_row_hash(payload)


def _trade_normalized_dedupe_key(row: TradeNormalized) -> str:
    return trade_dedupe_key(
        {
            "account_id": row.account_id,
            "broker": row.broker,
            "trade_id": row.trade_id,
            "executed_at": row.executed_at,
            "instrument_type": row.instrument_type,
            "symbol": row.symbol,
            "side": row.side,
            "option_symbol_raw": row.option_symbol_raw,
            "underlying": row.underlying,
            "expiration": row.expiration,
            "strike": row.strike,
            "multiplier": row.multiplier,
            "quantity": row.quantity,
            "price": row.price,
            "fees": row.fees,
            "net_amount": row.net_amount,
            "currency": row.currency,
        }
    )


def _cash_activity_dedupe_key(row: CashActivity) -> str:
    return cash_dedupe_key(
        {
            "account_id": row.account_id,
            "broker": row.broker,
            "posted_at": row.posted_at,
            "activity_type": row.activity_type,
            "amount": row.amount,
            "description": row.description,
            "source": row.source,
            "transfer_group_id": row.transfer_group_id,
        }
    )


def _backfill_trade_raw_hashes(engine: Engine, batch_size: int = 2000) -> None:
    with Session(engine) as session:
        _backfill_nullable_unique_key(
            session,
            model=TradeRaw,
            key_attr="row_hash",
            compute_key=_trade_raw_row_hash,
            batch_size=batch_size,
        )


def _backfill_trade_dedupe_keys(engine: Engine, batch_size: int = 2000) -> None:
    with Session(engine) as session:
        _backfill_nullable_unique_key(
            session,
            model=TradeNormalized,
            key_attr="dedupe_key",
            compute_key=_trade_normalized_dedupe_key,
            batch_size=batch_size,
        )


def _backfill_cash_dedupe_keys(engine: Engine, batch_size: int = 2000) -> None:
    with Session(engine) as session:
        _backfill_nullable_unique_key(
            session,
            model=CashActivity,
            key_attr="dedupe_key",
            compute_key=_cash_activity_dedupe_key,
            batch_size=batch_size,
        )


def _backfill_sqlite_dedupe_columns(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    _backfill_trade_raw_hashes(engine)
    _backfill_trade_dedupe_keys(engine)
    _backfill_cash_dedupe_keys(engine)


def _dedupe_sqlite_rows_for_unique_keys(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return

    statements = [
        """
        DELETE FROM trades_raw
        WHERE row_hash IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM trades_raw older
            WHERE older.account_id = trades_raw.account_id
              AND older.row_hash = trades_raw.row_hash
              AND older.id < trades_raw.id
          )
        """,
        """
        DELETE FROM trades_normalized
        WHERE dedupe_key IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM trades_normalized older
            WHERE older.account_id = trades_normalized.account_id
              AND older.dedupe_key = trades_normalized.dedupe_key
              AND older.id < trades_normalized.id
          )
        """,
        """
        DELETE FROM cash_activity
        WHERE dedupe_key IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM cash_activity older
            WHERE older.account_id = cash_activity.account_id
              AND older.dedupe_key = cash_activity.dedupe_key
              AND older.id < cash_activity.id
          )
        """,
    ]

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def build_engine(database_url: str | None = None) -> Engine:
    settings = get_settings()
    url = database_url or settings.database_url

    if url.startswith("sqlite:///"):
        sqlite_path = Path(url.removeprefix("sqlite:///")).expanduser()
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(url, future=True)
    if engine.dialect.name == "sqlite":
        _enable_sqlite_pragmas(engine)
    return engine


def migrate(database_url: str | None = None) -> Engine:
    ensure_data_dirs()
    engine = build_engine(database_url=database_url)
    Base.metadata.create_all(bind=engine)
    _ensure_sqlite_columns(engine)
    _backfill_sqlite_dedupe_columns(engine)
    _dedupe_sqlite_rows_for_unique_keys(engine)
    _drop_sqlite_redundant_indexes(engine)
    _ensure_sqlite_indexes(engine)
    return engine


if __name__ == "__main__":
    migrate()
