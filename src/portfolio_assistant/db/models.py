from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Enum as SqlEnum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class AccountType(str, Enum):
    TAXABLE = "TAXABLE"
    TRAD_IRA = "TRAD_IRA"
    ROTH_IRA = "ROTH_IRA"


class InstrumentType(str, Enum):
    STOCK = "STOCK"
    OPTION = "OPTION"


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    BTO = "BTO"
    STO = "STO"
    BTC = "BTC"
    STC = "STC"


class OptionType(str, Enum):
    CALL = "C"
    PUT = "P"


class CashActivityType(str, Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"


class DisposalTerm(str, Enum):
    SHORT = "SHORT"
    LONG = "LONG"
    UNKNOWN = "UNKNOWN"


class WashSaleComputationMode(str, Enum):
    BROKER = "BROKER"
    IRS = "IRS"


class WashSaleAdjustmentStatus(str, Enum):
    PROPOSED = "PROPOSED"
    APPLIED = "APPLIED"
    REVERSED = "REVERSED"


class ReconciliationRunStatus(str, Enum):
    DRAFT = "DRAFT"
    COMPLETE = "COMPLETE"
    SUPERSEDED = "SUPERSEDED"


class ReconciliationArtifactType(str, Enum):
    BROKER_INPUT = "BROKER_INPUT"
    APP_8949 = "APP_8949"
    APP_SUMMARY = "APP_SUMMARY"
    DIFF_SYMBOL = "DIFF_SYMBOL"
    DIFF_SALE_DATE = "DIFF_SALE_DATE"
    DIFF_TERM = "DIFF_TERM"
    CHECKLIST = "CHECKLIST"
    PACKET = "PACKET"


class FeedType(str, Enum):
    PRICE = "PRICE"
    EARNINGS = "EARNINGS"
    MACRO = "MACRO"
    NEWS = "NEWS"


class FeedSyncStatus(str, Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class FeedRunStatus(str, Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (
        UniqueConstraint("broker", "account_label", name="uq_accounts_broker_label"),
        Index("ix_accounts_type_broker", "account_type", "broker"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid4())
    )
    broker: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    account_label: Mapped[str] = mapped_column(String(128), nullable=False)
    account_type: Mapped[AccountType] = mapped_column(
        SqlEnum(AccountType, native_enum=False), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class TradeRaw(Base):
    __tablename__ = "trades_raw"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "source_file", "row_index", name="uq_trades_raw_row"
        ),
        UniqueConstraint("account_id", "row_hash", name="uq_trades_raw_account_row_hash"),
        Index("ix_trades_raw_account_signature", "account_id", "file_signature"),
        Index("ix_trades_raw_account_imported_at", "account_id", "imported_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.id"), nullable=False, index=True
    )
    broker: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_file: Mapped[str] = mapped_column(String(256), nullable=False)
    file_signature: Mapped[str | None] = mapped_column(String(128), nullable=True)
    row_index: Mapped[int] = mapped_column(Integer, nullable=False)
    row_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    mapping_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class TradeNormalized(Base):
    __tablename__ = "trades_normalized"
    __table_args__ = (
        Index("ix_trades_norm_account_exec_id", "account_id", "executed_at", "id"),
        Index("ix_trades_norm_exec_id", "executed_at", "id"),
        Index("ix_trades_norm_symbol_side_exec", "symbol", "side", "executed_at"),
        Index(
            "ix_trades_norm_underlying_side_exec", "underlying", "side", "executed_at"
        ),
        Index("ix_trades_norm_account_symbol_exec", "account_id", "symbol", "executed_at"),
        UniqueConstraint("account_id", "dedupe_key", name="uq_trades_norm_account_dedupe"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.id"), nullable=False, index=True
    )
    broker: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    trade_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    executed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    instrument_type: Mapped[InstrumentType] = mapped_column(
        SqlEnum(InstrumentType, native_enum=False), nullable=False, index=True
    )
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    side: Mapped[TradeSide] = mapped_column(
        SqlEnum(TradeSide, native_enum=False), nullable=False, index=True
    )
    option_symbol_raw: Mapped[str | None] = mapped_column(String(128), nullable=True)
    underlying: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    expiration: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    strike: Mapped[float | None] = mapped_column(Float, nullable=True)
    call_put: Mapped[OptionType | None] = mapped_column(
        SqlEnum(OptionType, native_enum=False), nullable=True
    )
    multiplier: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    fees: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    net_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="USD")
    dedupe_key: Mapped[str | None] = mapped_column(String(96), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class CashActivity(Base):
    __tablename__ = "cash_activity"
    __table_args__ = (
        Index(
            "ix_cash_activity_account_external_posted",
            "account_id",
            "is_external",
            "posted_at",
        ),
        Index("ix_cash_activity_account_posted", "account_id", "posted_at"),
        UniqueConstraint("account_id", "dedupe_key", name="uq_cash_activity_account_dedupe"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.id"), nullable=False, index=True
    )
    broker: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    posted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    activity_type: Mapped[CashActivityType] = mapped_column(
        "type", SqlEnum(CashActivityType, native_enum=False), nullable=False, index=True
    )
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_external: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    transfer_group_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    dedupe_key: Mapped[str | None] = mapped_column(String(96), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class PriceCache(Base):
    __tablename__ = "price_cache"
    __table_args__ = (
        UniqueConstraint("symbol", "as_of", "interval", name="uq_price_cache_symbol_asof"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    as_of: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    interval: Mapped[str] = mapped_column(String(16), nullable=False, default="1d")
    close: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    cached_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class PnlRealized(Base):
    __tablename__ = "pnl_realized"
    __table_args__ = (
        Index("ix_pnl_realized_account_close", "account_id", "close_date"),
        Index("ix_pnl_realized_account_close_id", "account_id", "close_date", "id"),
        Index("ix_pnl_realized_symbol_close", "symbol", "close_date"),
        Index("ix_pnl_realized_disposal_term_close", "disposal_term", "close_date"),
        Index("ix_pnl_realized_security_close", "security_id", "close_date"),
        Index("ix_pnl_realized_close_trade_row", "close_trade_row_id"),
        Index("ix_pnl_realized_loss_trade_row", "loss_trade_row_id"),
        Index(
            "ix_pnl_realized_account_symbol_inst",
            "account_id",
            "symbol",
            "instrument_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.id"), nullable=False, index=True
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    instrument_type: Mapped[InstrumentType] = mapped_column(
        SqlEnum(InstrumentType, native_enum=False), nullable=False, index=True
    )
    close_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    proceeds: Mapped[float] = mapped_column(Float, nullable=False)
    cost_basis: Mapped[float] = mapped_column(Float, nullable=False)
    fees: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    pnl: Mapped[float] = mapped_column(Float, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    disposal_label: Mapped[str | None] = mapped_column(String(256), nullable=True)
    security_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    acquired_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    disposal_term: Mapped[DisposalTerm | None] = mapped_column(
        SqlEnum(DisposalTerm, native_enum=False), nullable=True
    )
    close_trade_row_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    loss_trade_row_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lot_method: Mapped[str | None] = mapped_column(String(32), nullable=True)
    adjustment_codes: Mapped[str | None] = mapped_column(String(16), nullable=True)
    adjustment_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    wash_sale_disallowed: Mapped[float | None] = mapped_column(Float, nullable=True)
    disposal_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class ReconciliationRun(Base):
    __tablename__ = "reconciliation_runs"
    __table_args__ = (
        Index("ix_reconciliation_runs_tax_year_created", "tax_year", "created_at"),
        Index("ix_reconciliation_runs_account_tax_year", "account_id", "tax_year"),
        Index("ix_reconciliation_runs_status_created", "status", "created_at"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid4())
    )
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    account_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("accounts.id"), nullable=True, index=True
    )
    broker: Mapped[str | None] = mapped_column(String(64), nullable=True)
    scope_label: Mapped[str | None] = mapped_column(String(64), nullable=True)
    broker_input_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[ReconciliationRunStatus] = mapped_column(
        SqlEnum(ReconciliationRunStatus, native_enum=False),
        nullable=False,
        default=ReconciliationRunStatus.DRAFT,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class ReconciliationArtifact(Base):
    __tablename__ = "reconciliation_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "reconciliation_run_id",
            "artifact_type",
            "artifact_name",
            name="uq_reconciliation_artifacts_run_type_name",
        ),
        Index(
            "ix_reconciliation_artifacts_run_type",
            "reconciliation_run_id",
            "artifact_type",
        ),
        Index("ix_reconciliation_artifacts_tax_year_type", "tax_year", "artifact_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reconciliation_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("reconciliation_runs.id"), nullable=False, index=True
    )
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    artifact_type: Mapped[ReconciliationArtifactType] = mapped_column(
        SqlEnum(ReconciliationArtifactType, native_enum=False), nullable=False, index=True
    )
    artifact_name: Mapped[str] = mapped_column(String(128), nullable=False)
    storage_format: Mapped[str] = mapped_column(String(24), nullable=False, default="json")
    file_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class WashSaleAdjustment(Base):
    __tablename__ = "wash_sale_adjustments"
    __table_args__ = (
        UniqueConstraint(
            "mode",
            "loss_sale_row_id",
            "replacement_trade_row_id",
            "adjustment_sequence",
            name="uq_wash_sale_adjustments_mode_sale_replacement",
        ),
        Index("ix_wash_sale_adjustments_mode_tax_year", "mode", "tax_year"),
        Index("ix_wash_sale_adjustments_symbol_sale_date", "sale_symbol", "sale_date"),
        Index(
            "ix_wash_sale_adjustments_replacement_account_exec",
            "replacement_account_id",
            "replacement_executed_at",
        ),
        Index("ix_wash_sale_adjustments_reconciliation_run", "reconciliation_run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mode: Mapped[WashSaleComputationMode] = mapped_column(
        SqlEnum(WashSaleComputationMode, native_enum=False), nullable=False, index=True
    )
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    reconciliation_run_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("reconciliation_runs.id"), nullable=True, index=True
    )
    loss_sale_row_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pnl_realized.id"), nullable=False, index=True
    )
    loss_trade_row_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("trades_normalized.id"), nullable=True, index=True
    )
    replacement_trade_row_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("trades_normalized.id"), nullable=True, index=True
    )
    replacement_account_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("accounts.id"), nullable=True, index=True
    )
    sale_symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    sale_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    replacement_executed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    window_offset_days: Mapped[int] = mapped_column(Integer, nullable=False)
    replacement_quantity_equiv: Mapped[float] = mapped_column(Float, nullable=False)
    disallowed_loss: Mapped[float] = mapped_column(Float, nullable=False)
    basis_adjustment: Mapped[float | None] = mapped_column(Float, nullable=True)
    permanently_disallowed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    adjustment_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[WashSaleAdjustmentStatus] = mapped_column(
        SqlEnum(WashSaleAdjustmentStatus, native_enum=False),
        nullable=False,
        default=WashSaleAdjustmentStatus.PROPOSED,
    )
    rationale: Mapped[str | None] = mapped_column(Text, nullable=True)
    adjustment_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class FeedSource(Base):
    __tablename__ = "feed_sources"
    __table_args__ = (
        UniqueConstraint("feed_type", "provider", "scope_key", name="uq_feed_sources_scope"),
        Index("ix_feed_sources_type_provider", "feed_type", "provider"),
        Index("ix_feed_sources_symbol_type", "symbol", "feed_type"),
        Index("ix_feed_sources_status_next_poll", "status", "next_poll_after"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feed_type: Mapped[FeedType] = mapped_column(
        SqlEnum(FeedType, native_enum=False), nullable=False, index=True
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    scope_key: Mapped[str] = mapped_column(String(256), nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    interval: Mapped[str | None] = mapped_column(String(16), nullable=True)
    request_params: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_modified: Mapped[str | None] = mapped_column(String(128), nullable=True)
    cursor: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[FeedSyncStatus] = mapped_column(
        SqlEnum(FeedSyncStatus, native_enum=False),
        nullable=False,
        default=FeedSyncStatus.ACTIVE,
        index=True,
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_poll_after: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class FeedIngestRun(Base):
    __tablename__ = "feed_ingest_runs"
    __table_args__ = (
        Index("ix_feed_ingest_runs_source_started", "feed_source_id", "started_at"),
        Index("ix_feed_ingest_runs_status_started", "status", "started_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feed_source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("feed_sources.id"), nullable=False, index=True
    )
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=_utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[FeedRunStatus] = mapped_column(
        SqlEnum(FeedRunStatus, native_enum=False),
        nullable=False,
        default=FeedRunStatus.PENDING,
        index=True,
    )
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fetched_items: Mapped[int | None] = mapped_column(Integer, nullable=True)
    upserted_items: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deduped_items: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rate_limit_remaining: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    response_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class FeedItem(Base):
    __tablename__ = "feed_items"
    __table_args__ = (
        UniqueConstraint("feed_type", "provider", "external_id", name="uq_feed_items_external"),
        Index("ix_feed_items_source_published", "feed_source_id", "published_at"),
        Index("ix_feed_items_symbol_published", "symbol", "published_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feed_source_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("feed_sources.id"), nullable=True, index=True
    )
    feed_type: Mapped[FeedType] = mapped_column(
        SqlEnum(FeedType, native_enum=False), nullable=False, index=True
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    event_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class PositionOpen(Base):
    __tablename__ = "positions_open"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "instrument_type",
            "symbol",
            "option_symbol_raw",
            name="uq_positions_open_key",
        ),
        Index("ix_positions_open_account_asof", "account_id", "as_of"),
        Index("ix_positions_open_account_asof_id", "account_id", "as_of", "id"),
        Index(
            "ix_positions_open_account_symbol_inst",
            "account_id",
            "symbol",
            "instrument_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.id"), nullable=False, index=True
    )
    instrument_type: Mapped[InstrumentType] = mapped_column(
        SqlEnum(InstrumentType, native_enum=False), nullable=False, index=True
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    option_symbol_raw: Mapped[str | None] = mapped_column(String(128), nullable=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    avg_cost: Mapped[float] = mapped_column(Float, nullable=False)
    last_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    as_of: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=_utcnow)
