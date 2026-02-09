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
