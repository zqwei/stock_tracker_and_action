"""Core domain models used by ingest, analytics, and UI layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class AccountType(StrEnum):
    TAXABLE = "TAXABLE"
    TRAD_IRA = "TRAD_IRA"
    ROTH_IRA = "ROTH_IRA"


class InstrumentType(StrEnum):
    STOCK = "STOCK"
    OPTION = "OPTION"


class TradeSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    BTO = "BTO"
    STO = "STO"
    BTC = "BTC"
    STC = "STC"


@dataclass(slots=True)
class Account:
    account_id: str
    account_label: str
    broker: str
    account_type: AccountType


@dataclass(slots=True)
class Trade:
    broker: str
    account_id: str
    account_type: AccountType
    account_label: str
    executed_at: datetime
    instrument_type: InstrumentType
    symbol: str
    side: TradeSide
    quantity: float
    price: float
    fees: float = 0.0
    net_amount: float | None = None
    currency: str = "USD"
    trade_id: str | None = None
    option_symbol_raw: str | None = None
    underlying: str | None = None
    expiration: str | None = None
    strike: float | None = None
    call_put: str | None = None
    multiplier: int = 1
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Options usually represent contracts with a 100-share multiplier by default.
        if self.instrument_type == InstrumentType.OPTION and self.multiplier in {0, 1}:
            self.multiplier = 100

    def contract_symbol(self) -> str:
        if self.instrument_type == InstrumentType.STOCK:
            return self.symbol.upper()
        if self.option_symbol_raw:
            return self.option_symbol_raw
        parts = [
            (self.underlying or self.symbol).upper(),
            str(self.expiration or ""),
            str(self.strike or ""),
            str(self.call_put or ""),
        ]
        return "|".join(parts)


@dataclass(slots=True)
class CashActivity:
    broker: str
    account_id: str
    account_type: AccountType
    posted_at: datetime
    type: str
    amount: float
    description: str = ""
    source: str = ""
    is_external: bool = True
    transfer_group_id: str | None = None


@dataclass(slots=True)
class RealizedPnLRow:
    symbol: str
    account_id: str
    account_type: AccountType
    instrument_type: InstrumentType
    opened_at: datetime
    closed_at: datetime
    quantity: float
    proceeds: float
    cost_basis: float
    fees: float
    realized_pnl: float
    holding_days: int
    close_trade_id: str | None = None
    is_wash_sale: bool = False
    wash_disallowed_loss: float = 0.0


@dataclass(slots=True)
class OpenPositionRow:
    symbol: str
    account_id: str
    account_type: AccountType
    instrument_type: InstrumentType
    quantity: float
    average_cost: float
    mark_price: float | None
    market_value: float | None
    unrealized_pnl: float | None


@dataclass(slots=True)
class WashSaleRiskRow:
    symbol: str
    loss_sale_date: datetime
    replacement_buy_date: datetime
    sale_account_id: str
    replacement_account_id: str
    sale_account_type: AccountType
    replacement_account_type: AccountType
    loss_amount: float
    notes: str
    sale_trade_id: str | None = None
    replacement_trade_id: str | None = None
