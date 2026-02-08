"""SQLite schema bootstrap for local analytics storage."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from portfolio_assistant.config.settings import get_settings


SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    account_label TEXT NOT NULL,
    broker TEXT NOT NULL,
    account_type TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trades_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker TEXT NOT NULL,
    account_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    row_index INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trades_normalized (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT,
    broker TEXT NOT NULL,
    account_id TEXT NOT NULL,
    account_type TEXT NOT NULL,
    account_label TEXT NOT NULL,
    executed_at TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    fees REAL NOT NULL DEFAULT 0,
    net_amount REAL,
    currency TEXT NOT NULL DEFAULT 'USD',
    option_symbol_raw TEXT,
    underlying TEXT,
    expiration TEXT,
    strike REAL,
    call_put TEXT,
    multiplier INTEGER NOT NULL DEFAULT 100
);

CREATE TABLE IF NOT EXISTS cash_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker TEXT NOT NULL,
    account_id TEXT NOT NULL,
    account_type TEXT NOT NULL,
    posted_at TEXT NOT NULL,
    type TEXT NOT NULL,
    amount REAL NOT NULL,
    description TEXT,
    source TEXT,
    is_external INTEGER NOT NULL DEFAULT 1,
    transfer_group_id TEXT
);

CREATE TABLE IF NOT EXISTS price_cache (
    symbol TEXT NOT NULL,
    as_of TEXT NOT NULL,
    price REAL NOT NULL,
    provider TEXT,
    PRIMARY KEY(symbol, as_of)
);

CREATE TABLE IF NOT EXISTS pnl_realized (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    account_id TEXT NOT NULL,
    account_type TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT NOT NULL,
    quantity REAL NOT NULL,
    proceeds REAL NOT NULL,
    cost_basis REAL NOT NULL,
    fees REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    holding_days INTEGER NOT NULL,
    is_wash_sale INTEGER NOT NULL DEFAULT 0,
    wash_disallowed_loss REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS positions_open (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    account_id TEXT NOT NULL,
    account_type TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    quantity REAL NOT NULL,
    average_cost REAL NOT NULL,
    mark_price REAL,
    market_value REAL,
    unrealized_pnl REAL
);
"""


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    settings = get_settings()
    target = Path(db_path) if db_path else settings.db_path
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(target)
    connection.row_factory = sqlite3.Row
    return connection


def run_migrations(db_path: str | Path | None = None) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.commit()


if __name__ == "__main__":
    run_migrations()
