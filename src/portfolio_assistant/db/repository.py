"""DB persistence/query helpers for accounts, imports, and analytics snapshots."""

from __future__ import annotations

import json
from datetime import datetime

from portfolio_assistant.db.migrate import get_connection
from portfolio_assistant.db.models import (
    Account,
    AccountType,
    CashActivity,
    InstrumentType,
    OpenPositionRow,
    RealizedPnLRow,
    Trade,
    TradeSide,
)
from portfolio_assistant.utils.dates import parse_datetime


def save_account(account: Account) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO accounts(account_id, account_label, broker, account_type)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                account_label = excluded.account_label,
                broker = excluded.broker,
                account_type = excluded.account_type
            """,
            (account.account_id, account.account_label, account.broker, account.account_type.value),
        )
        conn.commit()


def list_accounts() -> list[Account]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT account_id, account_label, broker, account_type FROM accounts ORDER BY account_label"
        ).fetchall()

    return [
        Account(
            account_id=row["account_id"],
            account_label=row["account_label"],
            broker=row["broker"],
            account_type=AccountType(row["account_type"]),
        )
        for row in rows
    ]


def get_account(account_id: str) -> Account | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT account_id, account_label, broker, account_type FROM accounts WHERE account_id = ?",
            (account_id,),
        ).fetchone()

    if row is None:
        return None

    return Account(
        account_id=row["account_id"],
        account_label=row["account_label"],
        broker=row["broker"],
        account_type=AccountType(row["account_type"]),
    )


def save_trade_import(
    source_file: str,
    account: Account,
    signature: str,
    mapping: dict[str, str],
    raw_rows: list[dict[str, str]],
    trades: list[Trade],
) -> int:
    payload_template = {"signature": signature, "mapping": mapping}

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO accounts(account_id, account_label, broker, account_type)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                account_label = excluded.account_label,
                broker = excluded.broker,
                account_type = excluded.account_type
            """,
            (account.account_id, account.account_label, account.broker, account.account_type.value),
        )

        for idx, raw_row in enumerate(raw_rows):
            payload = dict(payload_template)
            payload["row"] = raw_row
            conn.execute(
                """
                INSERT INTO trades_raw(broker, account_id, source_file, row_index, payload_json)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    account.broker,
                    account.account_id,
                    source_file,
                    idx,
                    json.dumps(payload, sort_keys=True),
                ),
            )

        for trade in trades:
            conn.execute(
                """
                INSERT INTO trades_normalized(
                    trade_id, broker, account_id, account_type, account_label,
                    executed_at, instrument_type, symbol, side,
                    quantity, price, fees, net_amount, currency,
                    option_symbol_raw, underlying, expiration, strike,
                    call_put, multiplier
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.trade_id,
                    trade.broker,
                    trade.account_id,
                    trade.account_type.value,
                    trade.account_label,
                    trade.executed_at.isoformat(),
                    trade.instrument_type.value,
                    trade.symbol,
                    trade.side.value,
                    trade.quantity,
                    trade.price,
                    trade.fees,
                    trade.net_amount,
                    trade.currency,
                    trade.option_symbol_raw,
                    trade.underlying,
                    trade.expiration,
                    trade.strike,
                    trade.call_put,
                    trade.multiplier,
                ),
            )

        conn.commit()

    return len(trades)


def save_cash_activity(activities: list[CashActivity]) -> int:
    if not activities:
        return 0

    with get_connection() as conn:
        for item in activities:
            conn.execute(
                """
                INSERT INTO cash_activity(
                    broker, account_id, account_type, posted_at, type,
                    amount, description, source, is_external, transfer_group_id
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.broker,
                    item.account_id,
                    item.account_type.value,
                    item.posted_at.isoformat(),
                    item.type,
                    item.amount,
                    item.description,
                    item.source,
                    1 if item.is_external else 0,
                    item.transfer_group_id,
                ),
            )
        conn.commit()

    return len(activities)


def list_trades(account_id: str | None = None) -> list[Trade]:
    sql = """
        SELECT
            trade_id, broker, account_id, account_type, account_label,
            executed_at, instrument_type, symbol, side,
            quantity, price, fees, net_amount, currency,
            option_symbol_raw, underlying, expiration, strike,
            call_put, multiplier
        FROM trades_normalized
    """
    params: tuple[str, ...] = ()
    if account_id:
        sql += " WHERE account_id = ?"
        params = (account_id,)
    sql += " ORDER BY executed_at, id"

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        Trade(
            trade_id=row["trade_id"],
            broker=row["broker"],
            account_id=row["account_id"],
            account_type=AccountType(row["account_type"]),
            account_label=row["account_label"],
            executed_at=parse_datetime(row["executed_at"]),
            instrument_type=InstrumentType(row["instrument_type"]),
            symbol=row["symbol"],
            side=TradeSide(row["side"]),
            quantity=float(row["quantity"]),
            price=float(row["price"]),
            fees=float(row["fees"]),
            net_amount=float(row["net_amount"]) if row["net_amount"] is not None else None,
            currency=row["currency"],
            option_symbol_raw=row["option_symbol_raw"],
            underlying=row["underlying"],
            expiration=row["expiration"],
            strike=float(row["strike"]) if row["strike"] is not None else None,
            call_put=row["call_put"],
            multiplier=int(row["multiplier"]),
        )
        for row in rows
    ]


def list_cash_activity(account_id: str | None = None) -> list[CashActivity]:
    sql = """
        SELECT
            broker, account_id, account_type, posted_at, type,
            amount, description, source, is_external, transfer_group_id
        FROM cash_activity
    """
    params: tuple[str, ...] = ()
    if account_id:
        sql += " WHERE account_id = ?"
        params = (account_id,)
    sql += " ORDER BY posted_at, id"

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        CashActivity(
            broker=row["broker"],
            account_id=row["account_id"],
            account_type=AccountType(row["account_type"]),
            posted_at=parse_datetime(row["posted_at"]),
            type=row["type"],
            amount=float(row["amount"]),
            description=row["description"] or "",
            source=row["source"] or "",
            is_external=bool(row["is_external"]),
            transfer_group_id=row["transfer_group_id"],
        )
        for row in rows
    ]


def replace_derived_pnl(
    realized_rows: list[RealizedPnLRow],
    open_rows: list[OpenPositionRow],
) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM pnl_realized")
        conn.execute("DELETE FROM positions_open")

        for row in realized_rows:
            conn.execute(
                """
                INSERT INTO pnl_realized(
                    symbol, account_id, account_type, instrument_type,
                    opened_at, closed_at, quantity, proceeds, cost_basis,
                    fees, realized_pnl, holding_days, is_wash_sale, wash_disallowed_loss
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.symbol,
                    row.account_id,
                    row.account_type.value,
                    row.instrument_type.value,
                    row.opened_at.isoformat(),
                    row.closed_at.isoformat(),
                    row.quantity,
                    row.proceeds,
                    row.cost_basis,
                    row.fees,
                    row.realized_pnl,
                    row.holding_days,
                    1 if row.is_wash_sale else 0,
                    row.wash_disallowed_loss,
                ),
            )

        for row in open_rows:
            conn.execute(
                """
                INSERT INTO positions_open(
                    symbol, account_id, account_type, instrument_type,
                    quantity, average_cost, mark_price, market_value, unrealized_pnl
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.symbol,
                    row.account_id,
                    row.account_type.value,
                    row.instrument_type.value,
                    row.quantity,
                    row.average_cost,
                    row.mark_price,
                    row.market_value,
                    row.unrealized_pnl,
                ),
            )

        conn.commit()


def table_counts() -> dict[str, int]:
    tables = ["accounts", "trades_raw", "trades_normalized", "cash_activity", "pnl_realized", "positions_open"]
    counts: dict[str, int] = {}

    with get_connection() as conn:
        for table in tables:
            value = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"]
            counts[table] = int(value)

    return counts


def raw_trade_payload_sample(limit: int = 20) -> list[dict[str, str]]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT source_file, row_index, payload_json FROM trades_raw ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()

    output: list[dict[str, str]] = []
    for row in rows:
        payload = json.loads(row["payload_json"])
        output.append(
            {
                "source_file": row["source_file"],
                "row_index": str(row["row_index"]),
                "signature": str(payload.get("signature", "")),
                "mapping": json.dumps(payload.get("mapping", {}), sort_keys=True),
            }
        )
    return output


def load_daily_realized(account_id: str | None = None) -> list[tuple[datetime, float]]:
    sql = "SELECT closed_at, realized_pnl FROM pnl_realized"
    params: tuple[str, ...] = ()
    if account_id:
        sql += " WHERE account_id = ?"
        params = (account_id,)
    sql += " ORDER BY closed_at"

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    daily: dict[str, float] = {}
    for row in rows:
        key = parse_datetime(row["closed_at"]).date().isoformat()
        daily[key] = daily.get(key, 0.0) + float(row["realized_pnl"])

    return [(parse_datetime(k), v) for k, v in sorted(daily.items())]
