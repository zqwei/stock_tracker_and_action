"""Safe DB read helpers for assistant tool usage."""

from __future__ import annotations

import sqlite3
from typing import Any

from portfolio_assistant.db.migrate import get_connection


def query_rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if not sql.strip().lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed")

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def table_count(table_name: str) -> int:
    sql = f"SELECT COUNT(*) AS count FROM {table_name}"
    result = query_rows(sql)
    return int(result[0]["count"]) if result else 0
