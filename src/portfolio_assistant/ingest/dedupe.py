from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
from typing import Any


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Enum):
        value = value.value
    return str(value).strip().upper()


def _normalize_float(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip().replace(",", "").replace("$", "")
        if not text:
            return ""
        if text.startswith("(") and text.endswith(")"):
            text = f"-{text[1:-1]}"
        value = text
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return ""
    if parsed == 0:
        return "0"
    return f"{parsed:.10f}".rstrip("0").rstrip(".")


def _normalize_datetime(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "to_pydatetime"):
        try:
            dt = value.to_pydatetime()
            return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
        except Exception:
            pass
    text = str(value).strip()
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    except ValueError:
        return text


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "to_pydatetime"):
        try:
            dt = value.to_pydatetime()
            return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
        except Exception:
            return str(value)
    return str(value)


def raw_row_hash(raw_payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(raw_payload),
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def trade_dedupe_key(row: Mapping[str, Any]) -> str:
    trade_id = _normalize_text(row.get("trade_id"))
    if trade_id:
        return f"TID:{trade_id}"

    parts = [
        _normalize_datetime(row.get("executed_at")),
        _normalize_text(row.get("instrument_type")),
        _normalize_text(row.get("symbol")),
        _normalize_text(row.get("side")),
        _normalize_text(row.get("option_symbol_raw")),
        _normalize_text(row.get("underlying")),
        _normalize_datetime(row.get("expiration")),
        _normalize_float(row.get("strike")),
        _normalize_float(row.get("quantity")),
        _normalize_float(row.get("price")),
        _normalize_float(row.get("fees")),
        _normalize_float(row.get("net_amount")),
        _normalize_text(row.get("currency")),
        _normalize_float(row.get("multiplier")),
        _normalize_text(row.get("broker")),
    ]
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"SIG:{digest}"


def cash_dedupe_key(row: Mapping[str, Any]) -> str:
    parts = [
        _normalize_datetime(row.get("posted_at")),
        _normalize_text(row.get("activity_type")),
        _normalize_float(row.get("amount")),
        _normalize_text(row.get("description")),
        _normalize_text(row.get("source")),
        _normalize_text(row.get("transfer_group_id")),
        _normalize_text(row.get("broker")),
    ]
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"CASH:{digest}"
