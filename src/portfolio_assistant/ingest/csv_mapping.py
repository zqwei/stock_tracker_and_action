from __future__ import annotations

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from portfolio_assistant.config.paths import PRIVATE_DIR

TRADE_REQUIRED_FIELDS = [
    "executed_at",
    "instrument_type",
    "side",
    "quantity",
    "price",
]

CASH_REQUIRED_FIELDS = ["posted_at", "activity_type", "amount"]

TRADE_CANONICAL_FIELDS = [
    "trade_id",
    "executed_at",
    "instrument_type",
    "symbol",
    "side",
    "quantity",
    "price",
    "fees",
    "net_amount",
    "currency",
    "option_symbol_raw",
]

CASH_CANONICAL_FIELDS = [
    "posted_at",
    "activity_type",
    "amount",
    "description",
    "source",
]

BROKER_TEMPLATES = {
    "generic": {
        "trade_id": ["id", "order id", "trade id"],
        "executed_at": ["date", "datetime", "time", "executed at"],
        "instrument_type": ["instrument", "type", "asset type"],
        "symbol": ["ticker", "symbol", "underlying"],
        "side": ["side", "action"],
        "quantity": ["qty", "quantity", "filled"],
        "price": ["price", "avg price", "fill price"],
        "fees": ["fee", "fees", "commission"],
        "net_amount": ["amount", "net", "net amount"],
        "currency": ["currency", "ccy"],
        "option_symbol_raw": ["option symbol", "option_symbol_raw", "description"],
    },
    "webull": {
        "trade_id": ["order id", "id"],
        "executed_at": ["filled time", "time", "date"],
        "instrument_type": ["type"],
        "symbol": ["symbol", "ticker"],
        "side": ["side", "action"],
        "quantity": ["filled", "quantity", "qty"],
        "price": ["price", "avg price"],
        "fees": ["fee", "fees", "commission"],
        "net_amount": ["amount", "net amount"],
        "currency": ["currency"],
    },
}

CASH_TEMPLATES = {
    "generic": {
        "posted_at": ["date", "posted at", "time"],
        "activity_type": ["type", "activity type", "direction"],
        "amount": ["amount", "net amount", "value"],
        "description": ["description", "memo", "details"],
        "source": ["source", "method", "channel"],
    }
}


def _normalize(text: str) -> str:
    return " ".join(text.strip().lower().replace("_", " ").split())


def file_signature(columns: list[str]) -> str:
    canonical = "|".join(_normalize(c) for c in columns)
    return sha256(canonical.encode("utf-8")).hexdigest()


def _infer_with_template(
    columns: list[str], template: dict[str, list[str]]
) -> dict[str, str]:
    normalized_to_original = {_normalize(column): column for column in columns}

    mapping: dict[str, str] = {}
    for canonical, aliases in template.items():
        candidates = [canonical, *aliases]
        for candidate in candidates:
            source_column = normalized_to_original.get(_normalize(candidate))
            if source_column:
                mapping[canonical] = source_column
                break
    return mapping


def infer_trade_column_map(columns: list[str], broker: str = "generic") -> dict[str, str]:
    template = BROKER_TEMPLATES.get(broker.lower(), BROKER_TEMPLATES["generic"])
    return _infer_with_template(columns, template=template)


def infer_cash_column_map(columns: list[str], broker: str = "generic") -> dict[str, str]:
    template = CASH_TEMPLATES.get(broker.lower(), CASH_TEMPLATES["generic"])
    return _infer_with_template(columns, template=template)


def infer_column_map(columns: list[str], broker: str = "generic") -> dict[str, str]:
    # Backward compatible alias used by earlier code for trade imports.
    return infer_trade_column_map(columns, broker=broker)


def missing_required_fields(
    mapping: dict[str, str], required_fields: list[str] | None = None
) -> list[str]:
    required = required_fields or TRADE_REQUIRED_FIELDS
    return [field for field in required if field not in mapping]


def _mapping_store_path() -> Path:
    return PRIVATE_DIR / "mappings" / "trade_column_mappings.json"


def load_trade_mapping_store() -> dict[str, Any]:
    path = _mapping_store_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_trade_mapping(
    broker: str, signature: str, columns: list[str], mapping: dict[str, str]
) -> None:
    path = _mapping_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    store = load_trade_mapping_store()
    key = f"{broker.lower()}::{signature}"
    store[key] = {
        "broker": broker,
        "signature": signature,
        "columns": columns,
        "mapping": mapping,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    path.write_text(json.dumps(store, indent=2, sort_keys=True), encoding="utf-8")


def get_saved_trade_mapping(broker: str, signature: str) -> dict[str, str] | None:
    store = load_trade_mapping_store()
    key = f"{broker.lower()}::{signature}"
    record = store.get(key)
    if not record:
        return None
    mapping = record.get("mapping")
    if not isinstance(mapping, dict):
        return None
    return {str(k): str(v) for k, v in mapping.items()}
