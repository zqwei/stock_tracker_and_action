from __future__ import annotations

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

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


def validate_mapping(
    mapping: dict[str, Any] | None,
    *,
    columns: list[str] | None = None,
    canonical_fields: list[str] | None = None,
    required_fields: list[str] | None = None,
) -> tuple[dict[str, str], list[str]]:
    errors: list[str] = []
    if mapping is None:
        mapping = {}
    if not isinstance(mapping, dict):
        return {}, ["Mapping must be a dictionary."]

    normalized_columns = {str(col): str(col) for col in (columns or [])}
    allowed_fields = set(canonical_fields or [])

    cleaned: dict[str, str] = {}
    for canonical, source in mapping.items():
        canonical_text = str(canonical).strip()
        source_text = str(source).strip()
        if not canonical_text:
            errors.append("Mapping contains an empty canonical field name.")
            continue
        if not source_text:
            errors.append(f"Canonical field '{canonical_text}' has an empty source column.")
            continue
        if allowed_fields and canonical_text not in allowed_fields:
            errors.append(f"Unsupported canonical field '{canonical_text}'.")
            continue
        if columns is not None and source_text not in normalized_columns:
            errors.append(
                f"Source column '{source_text}' for field '{canonical_text}' is not present in the CSV."
            )
            continue
        cleaned[canonical_text] = normalized_columns.get(source_text, source_text)

    source_to_canonical: dict[str, str] = {}
    for canonical, source in cleaned.items():
        previous = source_to_canonical.get(source)
        if previous:
            errors.append(
                "Source column "
                f"'{source}' is mapped to multiple fields ('{previous}' and '{canonical}')."
            )
        else:
            source_to_canonical[source] = canonical

    if required_fields:
        missing = missing_required_fields(cleaned, required_fields=required_fields)
        errors.extend(f"Missing required field mapping '{field}'." for field in missing)

    return cleaned, errors


def _mapping_store_path() -> Path:
    return PRIVATE_DIR / "mappings" / "trade_column_mappings.json"


def load_trade_mapping_store() -> dict[str, Any]:
    path = _mapping_store_path()
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            return {}
        return loaded
    except json.JSONDecodeError:
        return {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    temp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(path)


def save_trade_mapping(
    broker: str, signature: str, columns: list[str], mapping: dict[str, str]
) -> None:
    path = _mapping_store_path()
    cleaned_mapping, errors = validate_mapping(
        mapping,
        columns=columns,
        canonical_fields=TRADE_CANONICAL_FIELDS,
    )
    if errors:
        raise ValueError("; ".join(errors))

    broker_text = broker.strip()
    signature_text = signature.strip()
    if not broker_text:
        raise ValueError("Broker is required.")
    if not signature_text:
        raise ValueError("Signature is required.")

    clean_columns = [str(col) for col in columns]

    store = load_trade_mapping_store()
    key = f"{broker_text.lower()}::{signature_text}"
    store[key] = {
        "broker": broker_text,
        "signature": signature_text,
        "columns": clean_columns,
        "mapping": cleaned_mapping,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    _write_json_atomic(path, store)


def get_saved_trade_mapping(broker: str, signature: str) -> dict[str, str] | None:
    store = load_trade_mapping_store()
    key = f"{broker.lower()}::{signature}"
    record = store.get(key)
    if not record:
        return None
    columns = record.get("columns")
    if columns is not None and not isinstance(columns, list):
        return None
    mapping = record.get("mapping")
    if mapping is None:
        return None
    cleaned_mapping, errors = validate_mapping(
        mapping,
        columns=[str(col) for col in columns] if isinstance(columns, list) else None,
        canonical_fields=TRADE_CANONICAL_FIELDS,
    )
    if errors:
        return None
    return cleaned_mapping
