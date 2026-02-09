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

    exact_columns: dict[str, str] = {}
    normalized_columns: dict[str, str] = {}
    ambiguous_normalized_columns: set[str] = set()
    for col in columns or []:
        col_text = str(col)
        exact_columns[col_text] = col_text
        normalized_col = _normalize(col_text)
        previous = normalized_columns.get(normalized_col)
        if previous is None:
            normalized_columns[normalized_col] = col_text
        elif previous != col_text:
            ambiguous_normalized_columns.add(normalized_col)

    allowed_field_map = {
        _normalize(str(field)): str(field) for field in (canonical_fields or [])
    }

    cleaned: dict[str, str] = {}
    for canonical, source in mapping.items():
        canonical_text = str(canonical).strip()
        source_text = str(source).strip()
        if not canonical_text:
            errors.append("Mapping contains an empty canonical field name.")
            continue
        if isinstance(source, (dict, list, tuple, set)):
            errors.append(
                f"Canonical field '{canonical_text}' has a non-scalar source column value."
            )
            continue
        if not source_text:
            errors.append(f"Canonical field '{canonical_text}' has an empty source column.")
            continue

        normalized_canonical = _normalize(canonical_text)
        if allowed_field_map:
            resolved_canonical = allowed_field_map.get(normalized_canonical)
            if resolved_canonical is None:
                errors.append(f"Unsupported canonical field '{canonical_text}'.")
                continue
        else:
            resolved_canonical = canonical_text

        if columns is not None:
            if source_text in exact_columns:
                resolved_source = exact_columns[source_text]
            else:
                normalized_source = _normalize(source_text)
                if normalized_source in ambiguous_normalized_columns:
                    errors.append(
                        "Source column "
                        f"'{source_text}' for field '{resolved_canonical}' is ambiguous in the CSV."
                    )
                    continue
                resolved_source = normalized_columns.get(normalized_source)
                if resolved_source is None:
                    errors.append(
                        f"Source column '{source_text}' for field '{resolved_canonical}' is not present in the CSV."
                    )
                    continue
        else:
            resolved_source = source_text

        previous_source = cleaned.get(resolved_canonical)
        if previous_source is not None and previous_source != resolved_source:
            errors.append(
                "Canonical field "
                f"'{resolved_canonical}' is mapped to multiple source columns "
                f"('{previous_source}' and '{resolved_source}')."
            )
            continue
        if previous_source is not None:
            errors.append(
                "Canonical field "
                f"'{resolved_canonical}' is mapped more than once."
            )
            continue

        cleaned[resolved_canonical] = resolved_source

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
        sanitized: dict[str, Any] = {}
        for key, value in loaded.items():
            if not isinstance(key, str):
                continue
            if not isinstance(value, dict):
                continue
            sanitized[key] = value
        return sanitized
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
    broker_text = broker.strip()
    signature_text = signature.strip()
    if not broker_text:
        raise ValueError("Broker is required.")
    if not signature_text:
        raise ValueError("Signature is required.")

    path = _mapping_store_path()
    cleaned_mapping, errors = validate_mapping(
        mapping,
        columns=columns,
        canonical_fields=TRADE_CANONICAL_FIELDS,
        required_fields=TRADE_REQUIRED_FIELDS,
    )
    if errors:
        raise ValueError("; ".join(errors))

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
    broker_text = broker.strip().lower()
    signature_text = signature.strip()
    if not broker_text or not signature_text:
        return None

    store = load_trade_mapping_store()
    key = f"{broker_text}::{signature_text}"
    record = store.get(key)
    if not record:
        return None
    if not isinstance(record, dict):
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
        required_fields=TRADE_REQUIRED_FIELDS,
    )
    if errors:
        return None
    return cleaned_mapping
