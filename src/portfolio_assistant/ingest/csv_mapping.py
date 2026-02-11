from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from portfolio_assistant.config.paths import PRIVATE_DIR

TRADE_REQUIRED_FIELDS = [
    "executed_at",
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
    "total_cost",
    "fees",
    "net_amount",
    "option_symbol_raw",
    "multiplier",
    "currency",
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
        "executed_at": ["trade date", "filled time", "date", "datetime", "time", "executed at"],
        "instrument_type": ["instrument", "type", "asset type", "asset class"],
        "symbol": ["ticker", "symbol", "underlying"],
        "side": ["buy/sell", "buy sell", "side", "action", "transaction type"],
        "quantity": ["qty", "quantity", "filled"],
        "price": ["unit price", "price", "avg price", "average price", "fill price"],
        "total_cost": ["total cost", "total amount", "gross amount"],
        "fees": ["fee", "fees", "commission", "commission/fees", "charges"],
        "net_amount": ["amount", "net", "net amount"],
        "multiplier": ["multiplier", "contract size", "contract multiplier"],
        "currency": ["currency", "ccy"],
        "option_symbol_raw": ["option symbol", "option_symbol_raw", "description"],
    },
    "webull": {
        "trade_id": ["order id", "id"],
        "executed_at": ["filled time", "trade date", "time", "date", "placed time"],
        "instrument_type": ["type", "asset type"],
        "symbol": ["symbol", "ticker"],
        "side": ["buy/sell", "side", "action"],
        "quantity": ["filled", "filled qty", "quantity", "qty", "total qty"],
        "price": ["avg price", "unit price", "price", "fill price"],
        "total_cost": ["total cost", "amount", "net amount"],
        "fees": ["fee", "fees", "commission", "transaction fee"],
        "net_amount": ["net amount", "amount"],
        "multiplier": ["multiplier", "contract size"],
        "currency": ["currency"],
        "option_symbol_raw": ["option symbol", "option_symbol_raw"],
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


def _match_key(text: str) -> str:
    return "".join(ch for ch in text.strip().lower() if ch.isalnum())


def _tokenize(text: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", text.strip().lower()) if token]


def file_signature(columns: list[str]) -> str:
    canonical = "|".join(_normalize(c) for c in columns)
    return sha256(canonical.encode("utf-8")).hexdigest()


def _infer_with_template(
    columns: list[str], template: dict[str, list[str]]
) -> dict[str, str]:
    normalized_to_original = {_normalize(column): column for column in columns}
    compact_to_original: dict[str, str] = {}
    ambiguous_compact: set[str] = set()
    for column in columns:
        compact = _match_key(column)
        if not compact:
            continue
        previous = compact_to_original.get(compact)
        if previous is None:
            compact_to_original[compact] = column
        elif previous != column:
            ambiguous_compact.add(compact)

    mapping: dict[str, str] = {}
    for canonical, aliases in template.items():
        candidates = [canonical, *aliases]
        for candidate in candidates:
            source_column = normalized_to_original.get(_normalize(candidate))
            if not source_column:
                compact_candidate = _match_key(candidate)
                if compact_candidate and compact_candidate not in ambiguous_compact:
                    source_column = compact_to_original.get(compact_candidate)
            if source_column:
                mapping[canonical] = source_column
                break
    return mapping


def infer_trade_column_map(columns: list[str], broker: str = "generic") -> dict[str, str]:
    broker_key = broker.lower()
    template = BROKER_TEMPLATES.get(broker_key, BROKER_TEMPLATES["generic"])
    mapping = _infer_with_template(columns, template=template)

    if broker_key == "webull":
        normalized_to_original = {_normalize(column): column for column in columns}

        if "filled time" in normalized_to_original:
            mapping["executed_at"] = normalized_to_original["filled time"]
        elif "placed time" in normalized_to_original:
            mapping["executed_at"] = normalized_to_original["placed time"]

        if "filled" in normalized_to_original:
            mapping["quantity"] = normalized_to_original["filled"]
        elif "total qty" in normalized_to_original:
            mapping["quantity"] = normalized_to_original["total qty"]

        if "avg price" in normalized_to_original:
            mapping["price"] = normalized_to_original["avg price"]

    return mapping


def infer_cash_column_map(columns: list[str], broker: str = "generic") -> dict[str, str]:
    template = CASH_TEMPLATES.get(broker.lower(), CASH_TEMPLATES["generic"])
    return _infer_with_template(columns, template=template)


def infer_column_map(columns: list[str], broker: str = "generic") -> dict[str, str]:
    # Backward compatible alias used by earlier code for trade imports.
    return infer_trade_column_map(columns, broker=broker)


TRADE_FIELD_HELP: dict[str, str] = {
    "trade_id": "Optional broker order/trade id used for traceability and de-dupe.",
    "executed_at": "Trade Date / Filled Time. Prefer execution/fill time over order submission time.",
    "instrument_type": "Stock or Option. Optional if the file is single-type or options can be inferred.",
    "symbol": "Ticker or underlying symbol.",
    "side": "Buy/Sell for stock. Option files can also use BTO/STO/BTC/STC.",
    "quantity": "Filled quantity/contracts (not total order quantity when partially filled).",
    "price": "Unit price per share/contract (prefer average fill price).",
    "total_cost": "Optional total trade amount from broker export; can be used to estimate fees.",
    "fees": "Optional commission/fees. If missing, app can estimate from Total Cost and Unit Price.",
    "net_amount": "Signed cash impact. Can be auto-computed if missing.",
    "option_symbol_raw": "Optional raw OCC option symbol (for expiry/strike parsing). Leave blank for stock files.",
    "multiplier": "Contract multiplier (usually 100 for US equity options).",
    "currency": "Optional. If omitted, imports default to USD.",
}

CASH_FIELD_HELP: dict[str, str] = {
    "posted_at": "Cash posting timestamp (use settlement or posted date when available).",
    "activity_type": "Deposit, withdrawal, dividend, interest, fee, transfer, or similar cash activity.",
    "amount": "Positive cash amount in account currency for the row.",
    "description": "Broker memo/details shown for this cash activity.",
    "source": "Funding source/channel such as ACH, wire, journal, or internal transfer.",
}


def trade_mapping_hints(columns: list[str], broker: str = "generic") -> list[str]:
    normalized = {_normalize(column) for column in columns}
    hints: list[str] = []

    if broker.strip().lower() == "webull":
        if "filled" in normalized and "total qty" in normalized:
            hints.append("Use `Filled` for quantity; `Total Qty` includes unfilled size.")
        if "filled time" in normalized and "placed time" in normalized:
            hints.append(
                "Use `Filled Time` for Trade Date / Filled Time; `Placed Time` is order submission time."
            )
        if "avg price" in normalized and "price" in normalized:
            hints.append("Use `Avg Price` for execution price; `Price` can be the limit quote.")
        if "unit price" in normalized:
            hints.append("`Unit Price` maps to Unit Price.")
        if "status" in normalized:
            hints.append("Rows with cancelled/rejected status often have zero filled qty and will be skipped.")
        if "type" not in normalized and "symbol" in normalized:
            hints.append(
                "No explicit `Type` detected: use default instrument type in UI or infer options from OCC symbols."
            )

    if "price" in normalized and "avg price" in normalized:
        hints.append("When both `Price` and `Avg Price` exist, average fill price is usually safer for P&L.")
    if "trade date" in normalized and "filled time" not in normalized:
        hints.append(
            "`Trade Date` is acceptable for Trade Date / Filled Time when filled time is unavailable."
        )
    if "total cost" in normalized:
        hints.append("Map `Total Cost` if you want automatic fee estimation when fee column is missing.")

    return hints


def suggest_trade_column_candidates(
    columns: list[str],
    canonical_field: str,
    *,
    broker: str = "generic",
    limit: int = 3,
) -> list[str]:
    if canonical_field not in TRADE_CANONICAL_FIELDS:
        return []

    broker_key = broker.lower()
    generic_template = BROKER_TEMPLATES["generic"]
    broker_template = BROKER_TEMPLATES.get(broker_key, generic_template)
    alias_pool = [
        canonical_field,
        *generic_template.get(canonical_field, []),
        *broker_template.get(canonical_field, []),
    ]
    alias_pool = [alias for alias in alias_pool if alias]
    if not alias_pool:
        return []

    alias_normalized = {_normalize(alias) for alias in alias_pool}
    alias_match_keys = {_match_key(alias) for alias in alias_pool if _match_key(alias)}
    alias_tokens = {token for alias in alias_pool for token in _tokenize(alias)}

    candidates: list[tuple[int, int, str]] = []
    for idx, column in enumerate(columns):
        normalized_column = _normalize(column)
        match_key = _match_key(column)
        column_tokens = set(_tokenize(column))
        score = 0
        if normalized_column in alias_normalized:
            score += 200
        if match_key and match_key in alias_match_keys:
            score += 180
        shared_tokens = alias_tokens.intersection(column_tokens)
        if shared_tokens:
            score += len(shared_tokens) * 25
        if canonical_field == "executed_at" and {"date", "time"}.intersection(column_tokens):
            score += 10
        if canonical_field == "side" and {"buy", "sell", "action", "side"}.intersection(column_tokens):
            score += 10
        if canonical_field == "quantity" and {"qty", "quantity", "filled"}.intersection(column_tokens):
            score += 10
        if canonical_field == "price" and {"price", "avg", "average", "unit"}.intersection(column_tokens):
            score += 10
        if score > 0:
            candidates.append((score, -idx, column))

    candidates.sort(reverse=True)
    ordered: list[str] = []
    for _, _, column in candidates:
        if column in ordered:
            continue
        ordered.append(column)
        if len(ordered) >= limit:
            break
    return ordered


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
    compact_columns: dict[str, str] = {}
    ambiguous_compact_columns: set[str] = set()
    for col in columns or []:
        col_text = str(col)
        exact_columns[col_text] = col_text
        normalized_col = _normalize(col_text)
        previous = normalized_columns.get(normalized_col)
        if previous is None:
            normalized_columns[normalized_col] = col_text
        elif previous != col_text:
            ambiguous_normalized_columns.add(normalized_col)
        compact_col = _match_key(col_text)
        if compact_col:
            compact_previous = compact_columns.get(compact_col)
            if compact_previous is None:
                compact_columns[compact_col] = col_text
            elif compact_previous != col_text:
                ambiguous_compact_columns.add(compact_col)

    allowed_field_map = {
        _normalize(str(field)): str(field) for field in (canonical_fields or [])
    }
    allowed_field_match_map = {
        _match_key(str(field)): str(field)
        for field in (canonical_fields or [])
        if _match_key(str(field))
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
                resolved_canonical = allowed_field_match_map.get(_match_key(canonical_text))
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
                    compact_source = _match_key(source_text)
                    if compact_source in ambiguous_compact_columns:
                        errors.append(
                            "Source column "
                            f"'{source_text}' for field '{resolved_canonical}' is ambiguous in the CSV."
                        )
                        continue
                    resolved_source = compact_columns.get(compact_source)
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
        "updated_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
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
