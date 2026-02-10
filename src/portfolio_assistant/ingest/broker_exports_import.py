from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd

from portfolio_assistant.ingest.csv_mapping import file_signature
from portfolio_assistant.ingest.validators import parse_datetime, parse_float

BROKER_EXPORT_CANONICAL_FIELDS = [
    "description",
    "symbol",
    "date_acquired",
    "date_sold",
    "proceeds",
    "cost_basis",
    "wash_sale_disallowed",
    "gain_or_loss",
    "term",
]

BROKER_EXPORT_REQUIRED_FIELDS = ["proceeds", "cost_basis"]

_FIELD_TYPE_DEFAULTS = {
    "description": "string",
    "symbol": "string",
    "date_acquired": "date",
    "date_sold": "date",
    "proceeds": "money",
    "cost_basis": "money",
    "wash_sale_disallowed": "money",
    "gain_or_loss": "money",
    "term": "term",
}

_FIELD_ALIASES = {
    "description": ["description", "security", "name"],
    "symbol": ["symbol", "ticker", "underlying"],
    "date_acquired": ["date acquired", "acquired date", "purchase date", "open date"],
    "date_sold": ["date sold", "sale date", "close date", "disposed date", "trade date"],
    "proceeds": ["proceeds", "sell amount", "sales proceeds", "total cost", "unit price"],
    "cost_basis": ["cost basis", "basis", "cost", "purchase amount", "total cost"],
    "wash_sale_disallowed": [
        "wash sale disallowed",
        "wash sale",
        "wash adjustment",
        "wash disallowed",
        "disallowed loss",
    ],
    "gain_or_loss": ["gain/loss", "gain loss", "gain", "profit/loss", "pnl"],
    "term": ["term", "holding period", "st/lt", "short/long"],
}

_CANONICAL_FIELD_ALIASES = {
    "description": "description",
    "security": "description",
    "name": "description",
    "symbol": "symbol",
    "ticker": "symbol",
    "underlying": "symbol",
    "date acquired": "date_acquired",
    "acquired date": "date_acquired",
    "purchase date": "date_acquired",
    "open date": "date_acquired",
    "date sold": "date_sold",
    "sale date": "date_sold",
    "close date": "date_sold",
    "disposed date": "date_sold",
    "proceeds": "proceeds",
    "sell amount": "proceeds",
    "sales proceeds": "proceeds",
    "cost basis": "cost_basis",
    "basis": "cost_basis",
    "cost": "cost_basis",
    "wash sale disallowed": "wash_sale_disallowed",
    "wash sale": "wash_sale_disallowed",
    "wash adjustment": "wash_sale_disallowed",
    "wash disallowed": "wash_sale_disallowed",
    "gain/loss": "gain_or_loss",
    "gain loss": "gain_or_loss",
    "gain or loss": "gain_or_loss",
    "gain": "gain_or_loss",
    "profit/loss": "gain_or_loss",
    "pnl": "gain_or_loss",
    "term": "term",
    "holding period": "term",
    "st/lt": "term",
    "short/long": "term",
}

_TERM_ALIASES = {
    "ST": "ST",
    "SHORT": "ST",
    "SHORT TERM": "ST",
    "SHORT-TERM": "ST",
    "LT": "LT",
    "LONG": "LT",
    "LONG TERM": "LT",
    "LONG-TERM": "LT",
    "UNKNOWN": "UNKNOWN",
}

_ALLOWED_PARSER_TYPES = {"string", "date", "money", "number", "term", "code"}
_ALLOWED_TRANSFORMS = {"normalize term", "uppercase", "lowercase"}

_DEFAULT_DATE_FORMATS = [
    "%m/%d/%Y",
    "%m/%d/%y",
    "%Y-%m-%d",
]


_REALIZED_ACTIVITY_REQUIRED_KEYS = {
    "buy/sell",
    "trade date",
    "quantity",
    "total cost",
}


@dataclass(frozen=True)
class BrokerExportPreview:
    columns: list[str]
    sample_rows: list[dict[str, Any]]
    mapping: dict[str, str]
    missing_required: list[str]
    signature: str


@dataclass(frozen=True)
class ReconciliationImportResult:
    rows: list[dict[str, Any]]
    issues: list[str]
    source: str | None
    used_pdf_fallback: bool
    needs_review: bool


def _normalize(text: str) -> str:
    return " ".join(str(text).strip().lower().replace("_", " ").split())


def _canonical_field_name(field: str) -> str | None:
    normalized = _normalize(field)
    resolved = _CANONICAL_FIELD_ALIASES.get(normalized, normalized)
    if resolved in BROKER_EXPORT_CANONICAL_FIELDS:
        return resolved
    return None


def normalize_term(value: Any, *, term_map: dict[str, str] | None = None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    normalized = _normalize(text).upper()
    if term_map:
        mapped = term_map.get(_normalize(text))
        if mapped is not None:
            normalized = _normalize(mapped).upper()

    normalized = normalized.replace("_", " ")
    return _TERM_ALIASES.get(normalized, normalized)


def _resolve_source_column(source: str, columns: list[str] | None) -> tuple[str | None, str | None]:
    source_text = str(source).strip()
    if not source_text:
        return None, "source column is empty"
    if columns is None:
        return source_text, None

    exact_columns: dict[str, str] = {}
    normalized_columns: dict[str, str] = {}
    ambiguous_normalized: set[str] = set()
    for column in columns:
        col_text = str(column)
        exact_columns[col_text] = col_text
        normalized_col = _normalize(col_text)
        previous = normalized_columns.get(normalized_col)
        if previous is None:
            normalized_columns[normalized_col] = col_text
        elif previous != col_text:
            ambiguous_normalized.add(normalized_col)

    if source_text in exact_columns:
        return source_text, None

    normalized_source = _normalize(source_text)
    if normalized_source in ambiguous_normalized:
        return None, f"source column '{source_text}' is ambiguous in the CSV"
    resolved = normalized_columns.get(normalized_source)
    if resolved is None:
        return None, f"source column '{source_text}' is not present in the CSV"
    return resolved, None


def infer_broker_export_column_map(columns: list[str]) -> dict[str, str]:
    normalized_to_original = {_normalize(column): column for column in columns}
    inferred: dict[str, str] = {}

    for field in BROKER_EXPORT_CANONICAL_FIELDS:
        aliases = _FIELD_ALIASES.get(field, [])
        candidates = [field, *aliases]
        for candidate in candidates:
            source_column = normalized_to_original.get(_normalize(candidate))
            if source_column:
                inferred[field] = source_column
                break
    return inferred


def infer_broker_tax_column_map(columns: list[str]) -> dict[str, str]:
    return infer_broker_export_column_map(columns)


def _looks_like_realized_activity_export(columns: list[str]) -> bool:
    normalized = {_normalize(column) for column in columns}
    if not _REALIZED_ACTIVITY_REQUIRED_KEYS.issubset(normalized):
        return False
    return any(
        key in normalized
        for key in {"short term gain/loss", "long term gain/loss", "gain/loss"}
    )


def _normalize_realized_activity_export(
    df: pd.DataFrame,
    *,
    broker: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    columns = list(df.columns)
    normalized_to_original = {_normalize(str(column)): str(column) for column in columns}

    def _col(name: str) -> str | None:
        return normalized_to_original.get(name)

    side_col = _col("buy/sell")
    trade_date_col = _col("trade date")
    symbol_col = _col("symbol")
    description_col = _col("description")
    quantity_col = _col("quantity")
    unit_price_col = _col("unit price")
    total_cost_col = _col("total cost")
    st_gain_col = _col("short term gain/loss")
    lt_gain_col = _col("long term gain/loss")
    gain_col = _col("gain/loss")
    term_col = _col("long/short position")
    disallowed_col = _col("disallowed loss") or _col("wash sale")

    rows: list[dict[str, Any]] = []
    issues: list[str] = []

    for row_number, row_data in enumerate(df.to_dict(orient="records"), start=1):
        side = str(row_data.get(side_col or "", "")).strip().upper()
        if side != "SELL":
            continue

        date_sold = _parse_date(row_data.get(trade_date_col or ""), _DEFAULT_DATE_FORMATS)
        quantity = parse_float(row_data.get(quantity_col or ""), default=None)
        unit_price = parse_float(row_data.get(unit_price_col or ""), default=None)
        total_cost = parse_float(row_data.get(total_cost_col or ""), default=None)
        short_gain = parse_float(row_data.get(st_gain_col or ""), default=0.0) or 0.0
        long_gain = parse_float(row_data.get(lt_gain_col or ""), default=0.0) or 0.0
        direct_gain = parse_float(row_data.get(gain_col or ""), default=None)
        gain_or_loss = direct_gain if direct_gain is not None else (short_gain + long_gain)

        proceeds: float | None = None
        if quantity not in (None, 0) and unit_price not in (None, 0):
            proceeds = abs(float(quantity) * float(unit_price))
        elif total_cost is not None:
            proceeds = abs(float(total_cost))

        cost_basis = (
            (proceeds - gain_or_loss) if (proceeds is not None and gain_or_loss is not None) else None
        )
        term = normalize_term(row_data.get(term_col or "")) or "UNKNOWN"
        wash_sale_disallowed = parse_float(row_data.get(disallowed_col or ""), default=0.0) or 0.0

        if date_sold is None:
            issues.append(f"Row {row_number}: invalid trade date")
            continue
        if proceeds is None or cost_basis is None:
            issues.append(f"Row {row_number}: could not derive proceeds/cost basis from realized export")
            continue

        rows.append(
            {
                "description": str(row_data.get(description_col or "", "")).strip() or None,
                "symbol": str(row_data.get(symbol_col or "", "")).strip().upper() or None,
                "date_acquired": None,
                "date_sold": date_sold,
                "proceeds": float(proceeds),
                "cost_basis": float(cost_basis),
                "wash_sale_disallowed": float(wash_sale_disallowed),
                "gain_or_loss": float(gain_or_loss or 0.0),
                "term": term,
                "currency": "USD",
                "broker": broker,
            }
        )

    return rows, issues


def _mapping_is_schema(mapping: dict[str, Any]) -> bool:
    return "columns" in mapping and isinstance(mapping.get("columns"), dict)


def _parse_date(value: Any, formats: list[str]) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.date()
        except ValueError:
            continue

    parsed_generic = parse_datetime(text)
    if parsed_generic is None:
        return None
    return parsed_generic.date()


def _parse_money(
    value: Any,
    *,
    allow_parentheses_for_negative: bool,
    allow_commas: bool,
) -> float | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.upper() in {"N/A", "NA", "--"}:
        return None

    negative = False
    if allow_parentheses_for_negative and text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1].strip()

    text = text.replace("$", "")
    if allow_commas:
        text = text.replace(",", "")
    text = text.replace("+", "")
    if text.startswith("-"):
        negative = True
        text = text[1:]

    parsed = parse_float(text, default=None)
    if parsed is None:
        return None
    return -abs(parsed) if negative else float(parsed)


def _parse_number(value: Any) -> float | None:
    return parse_float(value, default=None)


def _parse_value(
    value: Any,
    parser_type: str,
    *,
    date_formats: list[str],
    allow_parentheses_for_negative: bool,
    allow_commas: bool,
    term_map: dict[str, str],
) -> tuple[Any, bool]:
    if parser_type == "string":
        text = str(value).strip() if value is not None else ""
        return (text or None), True
    if parser_type == "code":
        text = str(value).strip().upper() if value is not None else ""
        return (text or None), True
    if parser_type == "date":
        parsed = _parse_date(value, date_formats)
        if parsed is None and str(value).strip():
            return None, False
        return parsed, True
    if parser_type == "money":
        parsed = _parse_money(
            value,
            allow_parentheses_for_negative=allow_parentheses_for_negative,
            allow_commas=allow_commas,
        )
        if parsed is None and str(value).strip():
            return None, False
        return parsed, True
    if parser_type == "number":
        parsed = _parse_number(value)
        if parsed is None and str(value).strip():
            return None, False
        return parsed, True
    if parser_type == "term":
        parsed = normalize_term(value, term_map=term_map)
        return parsed, True
    return value, True


def _apply_transform(
    value: Any,
    transform: str | None,
    *,
    term_map: dict[str, str],
) -> Any:
    if value is None or not transform:
        return value
    normalized_transform = _normalize(transform)
    if normalized_transform == "normalize term":
        return normalize_term(value, term_map=term_map)
    if normalized_transform == "uppercase":
        return str(value).upper()
    if normalized_transform == "lowercase":
        return str(value).lower()
    return value


def _coerce_simple_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    columns: dict[str, dict[str, Any]] = {}
    for field_name, source in mapping.items():
        field = _canonical_field_name(str(field_name))
        if field is None:
            continue
        source_text = str(source).strip()
        if not source_text:
            continue
        columns[source_text] = {
            "field": field,
            "type": _FIELD_TYPE_DEFAULTS.get(field, "string"),
            "required": field in BROKER_EXPORT_REQUIRED_FIELDS,
        }

    return {
        "version": 1,
        "mapping_kind": "broker_tax_export",
        "name": "Inline simple mapping",
        "broker": "generic",
        "output_schema": {
            "canonical_row_version": 1,
            "fields": BROKER_EXPORT_CANONICAL_FIELDS,
        },
        "parsers": {
            "date": {"formats": list(_DEFAULT_DATE_FORMATS)},
            "money": {
                "currency": "USD",
                "allow_parentheses_for_negative": True,
                "allow_commas": True,
            },
        },
        "columns": columns,
        "postprocess": {},
    }


def _to_simple_mapping(mapping: dict[str, Any] | None, columns: list[str]) -> dict[str, str]:
    if mapping is None:
        return infer_broker_export_column_map(columns)

    if _mapping_is_schema(mapping):
        simple: dict[str, str] = {}
        for source, config in mapping.get("columns", {}).items():
            if not isinstance(config, dict):
                continue
            field = _canonical_field_name(str(config.get("field", "")))
            if field is None:
                continue
            resolved_source, _ = _resolve_source_column(str(source), columns)
            if resolved_source:
                simple[field] = resolved_source
        return simple

    simple: dict[str, str] = {}
    for field_name, source in mapping.items():
        canonical = _canonical_field_name(str(field_name))
        if canonical is None:
            continue
        resolved_source, _ = _resolve_source_column(str(source), columns)
        if resolved_source:
            simple[canonical] = resolved_source
    return simple


def validate_broker_export_mapping(
    mapping: dict[str, Any] | None,
    *,
    columns: list[str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []

    if mapping is None:
        mapping = {}
    if not isinstance(mapping, dict):
        return {}, ["Mapping must be a dictionary."]

    schema_mapping = mapping
    if not _mapping_is_schema(schema_mapping):
        schema_mapping = _coerce_simple_mapping(mapping)

    mapping_kind = str(schema_mapping.get("mapping_kind", "")).strip().lower()
    if mapping_kind and mapping_kind != "broker_tax_export":
        errors.append("mapping_kind must be 'broker_tax_export'.")

    parser_config = schema_mapping.get("parsers", {})
    if parser_config is None:
        parser_config = {}
    if not isinstance(parser_config, dict):
        errors.append("parsers must be an object if provided.")
        parser_config = {}

    date_parser = parser_config.get("date", {})
    if not isinstance(date_parser, dict):
        errors.append("parsers.date must be an object if provided.")
        date_parser = {}
    date_formats_raw = date_parser.get("formats") or list(_DEFAULT_DATE_FORMATS)
    date_formats: list[str] = []
    if isinstance(date_formats_raw, list):
        for fmt in date_formats_raw:
            fmt_text = str(fmt).strip()
            if fmt_text:
                date_formats.append(fmt_text)
    if not date_formats:
        date_formats = list(_DEFAULT_DATE_FORMATS)

    money_parser = parser_config.get("money", {})
    if not isinstance(money_parser, dict):
        errors.append("parsers.money must be an object if provided.")
        money_parser = {}
    currency = str(money_parser.get("currency", "USD") or "USD").strip().upper()
    allow_parentheses_for_negative = bool(
        money_parser.get("allow_parentheses_for_negative", True)
    )
    allow_commas = bool(money_parser.get("allow_commas", True))

    postprocess = schema_mapping.get("postprocess", {})
    if postprocess is None:
        postprocess = {}
    if not isinstance(postprocess, dict):
        errors.append("postprocess must be an object if provided.")
        postprocess = {}
    term_map_raw = postprocess.get("normalize_term_map", {})
    term_map: dict[str, str] = {}
    if isinstance(term_map_raw, dict):
        for key, value in term_map_raw.items():
            key_text = str(key).strip()
            value_text = str(value).strip()
            if key_text and value_text:
                term_map[_normalize(key_text)] = value_text

    columns_cfg = schema_mapping.get("columns")
    if not isinstance(columns_cfg, dict) or not columns_cfg:
        errors.append("columns mapping is required.")
        return {}, errors

    normalized_columns: list[dict[str, Any]] = []
    field_to_source: dict[str, str] = {}

    for source, config in columns_cfg.items():
        source_text = str(source).strip()
        if not source_text:
            errors.append("columns contains an empty source column key.")
            continue
        if not isinstance(config, dict):
            errors.append(f"column '{source_text}' config must be an object.")
            continue

        canonical_field = _canonical_field_name(str(config.get("field", "")))
        if canonical_field is None:
            errors.append(
                f"column '{source_text}' has unsupported canonical field '{config.get('field')}'."
            )
            continue

        parser_type = str(config.get("type", "")).strip().lower()
        if parser_type not in _ALLOWED_PARSER_TYPES:
            errors.append(
                f"column '{source_text}' for field '{canonical_field}' has unsupported parser type '{config.get('type')}'."
            )
            continue

        required = bool(config.get("required", False))
        transform_raw = config.get("transform")
        transform = str(transform_raw).strip() if transform_raw is not None else None
        if transform:
            normalized_transform = _normalize(transform)
            if normalized_transform not in _ALLOWED_TRANSFORMS:
                errors.append(
                    f"column '{source_text}' has unsupported transform '{transform}'."
                )
                continue

        resolved_source, source_error = _resolve_source_column(source_text, columns)
        if source_error:
            errors.append(source_error)
            continue
        assert resolved_source is not None

        previous = field_to_source.get(canonical_field)
        if previous and previous != resolved_source:
            errors.append(
                f"canonical field '{canonical_field}' is mapped more than once ('{previous}' and '{resolved_source}')."
            )
            continue
        field_to_source[canonical_field] = resolved_source

        normalized_columns.append(
            {
                "source_column": resolved_source,
                "field": canonical_field,
                "type": parser_type,
                "required": required,
                "transform": transform,
            }
        )

    if not normalized_columns:
        errors.append("No valid column rules were produced from mapping.")
        return {}, errors

    missing_required = [
        field for field in BROKER_EXPORT_REQUIRED_FIELDS if field not in field_to_source
    ]
    for field in missing_required:
        errors.append(f"Missing required field mapping '{field}'.")

    normalized_mapping = {
        "version": int(schema_mapping.get("version", 1) or 1),
        "mapping_kind": "broker_tax_export",
        "name": str(schema_mapping.get("name", "")).strip() or "Broker tax export mapping",
        "broker": str(schema_mapping.get("broker", "")).strip() or "generic",
        "currency": currency or "USD",
        "date_formats": date_formats,
        "allow_parentheses_for_negative": allow_parentheses_for_negative,
        "allow_commas": allow_commas,
        "term_map": term_map,
        "columns": normalized_columns,
        "required_fields": sorted(
            {
                column_rule["field"]
                for column_rule in normalized_columns
                if bool(column_rule.get("required"))
            }
        ),
    }
    return normalized_mapping, errors


def validate_broker_tax_mapping(
    mapping: dict[str, Any] | None,
    *,
    columns: list[str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    return validate_broker_export_mapping(mapping, columns=columns)


def load_broker_export_csv_preview(
    file_obj: str | Path | BinaryIO,
    mapping: dict[str, Any] | None = None,
    *,
    max_rows: int = 200,
) -> BrokerExportPreview:
    df = pd.read_csv(file_obj)
    columns = [str(column) for column in df.columns]
    simple_mapping = _to_simple_mapping(mapping, columns)
    missing_required = [
        field for field in BROKER_EXPORT_REQUIRED_FIELDS if field not in simple_mapping
    ]
    sample_rows = df.head(max_rows).fillna("").to_dict(orient="records")
    return BrokerExportPreview(
        columns=columns,
        sample_rows=sample_rows,
        mapping=simple_mapping,
        missing_required=missing_required,
        signature=file_signature(columns),
    )


def load_broker_tax_csv_preview(
    file_obj: str | Path | BinaryIO,
    mapping: dict[str, Any] | None = None,
    *,
    max_rows: int = 200,
) -> BrokerExportPreview:
    return load_broker_export_csv_preview(file_obj=file_obj, mapping=mapping, max_rows=max_rows)


def normalize_broker_export_records(
    df: pd.DataFrame,
    mapping: dict[str, Any] | None = None,
    *,
    broker: str = "generic",
) -> tuple[list[dict[str, Any]], list[str]]:
    columns = [str(column) for column in df.columns]
    if _looks_like_realized_activity_export(columns) and (
        mapping is None or not _mapping_is_schema(mapping)
    ):
        realized_rows, realized_issues = _normalize_realized_activity_export(df, broker=broker)
        if realized_rows:
            return realized_rows, realized_issues

    if mapping is None:
        mapping = infer_broker_export_column_map(columns)

    normalized_mapping, mapping_errors = validate_broker_export_mapping(
        mapping,
        columns=columns,
    )
    if mapping_errors:
        return [], [f"Mapping error: {error}" for error in mapping_errors]

    normalized_rows: list[dict[str, Any]] = []
    issues: list[str] = []
    date_formats = list(normalized_mapping["date_formats"])
    term_map = dict(normalized_mapping["term_map"])

    for row_number, row_data in enumerate(df.to_dict(orient="records"), start=1):
        row_out: dict[str, Any] = {}
        fatal = False

        for rule in normalized_mapping["columns"]:
            source_column = str(rule["source_column"])
            field_name = str(rule["field"])
            parser_type = str(rule["type"])
            required = bool(rule.get("required", False))
            transform = rule.get("transform")

            raw_value = row_data.get(source_column)
            parsed_value, parse_ok = _parse_value(
                raw_value,
                parser_type,
                date_formats=date_formats,
                allow_parentheses_for_negative=bool(
                    normalized_mapping["allow_parentheses_for_negative"]
                ),
                allow_commas=bool(normalized_mapping["allow_commas"]),
                term_map=term_map,
            )
            parsed_value = _apply_transform(parsed_value, transform, term_map=term_map)

            if not parse_ok:
                issues.append(
                    f"Row {row_number}: invalid value '{raw_value}' for field '{field_name}'."
                )
                if required:
                    fatal = True
                continue

            if parsed_value is None or parsed_value == "":
                if required:
                    issues.append(f"Row {row_number}: missing required field '{field_name}'.")
                    fatal = True
                continue

            row_out[field_name] = parsed_value

        if fatal:
            continue
        if not row_out:
            continue

        description = str(row_out.get("description", "")).strip() or None
        symbol = str(row_out.get("symbol", "")).strip().upper() or None
        date_acquired = row_out.get("date_acquired")
        date_sold = row_out.get("date_sold")
        proceeds = _parse_number(row_out.get("proceeds"))
        cost_basis = _parse_number(row_out.get("cost_basis"))
        wash_sale_disallowed = _parse_number(row_out.get("wash_sale_disallowed")) or 0.0
        gain_or_loss = _parse_number(row_out.get("gain_or_loss"))
        term = normalize_term(row_out.get("term"), term_map=term_map) or "UNKNOWN"

        if gain_or_loss is None and proceeds is not None and cost_basis is not None:
            gain_or_loss = proceeds - cost_basis

        # If we only parsed sparse metadata with no reconciliation value, skip.
        has_numeric = any(
            value is not None
            for value in (proceeds, cost_basis, gain_or_loss, wash_sale_disallowed)
        )
        if not has_numeric and not description and not symbol and not date_sold:
            continue

        normalized_rows.append(
            {
                "description": description,
                "symbol": symbol,
                "date_acquired": date_acquired if isinstance(date_acquired, date) else None,
                "date_sold": date_sold if isinstance(date_sold, date) else None,
                "proceeds": proceeds,
                "cost_basis": cost_basis,
                "wash_sale_disallowed": float(wash_sale_disallowed or 0.0),
                "gain_or_loss": gain_or_loss,
                "term": term,
                "currency": str(normalized_mapping["currency"]).upper() or "USD",
                "broker": broker,
            }
        )

    return normalized_rows, issues


def normalize_broker_tax_records(
    df: pd.DataFrame,
    mapping: dict[str, Any] | None = None,
    *,
    broker: str = "generic",
) -> tuple[list[dict[str, Any]], list[str]]:
    return normalize_broker_export_records(df, mapping=mapping, broker=broker)


def normalize_broker_export_csv(
    file_obj: str | Path | BinaryIO,
    mapping: dict[str, Any] | None = None,
    *,
    broker: str = "generic",
) -> tuple[list[dict[str, Any]], list[str]]:
    df = pd.read_csv(file_obj)
    return normalize_broker_export_records(df, mapping=mapping, broker=broker)


def normalize_broker_tax_csv(
    file_obj: str | Path | BinaryIO,
    mapping: dict[str, Any] | None = None,
    *,
    broker: str = "generic",
) -> tuple[list[dict[str, Any]], list[str]]:
    return normalize_broker_export_csv(file_obj=file_obj, mapping=mapping, broker=broker)


def broker_export_totals(rows: list[dict[str, Any]]) -> dict[str, float]:
    totals = {
        "total_proceeds": 0.0,
        "total_cost_basis": 0.0,
        "total_gain_or_loss": 0.0,
        "short_term_gain_or_loss": 0.0,
        "long_term_gain_or_loss": 0.0,
        "total_wash_sale_disallowed": 0.0,
    }

    for row in rows:
        proceeds = parse_float(row.get("proceeds"), default=0.0) or 0.0
        cost_basis = parse_float(row.get("cost_basis"), default=0.0) or 0.0
        gain = parse_float(row.get("gain_or_loss"), default=0.0) or 0.0
        wash = parse_float(row.get("wash_sale_disallowed"), default=0.0) or 0.0
        term = normalize_term(row.get("term")) or "UNKNOWN"

        totals["total_proceeds"] += float(proceeds)
        totals["total_cost_basis"] += float(cost_basis)
        totals["total_gain_or_loss"] += float(gain)
        totals["total_wash_sale_disallowed"] += float(wash)

        if term == "ST":
            totals["short_term_gain_or_loss"] += float(gain)
        elif term == "LT":
            totals["long_term_gain_or_loss"] += float(gain)

    return totals


def summarize_broker_totals(rows: list[dict[str, Any]]) -> dict[str, float]:
    return broker_export_totals(rows)


def import_reconciliation_inputs(
    *,
    csv_file: str | Path | BinaryIO | None = None,
    csv_mapping: dict[str, Any] | None = None,
    pdf_file: str | Path | BinaryIO | bytes | None = None,
    broker: str = "generic",
) -> ReconciliationImportResult:
    issues: list[str] = []

    if csv_file is not None:
        try:
            csv_rows, csv_issues = normalize_broker_export_csv(
                csv_file,
                mapping=csv_mapping,
                broker=broker,
            )
            issues.extend(csv_issues)
            if csv_rows:
                return ReconciliationImportResult(
                    rows=csv_rows,
                    issues=issues,
                    source="csv",
                    used_pdf_fallback=False,
                    needs_review=False,
                )
        except Exception as exc:  # pragma: no cover - guardrail
            issues.append(f"CSV import failed: {exc}")

    if pdf_file is not None:
        from portfolio_assistant.ingest.pdf_import import import_broker_1099b_pdf

        pdf_result = import_broker_1099b_pdf(pdf_file, broker=broker)
        issues.extend(pdf_result.issues)
        return ReconciliationImportResult(
            rows=pdf_result.rows,
            issues=issues,
            source="pdf",
            used_pdf_fallback=True,
            needs_review=pdf_result.needs_review,
        )

    return ReconciliationImportResult(
        rows=[],
        issues=issues,
        source=None,
        used_pdf_fallback=False,
        needs_review=False,
    )


def import_broker_reconciliation_inputs(
    *,
    csv_file: str | Path | BinaryIO | None = None,
    csv_mapping: dict[str, Any] | None = None,
    pdf_file: str | Path | BinaryIO | bytes | None = None,
    broker: str = "generic",
) -> ReconciliationImportResult:
    return import_reconciliation_inputs(
        csv_file=csv_file,
        csv_mapping=csv_mapping,
        pdf_file=pdf_file,
        broker=broker,
    )


__all__ = [
    "BROKER_EXPORT_CANONICAL_FIELDS",
    "BROKER_EXPORT_REQUIRED_FIELDS",
    "BrokerExportPreview",
    "ReconciliationImportResult",
    "broker_export_totals",
    "import_broker_reconciliation_inputs",
    "import_reconciliation_inputs",
    "infer_broker_export_column_map",
    "infer_broker_tax_column_map",
    "load_broker_export_csv_preview",
    "load_broker_tax_csv_preview",
    "normalize_broker_export_csv",
    "normalize_broker_export_records",
    "normalize_broker_tax_csv",
    "normalize_broker_tax_records",
    "normalize_term",
    "summarize_broker_totals",
    "validate_broker_export_mapping",
    "validate_broker_tax_mapping",
]
