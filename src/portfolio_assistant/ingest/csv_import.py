from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Literal
import re

import pandas as pd

from portfolio_assistant.ingest.csv_mapping import (
    CASH_CANONICAL_FIELDS,
    CASH_REQUIRED_FIELDS,
    TRADE_CANONICAL_FIELDS,
    TRADE_REQUIRED_FIELDS,
    file_signature,
    infer_cash_column_map,
    infer_column_map,
    missing_required_fields,
    validate_mapping,
)
from portfolio_assistant.ingest.validators import (
    compute_signed_trade_cash,
    is_external_cash_guess,
    normalize_cash_type,
    normalize_instrument_type,
    normalize_side,
    parse_datetime,
    parse_float,
    parse_option_symbol,
)

_OPTION_INSTRUMENT_HINTS = {
    "OPTION",
    "OPTIONS",
    "OPT",
    "OPTION CONTRACT",
    "OPTIONS CONTRACT",
    "EQUITY OPTION",
    "CALL",
    "PUT",
    "DERIVATIVE",
}

_STOCK_INSTRUMENT_HINTS = {
    "STOCK",
    "EQUITY",
    "COMMON STOCK",
    "SHARE",
    "SHARES",
    "ETF",
}

_UNKNOWN_INSTRUMENT_HINTS = {"", "N/A", "NA", "--", "UNKNOWN", "UNSPECIFIED", "OTHER"}


ImportIssueSeverity = Literal["INFO", "WARNING", "ERROR"]
_IMPORT_ISSUE_SEVERITIES: tuple[ImportIssueSeverity, ...] = ("INFO", "WARNING", "ERROR")


# Import issue contract for UI/consumers:
# - Each issue string can be tagged with a leading severity token:
#   "[INFO] ...", "[WARNING] ...", "[ERROR] ..."
# - Legacy consumers can keep treating issues as plain strings.
# - Consumers that need severity should call `parse_import_issue`.
def _normalize_instrument_hint(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    tokens = [token for token in re.split(r"[^A-Z0-9]+", text) if token]
    return " ".join(tokens)


def _classify_instrument_hint(instrument_hint: str) -> str | None:
    if not instrument_hint:
        return None
    if instrument_hint in _OPTION_INSTRUMENT_HINTS:
        return "OPTION"
    if instrument_hint in _STOCK_INSTRUMENT_HINTS:
        return "STOCK"

    tokens = set(instrument_hint.split())
    if any(token.startswith("OPTION") for token in tokens) or {"CALL", "PUT"}.intersection(tokens):
        return "OPTION"
    if (
        any(token.startswith("STOCK") for token in tokens)
        or any(token.startswith("EQUIT") for token in tokens)
        or {"ETF", "SHARE", "SHARES"}.intersection(tokens)
    ):
        return "STOCK"
    return None


def make_import_issue(message: str, *, severity: ImportIssueSeverity = "WARNING") -> str:
    severity_text = severity.upper()
    if severity_text not in _IMPORT_ISSUE_SEVERITIES:
        severity_text = "WARNING"
    return f"[{severity_text}] {message}"


def parse_import_issue(issue: str) -> tuple[ImportIssueSeverity, str]:
    text = str(issue or "").strip()
    for severity in _IMPORT_ISSUE_SEVERITIES:
        prefix = f"[{severity}]"
        if text.upper().startswith(prefix):
            detail = text[len(prefix) :].lstrip(" :")
            return severity, detail or text
    # Backward-compatible default for old untagged issue text.
    return "WARNING", text


@dataclass(frozen=True)
class TradeImportPreview:
    columns: list[str]
    sample_rows: list[dict]
    mapping: dict[str, str]
    missing_required: list[str]
    signature: str


@dataclass(frozen=True)
class CashImportPreview:
    columns: list[str]
    sample_rows: list[dict]
    mapping: dict[str, str]
    missing_required: list[str]
    signature: str


def load_trade_csv_preview(
    file_obj: str | Path | BinaryIO,
    broker: str = "generic",
    max_rows: int = 200,
) -> TradeImportPreview:
    df = pd.read_csv(file_obj)
    columns = [str(c) for c in df.columns]
    mapping = infer_column_map(columns, broker=broker)
    missing_required = missing_required_fields(mapping, required_fields=TRADE_REQUIRED_FIELDS)

    sample = df.head(max_rows).fillna("").to_dict(orient="records")
    return TradeImportPreview(
        columns=columns,
        sample_rows=sample,
        mapping=mapping,
        missing_required=missing_required,
        signature=file_signature(columns),
    )


def apply_mapping(
    df: pd.DataFrame,
    mapping: dict[str, str],
    *,
    canonical_fields: list[str] | None = None,
    required_fields: list[str] | None = None,
) -> pd.DataFrame:
    cleaned_mapping, errors = validate_mapping(
        mapping,
        columns=[str(c) for c in df.columns],
        canonical_fields=canonical_fields,
        required_fields=required_fields,
    )
    if errors:
        raise ValueError("; ".join(errors))

    reverse_mapping = {source: target for target, source in cleaned_mapping.items()}
    selected_columns = [col for col in reverse_mapping if col in df.columns]
    out = df[selected_columns].rename(columns=reverse_mapping)
    return out


def load_cash_csv_preview(
    file_obj: str | Path | BinaryIO,
    broker: str = "generic",
    max_rows: int = 200,
) -> CashImportPreview:
    df = pd.read_csv(file_obj)
    columns = [str(c) for c in df.columns]
    mapping = infer_cash_column_map(columns, broker=broker)
    missing_required = missing_required_fields(mapping, required_fields=CASH_REQUIRED_FIELDS)
    sample = df.head(max_rows).fillna("").to_dict(orient="records")
    return CashImportPreview(
        columns=columns,
        sample_rows=sample,
        mapping=mapping,
        missing_required=missing_required,
        signature=file_signature(columns),
    )


def normalize_trade_records(
    df: pd.DataFrame,
    mapping: dict[str, str],
    account_id: str,
    broker: str,
    default_instrument_type: str | None = None,
) -> tuple[list[dict], list[str]]:
    normalized_rows: list[dict] = []
    issues: list[str] = []
    default_instrument = (default_instrument_type or "").strip().upper()
    if default_instrument not in {"STOCK", "OPTION"}:
        default_instrument = ""

    cleaned_mapping, mapping_errors = validate_mapping(
        mapping,
        columns=[str(c) for c in df.columns],
        canonical_fields=TRADE_CANONICAL_FIELDS,
        required_fields=TRADE_REQUIRED_FIELDS,
    )
    if mapping_errors:
        return [], [
            make_import_issue(f"Mapping error: {error}", severity="ERROR")
            for error in mapping_errors
        ]

    renamed = apply_mapping(
        df,
        cleaned_mapping,
        canonical_fields=TRADE_CANONICAL_FIELDS,
        required_fields=TRADE_REQUIRED_FIELDS,
    ).fillna("")
    for row_number, row_data in enumerate(renamed.to_dict(orient="records"), start=1):
        side = normalize_side(row_data.get("side"))
        symbol_value = str(row_data.get("symbol", "")).strip().upper() or None
        option_symbol_raw = str(row_data.get("option_symbol_raw", "")).strip() or None
        option_symbol_candidate = option_symbol_raw or symbol_value
        parsed_option = parse_option_symbol(option_symbol_candidate)
        instrument_value = row_data.get("instrument_type")
        instrument_hint = _normalize_instrument_hint(instrument_value)
        quantity = parse_float(row_data.get("quantity"), default=0.0) or 0.0
        quantity = abs(quantity)

        if quantity <= 0:
            issues.append(
                make_import_issue(
                    f"Row {row_number}: skipped non-filled row (quantity <= 0)",
                    severity="INFO",
                )
            )
            continue

        classified_instrument = _classify_instrument_hint(instrument_hint)
        if classified_instrument:
            instrument_value = classified_instrument
        elif default_instrument:
            instrument_value = default_instrument
        else:
            instrument_value = None

        # Contract for UI/consumers: when instrument type is neither mapped nor inferable,
        # rows are skipped with a warning so the user can choose an explicit Stock/Option default.
        if not instrument_value and side not in {"BTO", "STO", "BTC", "STC"} and not parsed_option:
            issues.append(
                make_import_issue(
                    f"Row {row_number}: instrument type is missing or ambiguous; choose Stock or Option as default.",
                    severity="WARNING",
                )
            )
            continue

        instrument_type = normalize_instrument_type(
            instrument_value,
            option_symbol_raw=option_symbol_candidate if parsed_option else None,
        )
        if side in {"BTO", "STO", "BTC", "STC"}:
            instrument_type = "OPTION"
        if parsed_option and instrument_type != "OPTION":
            instrument_type = "OPTION"

        executed_at = parse_datetime(row_data.get("executed_at"))
        price = parse_float(row_data.get("price"), default=0.0) or 0.0
        fees_raw = parse_float(row_data.get("fees"))
        total_cost = parse_float(row_data.get("total_cost"))
        multiplier_raw = parse_float(row_data.get("multiplier"), default=100.0)
        multiplier = int(multiplier_raw or 100)
        if fees_raw is None:
            fees = 0.0
            if total_cost is not None:
                effective_multiplier = multiplier if instrument_type == "OPTION" else 1
                gross_notional = quantity * price * effective_multiplier
                inferred_fees = abs(abs(total_cost) - gross_notional)
                if inferred_fees > 0.005:
                    fees = inferred_fees
        else:
            fees = abs(fees_raw)

        if parsed_option and not option_symbol_raw:
            option_symbol_raw = option_symbol_candidate

        underlying = parsed_option.get("underlying") or symbol_value
        expiration = parsed_option.get("expiration")
        strike = parsed_option.get("strike")
        call_put = parsed_option.get("call_put")

        symbol = underlying if instrument_type == "OPTION" else (symbol_value or underlying)

        if instrument_type == "OPTION" and not option_symbol_raw and underlying and expiration:
            option_symbol_raw = (
                f"{underlying} {expiration.strftime('%Y-%m-%d')} {strike} {call_put}"
            )

        net_amount = parse_float(row_data.get("net_amount"))
        if net_amount is None:
            net_amount = compute_signed_trade_cash(
                side=side,
                quantity=quantity,
                price=price,
                fees=fees,
                multiplier=multiplier if instrument_type == "OPTION" else 1,
            )

        if instrument_type == "STOCK" and side in {"BTO", "BTC"}:
            side = "BUY"
        elif instrument_type == "STOCK" and side in {"STO", "STC"}:
            side = "SELL"

        if executed_at is None:
            issues.append(
                make_import_issue(f"Row {row_number}: invalid executed_at", severity="WARNING")
            )
            continue
        if not side:
            issues.append(make_import_issue(f"Row {row_number}: missing side", severity="WARNING"))
            continue
        if instrument_type == "STOCK" and side not in {"BUY", "SELL"}:
            issues.append(
                make_import_issue(
                    f"Row {row_number}: invalid stock side '{side}'",
                    severity="WARNING",
                )
            )
            continue
        if instrument_type == "OPTION" and side not in {"BUY", "SELL", "BTO", "STO", "BTC", "STC"}:
            issues.append(
                make_import_issue(
                    f"Row {row_number}: invalid option side '{side}'",
                    severity="WARNING",
                )
            )
            continue
        if price < 0:
            issues.append(
                make_import_issue(
                    f"Row {row_number}: price cannot be negative",
                    severity="WARNING",
                )
            )
            continue
        if instrument_type == "OPTION" and multiplier <= 0:
            issues.append(
                make_import_issue(
                    f"Row {row_number}: option multiplier must be > 0",
                    severity="WARNING",
                )
            )
            continue
        if not symbol and not underlying:
            issues.append(
                make_import_issue(
                    f"Row {row_number}: symbol/underlying missing",
                    severity="WARNING",
                )
            )
            continue

        normalized_rows.append(
            {
                "account_id": account_id,
                "broker": broker,
                "trade_id": str(row_data.get("trade_id", "")).strip() or None,
                "executed_at": executed_at,
                "instrument_type": instrument_type,
                "symbol": symbol or underlying,
                "side": side,
                "option_symbol_raw": option_symbol_raw,
                "underlying": underlying,
                "expiration": expiration,
                "strike": strike,
                "call_put": call_put,
                "multiplier": multiplier if instrument_type == "OPTION" else 1,
                "quantity": quantity,
                "price": price,
                "fees": fees,
                "net_amount": net_amount,
                "currency": str(row_data.get("currency", "")).strip().upper() or "USD",
            }
        )

    return normalized_rows, issues


def normalize_cash_records(
    df: pd.DataFrame, mapping: dict[str, str], account_id: str, broker: str
) -> tuple[list[dict], list[str]]:
    rows: list[dict] = []
    issues: list[str] = []

    cleaned_mapping, mapping_errors = validate_mapping(
        mapping,
        columns=[str(c) for c in df.columns],
        canonical_fields=CASH_CANONICAL_FIELDS,
        required_fields=CASH_REQUIRED_FIELDS,
    )
    if mapping_errors:
        return [], [
            make_import_issue(f"Mapping error: {error}", severity="ERROR")
            for error in mapping_errors
        ]

    renamed = apply_mapping(
        df,
        cleaned_mapping,
        canonical_fields=CASH_CANONICAL_FIELDS,
        required_fields=CASH_REQUIRED_FIELDS,
    ).fillna("")
    for row_number, row_data in enumerate(renamed.to_dict(orient="records"), start=1):
        posted_at = parse_datetime(row_data.get("posted_at"))
        raw_amount = parse_float(row_data.get("amount"))
        if raw_amount is None:
            issues.append(
                make_import_issue(
                    f"Cash row {row_number}: invalid amount",
                    severity="WARNING",
                )
            )
            continue

        amount = abs(raw_amount)
        activity_type = normalize_cash_type(row_data.get("activity_type"), amount=raw_amount)
        description = str(row_data.get("description", "")).strip()
        source = str(row_data.get("source", "")).strip() or None
        is_external = is_external_cash_guess(description=description, source=source)

        if posted_at is None:
            issues.append(
                make_import_issue(
                    f"Cash row {row_number}: invalid posted_at",
                    severity="WARNING",
                )
            )
            continue
        if amount <= 0:
            issues.append(
                make_import_issue(
                    f"Cash row {row_number}: amount must be > 0",
                    severity="WARNING",
                )
            )
            continue

        rows.append(
            {
                "account_id": account_id,
                "broker": broker,
                "posted_at": posted_at,
                "activity_type": activity_type,
                "amount": amount,
                "description": description,
                "source": source,
                "is_external": is_external,
            }
        )

    return rows, issues
