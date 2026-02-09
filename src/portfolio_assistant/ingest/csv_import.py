from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

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
    df: pd.DataFrame, mapping: dict[str, str], account_id: str, broker: str
) -> tuple[list[dict], list[str]]:
    normalized_rows: list[dict] = []
    issues: list[str] = []

    cleaned_mapping, mapping_errors = validate_mapping(
        mapping,
        columns=[str(c) for c in df.columns],
        canonical_fields=TRADE_CANONICAL_FIELDS,
        required_fields=TRADE_REQUIRED_FIELDS,
    )
    if mapping_errors:
        return [], [f"Mapping error: {error}" for error in mapping_errors]

    renamed = apply_mapping(
        df,
        cleaned_mapping,
        canonical_fields=TRADE_CANONICAL_FIELDS,
        required_fields=TRADE_REQUIRED_FIELDS,
    ).fillna("")
    for row_number, row_data in enumerate(renamed.to_dict(orient="records"), start=1):
        executed_at = parse_datetime(row_data.get("executed_at"))
        side = normalize_side(row_data.get("side"))

        option_symbol_raw = str(row_data.get("option_symbol_raw", "")).strip() or None
        instrument_type = normalize_instrument_type(
            row_data.get("instrument_type"), option_symbol_raw=option_symbol_raw
        )
        quantity = parse_float(row_data.get("quantity"), default=0.0) or 0.0
        quantity = abs(quantity)
        price = parse_float(row_data.get("price"), default=0.0) or 0.0
        fees = parse_float(row_data.get("fees"), default=0.0) or 0.0
        multiplier_raw = parse_float(row_data.get("multiplier"), default=100.0)
        multiplier = int(multiplier_raw or 100)

        symbol = str(row_data.get("symbol", "")).strip().upper() or None
        parsed_option = parse_option_symbol(option_symbol_raw)
        underlying = parsed_option.get("underlying") or symbol
        expiration = parsed_option.get("expiration")
        strike = parsed_option.get("strike")
        call_put = parsed_option.get("call_put")

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
            issues.append(f"Row {row_number}: invalid executed_at")
            continue
        if not side:
            issues.append(f"Row {row_number}: missing side")
            continue
        if instrument_type == "STOCK" and side not in {"BUY", "SELL"}:
            issues.append(f"Row {row_number}: invalid stock side '{side}'")
            continue
        if instrument_type == "OPTION" and side not in {"BUY", "SELL", "BTO", "STO", "BTC", "STC"}:
            issues.append(f"Row {row_number}: invalid option side '{side}'")
            continue
        if quantity <= 0:
            issues.append(f"Row {row_number}: quantity must be > 0")
            continue
        if price < 0:
            issues.append(f"Row {row_number}: price cannot be negative")
            continue
        if instrument_type == "OPTION" and multiplier <= 0:
            issues.append(f"Row {row_number}: option multiplier must be > 0")
            continue
        if not symbol and not underlying:
            issues.append(f"Row {row_number}: symbol/underlying missing")
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
        return [], [f"Mapping error: {error}" for error in mapping_errors]

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
            issues.append(f"Cash row {row_number}: invalid amount")
            continue

        amount = abs(raw_amount)
        activity_type = normalize_cash_type(row_data.get("activity_type"), amount=raw_amount)
        description = str(row_data.get("description", "")).strip()
        source = str(row_data.get("source", "")).strip() or None
        is_external = is_external_cash_guess(description=description, source=source)

        if posted_at is None:
            issues.append(f"Cash row {row_number}: invalid posted_at")
            continue
        if amount <= 0:
            issues.append(f"Cash row {row_number}: amount must be > 0")
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
