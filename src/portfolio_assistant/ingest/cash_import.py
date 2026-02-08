"""CSV import pipeline for cash activity and contribution tagging."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from portfolio_assistant.db.models import Account, CashActivity
from portfolio_assistant.ingest.csv_mapping import MappingStore, header_signature
from portfolio_assistant.utils.dates import parse_datetime


CASH_FIELD_ALIASES = {
    "posted_at": {"posted_at", "date", "datetime", "transaction_date"},
    "type": {"type", "activity", "transaction_type", "action"},
    "amount": {"amount", "net_amount", "value", "cash_amount"},
    "description": {"description", "memo", "details"},
    "source": {"source", "method", "channel"},
    "is_external": {"is_external", "external"},
    "transfer_group_id": {"transfer_group_id", "transfer_id", "group_id"},
}


@dataclass(slots=True)
class CashImportResult:
    activities: list[CashActivity]
    mapping: dict[str, str]
    unmapped_required: list[str]
    headers: list[str]
    raw_rows: list[dict[str, str]]
    signature: str


def _read_rows(csv_path: str | Path) -> tuple[list[dict[str, str]], list[str]]:
    with Path(csv_path).open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        headers = reader.fieldnames or []
    return rows, headers


def _to_float(value: str | None, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip().replace(",", "")
    if not text:
        return default
    return float(text)


def _infer_mapping(headers: list[str]) -> dict[str, str]:
    normalized = {h.strip().lower(): h for h in headers}
    mapping: dict[str, str] = {}
    for canonical, aliases in CASH_FIELD_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                mapping[canonical] = normalized[alias]
                break
    return mapping


def _normalize_type(raw_type: str | None, amount: float) -> str:
    token = (raw_type or "").strip().upper()
    if token in {"DEPOSIT", "CREDIT", "INCOMING", "ACH_IN"}:
        return "DEPOSIT"
    if token in {"WITHDRAWAL", "DEBIT", "OUTGOING", "ACH_OUT"}:
        return "WITHDRAWAL"
    return "DEPOSIT" if amount >= 0 else "WITHDRAWAL"


def _guess_external(description: str, source: str) -> bool:
    haystack = f"{description} {source}".lower()
    internal_tokens = {"internal transfer", "journal", "between accounts", "sweep"}
    external_tokens = {"ach", "bank", "wire", "deposit", "withdraw"}

    if any(token in haystack for token in internal_tokens):
        return False
    if any(token in haystack for token in external_tokens):
        return True
    return True


def import_cash_csv(
    csv_path: str | Path,
    account: Account,
    broker_name: str | None = None,
    mapping_store: MappingStore | None = None,
    mapping_override: dict[str, str] | None = None,
) -> CashImportResult:
    rows, headers = _read_rows(csv_path)
    signature = header_signature(headers)
    broker = broker_name or account.broker
    store = mapping_store or MappingStore.default()

    key = f"cash:{signature}"
    mapping = mapping_override or store.get(broker, key) or _infer_mapping(headers)

    required = ["posted_at", "amount"]
    missing_required = [field for field in required if field not in mapping]
    if not missing_required:
        store.put(broker, key, mapping)

    activities: list[CashActivity] = []
    for row in rows:
        if missing_required:
            break

        raw_amount = _to_float(row.get(mapping["amount"]), default=0.0)
        cash_type = _normalize_type(row.get(mapping.get("type")), raw_amount)
        amount = abs(raw_amount)
        description = row.get(mapping.get("description"), "") if "description" in mapping else ""
        source = row.get(mapping.get("source"), "") if "source" in mapping else ""

        if "is_external" in mapping:
            raw_external = (row.get(mapping["is_external"], "") or "").strip().lower()
            is_external = raw_external not in {"0", "false", "no", "n"}
        else:
            is_external = _guess_external(description, source)

        activities.append(
            CashActivity(
                broker=broker,
                account_id=account.account_id,
                account_type=account.account_type,
                posted_at=parse_datetime(row[mapping["posted_at"]]),
                type=cash_type,
                amount=amount,
                description=description,
                source=source,
                is_external=is_external,
                transfer_group_id=row.get(mapping.get("transfer_group_id")) if "transfer_group_id" in mapping else None,
            )
        )

    activities.sort(key=lambda item: item.posted_at)
    return CashImportResult(
        activities=activities,
        mapping=mapping,
        unmapped_required=missing_required,
        headers=headers,
        raw_rows=rows,
        signature=signature,
    )
