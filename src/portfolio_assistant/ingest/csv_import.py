"""CSV import pipeline for trade and cash activity data."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from portfolio_assistant.db.models import Account, InstrumentType, Trade
from portfolio_assistant.ingest.csv_mapping import MappingStore, header_signature, infer_mapping, unmapped_required_fields
from portfolio_assistant.ingest.validators import normalize_instrument_type, normalize_side
from portfolio_assistant.utils.dates import parse_datetime
from portfolio_assistant.utils.money import round_money, signed_cash_inflow


@dataclass(slots=True)
class ImportResult:
    trades: list[Trade]
    mapping: dict[str, str]
    unmapped_required: list[str]
    headers: list[str]
    raw_rows: list[dict[str, str]]
    signature: str


def _to_float(value: str | None, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip().replace(",", "")
    if not text:
        return default
    return float(text)


def _read_rows(csv_path: str | Path) -> tuple[list[dict[str, str]], list[str]]:
    path = Path(csv_path)
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        headers = reader.fieldnames or []
    return rows, headers


def import_trades_csv(
    csv_path: str | Path,
    account: Account,
    broker_name: str | None = None,
    mapping_store: MappingStore | None = None,
    mapping_override: dict[str, str] | None = None,
) -> ImportResult:
    rows, headers = _read_rows(csv_path)
    signature = header_signature(headers)
    broker = broker_name or account.broker
    store = mapping_store or MappingStore.default()

    mapping = mapping_override or store.get(broker, signature) or infer_mapping(headers)
    missing_required = unmapped_required_fields(mapping)

    if not missing_required:
        store.put(broker, signature, mapping)

    trades: list[Trade] = []
    for row in rows:
        if missing_required:
            break
        side = normalize_side(row[mapping["side"]])
        quantity = _to_float(row.get(mapping["quantity"]))
        price = _to_float(row.get(mapping["price"]))
        instrument_type = normalize_instrument_type(row.get(mapping.get("instrument_type", "")))
        multiplier = int(_to_float(row.get(mapping.get("multiplier")), default=100)) if instrument_type == InstrumentType.OPTION else 1
        notional = quantity * price * multiplier
        fees = _to_float(row.get(mapping.get("fees")), default=0.0)

        net_amount_raw = row.get(mapping["net_amount"]) if "net_amount" in mapping else None
        net_amount = _to_float(net_amount_raw, default=0.0) if net_amount_raw not in {None, ""} else signed_cash_inflow(side.value, notional, fees)

        trade = Trade(
            broker=broker,
            account_id=account.account_id,
            account_type=account.account_type,
            account_label=account.account_label,
            trade_id=row.get(mapping.get("trade_id")) if "trade_id" in mapping else None,
            executed_at=parse_datetime(row[mapping["executed_at"]]),
            instrument_type=instrument_type,
            symbol=(row.get(mapping.get("symbol")) or "").strip().upper(),
            side=side,
            quantity=quantity,
            price=price,
            fees=round_money(fees),
            net_amount=round_money(net_amount),
            currency=(row.get(mapping.get("currency")) or "USD").upper(),
            option_symbol_raw=row.get(mapping.get("option_symbol_raw")),
            underlying=(row.get(mapping.get("underlying")) or "").strip().upper() or None,
            expiration=row.get(mapping.get("expiration")) or None,
            strike=_to_float(row.get(mapping.get("strike")), default=0.0) or None,
            call_put=(row.get(mapping.get("call_put")) or "").upper() or None,
            multiplier=multiplier,
        )
        trades.append(trade)

    trades.sort(key=lambda item: item.executed_at)
    return ImportResult(
        trades=trades,
        mapping=mapping,
        unmapped_required=missing_required,
        headers=headers,
        raw_rows=rows,
        signature=signature,
    )
