"""Column mapping helpers for broker CSV normalization."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from portfolio_assistant.config.paths import data_dir


CANONICAL_FIELDS = {
    "trade_id": {"trade_id", "id", "order_id"},
    "executed_at": {"executed_at", "date", "datetime", "trade_date", "filled_at"},
    "instrument_type": {"instrument_type", "asset_type", "type", "security_type"},
    "symbol": {"symbol", "ticker", "underlying"},
    "side": {"side", "action", "buy_sell"},
    "quantity": {"quantity", "qty", "shares", "contracts"},
    "price": {"price", "fill_price", "trade_price"},
    "fees": {"fees", "fee", "commission", "commissions"},
    "net_amount": {"net_amount", "net", "amount", "cash_amount"},
    "currency": {"currency", "ccy"},
    "option_symbol_raw": {"option_symbol", "option_symbol_raw", "contract"},
    "underlying": {"underlying", "option_underlying"},
    "expiration": {"expiration", "expiry", "expiration_date"},
    "strike": {"strike", "strike_price"},
    "call_put": {"call_put", "cp", "put_call", "right"},
    "multiplier": {"multiplier", "contract_multiplier"},
}


@dataclass(slots=True)
class MappingStore:
    path: Path

    @classmethod
    def default(cls) -> "MappingStore":
        return cls(path=data_dir() / "mapping_store.json")

    def _load(self) -> dict[str, dict[str, dict[str, str]]]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _save(self, payload: dict[str, dict[str, dict[str, str]]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def get(self, broker: str, signature: str) -> dict[str, str] | None:
        payload = self._load()
        return payload.get(broker, {}).get(signature)

    def put(self, broker: str, signature: str, mapping: dict[str, str]) -> None:
        payload = self._load()
        payload.setdefault(broker, {})[signature] = mapping
        self._save(payload)


def header_signature(headers: list[str]) -> str:
    canonical = "|".join(h.strip().lower() for h in headers)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def infer_mapping(headers: list[str]) -> dict[str, str]:
    """Infer canonical->source mapping from likely header aliases."""
    normalized = {h.strip().lower(): h for h in headers}
    mapping: dict[str, str] = {}
    for canonical, aliases in CANONICAL_FIELDS.items():
        for alias in aliases:
            if alias in normalized:
                mapping[canonical] = normalized[alias]
                break
    return mapping


def unmapped_required_fields(mapping: dict[str, str]) -> list[str]:
    required = ["executed_at", "symbol", "side", "quantity", "price"]
    return [field for field in required if field not in mapping]
