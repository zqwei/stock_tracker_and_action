from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import pandas as pd

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
]

OCC_OPTION_RE = re.compile(r"^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")
SIMPLE_OPTION_RE = re.compile(
    r"^([A-Z.\-]{1,10})\s+(\d{4}-\d{2}-\d{2})\s+(\d+(?:\.\d+)?)\s*([CP])$"
)


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    parsed = pd.to_datetime(text, errors="coerce", utc=False)
    if pd.notna(parsed):
        if isinstance(parsed, pd.Timestamp):
            return parsed.to_pydatetime()

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    text = str(value).strip().replace(",", "").replace("$", "")
    if text == "":
        return default
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    try:
        return float(text)
    except ValueError:
        return default


def normalize_instrument_type(value: Any, option_symbol_raw: str | None = None) -> str:
    text = (str(value).strip().upper() if value is not None else "")
    if text in {"OPTION", "OPT", "OPTIONS"}:
        return "OPTION"
    if text in {"STOCK", "EQUITY"}:
        return "STOCK"
    if option_symbol_raw:
        return "OPTION"
    return "STOCK"


def normalize_side(value: Any) -> str:
    text = str(value).strip().upper()
    aliases = {
        "BUY": "BUY",
        "B": "BUY",
        "SELL": "SELL",
        "S": "SELL",
        "BUY TO OPEN": "BTO",
        "BTO": "BTO",
        "SELL TO OPEN": "STO",
        "STO": "STO",
        "BUY TO CLOSE": "BTC",
        "BTC": "BTC",
        "SELL TO CLOSE": "STC",
        "STC": "STC",
    }
    return aliases.get(text, text)


def normalize_cash_type(value: Any, amount: float | None = None) -> str:
    text = str(value).strip().upper()
    if text in {"DEPOSIT", "CREDIT", "IN"}:
        return "DEPOSIT"
    if text in {"WITHDRAWAL", "DEBIT", "OUT"}:
        return "WITHDRAWAL"
    if amount is not None:
        return "DEPOSIT" if amount >= 0 else "WITHDRAWAL"
    return "DEPOSIT"


def parse_option_symbol(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    text = str(raw).strip().upper()
    if not text:
        return {}

    m_occ = OCC_OPTION_RE.match(text)
    if m_occ:
        underlying, yy, mm, dd, cp, strike_raw = m_occ.groups()
        expiration = datetime.strptime(f"20{yy}-{mm}-{dd}", "%Y-%m-%d")
        strike = int(strike_raw) / 1000
        return {
            "underlying": underlying.strip(),
            "expiration": expiration,
            "strike": strike,
            "call_put": cp,
        }

    m_simple = SIMPLE_OPTION_RE.match(text)
    if m_simple:
        underlying, expiration_text, strike_text, cp = m_simple.groups()
        expiration = datetime.strptime(expiration_text, "%Y-%m-%d")
        return {
            "underlying": underlying.strip(),
            "expiration": expiration,
            "strike": float(strike_text),
            "call_put": cp,
        }

    return {}


def compute_signed_trade_cash(
    side: str, quantity: float, price: float, fees: float, multiplier: int
) -> float:
    notional = quantity * price * multiplier
    if side in {"BUY", "BTO", "BTC"}:
        return -(notional + fees)
    return notional - fees


def is_external_cash_guess(description: Any, source: Any) -> bool | None:
    text = f"{description or ''} {source or ''}".strip().lower()
    if not text:
        return None

    external_keywords = [
        "ach",
        "bank",
        "wire",
        "deposit",
        "withdraw",
        "external",
        "payroll",
    ]
    internal_keywords = [
        "internal",
        "journal",
        "between accounts",
        "broker transfer",
    ]

    if any(keyword in text for keyword in internal_keywords):
        return False
    if any(keyword in text for keyword in external_keywords):
        return True
    return None
