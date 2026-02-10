from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, BinaryIO, Iterable

import pandas as pd

from portfolio_assistant.ingest.broker_exports_import import normalize_term
from portfolio_assistant.ingest.validators import parse_datetime, parse_float

_DATE_PATTERN = re.compile(
    r"\b(?:\d{1,2}/\d{1,2}/\d{2,4}|\d{1,2}-\d{1,2}-\d{2,4}|\d{4}-\d{2}-\d{2})\b"
)
_MONEY_PATTERN = re.compile(r"\(?-?\$?\d[\d,]*(?:\.\d+)?\)?")
_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z.\-]{0,9}$")


@dataclass(frozen=True)
class PdfImportResult:
    rows: list[dict[str, Any]]
    issues: list[str]
    needs_review: bool
    source: str


def _read_binary_payload(file_obj: str | Path | BinaryIO | bytes) -> bytes:
    if isinstance(file_obj, bytes):
        return file_obj
    if isinstance(file_obj, (str, Path)):
        return Path(file_obj).read_bytes()
    if hasattr(file_obj, "read"):
        payload = file_obj.read()
        if isinstance(payload, str):
            return payload.encode("utf-8", errors="ignore")
        return payload
    raise TypeError("Unsupported PDF input type.")


def _parse_date_token(token: str) -> date | None:
    parsed = parse_datetime(token)
    if parsed is None:
        return None
    return parsed.date()


def _parse_money_token(token: str) -> float | None:
    text = token.strip()
    if not text:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    text = text.replace("$", "").replace(",", "").strip()
    if text.startswith("-"):
        negative = True
        text = text[1:]
    parsed = parse_float(text, default=None)
    if parsed is None:
        return None
    return -abs(parsed) if negative else float(parsed)


def _infer_symbol_from_description(description: str | None) -> str | None:
    if not description:
        return None
    first_token = description.split()[0].upper()
    if _SYMBOL_PATTERN.match(first_token):
        return first_token
    return None


def _extract_term_from_line(line: str) -> str:
    uppercase = line.upper()
    if "SHORT" in uppercase or re.search(r"\bST\b", uppercase):
        return "ST"
    if "LONG" in uppercase or re.search(r"\bLT\b", uppercase):
        return "LT"
    return "UNKNOWN"


def _parse_1099b_line(line: str, *, broker: str) -> dict[str, Any] | None:
    compact = " ".join(line.strip().split())
    if not compact:
        return None

    header_like = {"description", "date sold", "proceeds", "cost basis", "gain/loss"}
    lowered = compact.lower()
    if all(fragment in lowered for fragment in header_like):
        return None

    date_matches = list(_DATE_PATTERN.finditer(compact))
    money_matches = list(_MONEY_PATTERN.finditer(compact))
    if len(money_matches) < 2:
        return None
    if not date_matches:
        return None

    tail_start = date_matches[-1].end()
    filtered_money_matches = [match for match in money_matches if match.start() >= tail_start]
    if len(filtered_money_matches) >= 2:
        money_matches = filtered_money_matches

    first_data_offset = min(date_matches[0].start(), money_matches[0].start())
    description = compact[:first_data_offset].strip(" -|,") or None

    acquired: date | None = None
    sold: date | None = None
    if len(date_matches) >= 2:
        acquired = _parse_date_token(date_matches[0].group(0))
        sold = _parse_date_token(date_matches[1].group(0))
    else:
        sold = _parse_date_token(date_matches[0].group(0))

    money_tokens = [match.group(0) for match in money_matches]
    parsed_money = [_parse_money_token(token) for token in money_tokens]
    if len(parsed_money) < 2:
        return None

    proceeds = parsed_money[0]
    cost_basis = parsed_money[1]
    wash_sale = 0.0
    gain_or_loss: float | None = None
    if len(parsed_money) >= 4:
        wash_sale = float(parsed_money[2] or 0.0)
        gain_or_loss = parsed_money[-1]
    elif len(parsed_money) == 3:
        gain_or_loss = parsed_money[2]
    elif proceeds is not None and cost_basis is not None:
        gain_or_loss = proceeds - cost_basis

    if proceeds is None and cost_basis is None and gain_or_loss is None:
        return None

    symbol = _infer_symbol_from_description(description)
    term = normalize_term(_extract_term_from_line(compact)) or "UNKNOWN"
    return {
        "description": description,
        "symbol": symbol,
        "date_acquired": acquired,
        "date_sold": sold,
        "proceeds": proceeds,
        "cost_basis": cost_basis,
        "wash_sale_disallowed": float(wash_sale),
        "gain_or_loss": gain_or_loss,
        "term": term,
        "currency": "USD",
        "broker": broker,
    }


def extract_1099b_rows_from_text(
    text_pages: Iterable[str],
    *,
    broker: str = "generic",
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    issues: list[str] = []
    for page_index, page_text in enumerate(text_pages, start=1):
        if not page_text:
            continue
        for line_index, line in enumerate(str(page_text).splitlines(), start=1):
            row = _parse_1099b_line(line, broker=broker)
            if row is None:
                continue
            if row.get("date_sold") is None:
                issues.append(
                    f"Page {page_index} line {line_index}: parsed row missing date_sold."
                )
            rows.append(row)
    return rows, issues


def _extract_with_pdfplumber(
    payload: bytes,
    *,
    broker: str,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return [], [], ["pdfplumber is unavailable; using text fallback parser."]

    rows: list[dict[str, Any]] = []
    text_pages: list[str] = []
    issues: list[str] = []

    try:
        with pdfplumber.open(io.BytesIO(payload)) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                if page_text:
                    text_pages.append(page_text)

                try:
                    tables = page.extract_tables() or []
                except Exception as exc:
                    issues.append(f"Page {page_index}: table extraction failed ({exc}).")
                    tables = []

                for table_index, table in enumerate(tables, start=1):
                    if not table or len(table) < 2:
                        continue
                    header = [str(cell or "").strip() for cell in table[0]]
                    if not any(header):
                        continue
                    records: list[dict[str, Any]] = []
                    for row in table[1:]:
                        if row is None:
                            continue
                        record: dict[str, Any] = {}
                        for idx, raw_cell in enumerate(row):
                            key = header[idx] if idx < len(header) else f"col_{idx}"
                            if not key:
                                key = f"col_{idx}"
                            record[key] = raw_cell
                        if any(str(value or "").strip() for value in record.values()):
                            records.append(record)
                    if not records:
                        continue

                    from portfolio_assistant.ingest.broker_exports_import import (
                        normalize_broker_export_records,
                    )

                    normalized, row_issues = normalize_broker_export_records(
                        pd.DataFrame(records),
                        mapping=None,
                        broker=broker,
                    )
                    rows.extend(normalized)
                    for issue in row_issues:
                        issues.append(
                            f"Page {page_index} table {table_index}: {issue}"
                        )
    except Exception as exc:
        return [], [], [f"pdfplumber failed to read PDF ({exc}); using text fallback parser."]

    return rows, text_pages, issues


def import_broker_1099b_pdf(
    file_obj: str | Path | BinaryIO | bytes,
    *,
    broker: str = "generic",
) -> PdfImportResult:
    payload = _read_binary_payload(file_obj)
    rows: list[dict[str, Any]] = []
    issues: list[str] = []

    table_rows, text_pages, extraction_issues = _extract_with_pdfplumber(payload, broker=broker)
    issues.extend(extraction_issues)
    rows.extend(table_rows)
    if rows:
        return PdfImportResult(
            rows=rows,
            issues=issues,
            needs_review=True,
            source="pdf",
        )

    if not text_pages:
        decoded = payload.decode("latin-1", errors="ignore")
        text_pages = [decoded]

    text_rows, text_issues = extract_1099b_rows_from_text(text_pages, broker=broker)
    rows.extend(text_rows)
    issues.extend(text_issues)
    return PdfImportResult(
        rows=rows,
        issues=issues,
        needs_review=True,
        source="pdf",
    )


def import_broker_tax_pdf(
    file_obj: str | Path | BinaryIO | bytes,
    *,
    broker: str = "generic",
) -> PdfImportResult:
    return import_broker_1099b_pdf(file_obj=file_obj, broker=broker)


__all__ = [
    "PdfImportResult",
    "extract_1099b_rows_from_text",
    "import_broker_1099b_pdf",
    "import_broker_tax_pdf",
]
