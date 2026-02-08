"""PDF tax import placeholder.

CSV imports are authoritative for MVP; PDF parsing is intentionally deferred.
"""

from __future__ import annotations

from pathlib import Path


def extract_1099b_rows(pdf_path: str | Path) -> list[dict[str, str]]:
    _ = Path(pdf_path)
    return []
