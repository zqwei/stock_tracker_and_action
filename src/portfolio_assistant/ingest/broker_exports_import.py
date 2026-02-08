"""Import helper for broker gain/loss export files used in reconciliation."""

from __future__ import annotations

import csv
from pathlib import Path


def import_broker_realized_export(csv_path: str | Path) -> list[dict[str, str]]:
    with Path(csv_path).open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))
