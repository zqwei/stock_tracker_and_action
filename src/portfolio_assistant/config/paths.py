from __future__ import annotations

import os
from pathlib import Path

# paths.py -> config -> portfolio_assistant -> src -> repo root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"

DATA_DIR = Path(os.getenv("PA_DATA_DIR", PROJECT_ROOT / "data")).expanduser()
IMPORTS_DIR = Path(os.getenv("PA_IMPORTS_DIR", DATA_DIR / "imports")).expanduser()
PRIVATE_DIR = Path(os.getenv("PA_PRIVATE_DIR", DATA_DIR / "private")).expanduser()
EXPORTS_DIR = Path(os.getenv("PA_EXPORTS_DIR", DATA_DIR / "exports")).expanduser()
BACKUP_DIR = Path(
    os.getenv("PA_BACKUP_DIR", "~/Dropbox/portfolio_assistant_backups")
).expanduser()


def ensure_data_dirs() -> None:
    """Ensure required local data directories exist."""
    for directory in (DATA_DIR, IMPORTS_DIR, PRIVATE_DIR, EXPORTS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
