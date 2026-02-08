"""Path helpers for local-first storage."""

from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]
SRC_DIR = ROOT_DIR / "src"
DEFAULT_DATA_DIR = ROOT_DIR / "data"


def data_dir() -> Path:
    override = os.getenv("PORTFOLIO_ASSISTANT_DATA_DIR")
    directory = Path(override).expanduser() if override else DEFAULT_DATA_DIR
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def default_db_path() -> Path:
    return data_dir() / "portfolio_assistant.sqlite"
