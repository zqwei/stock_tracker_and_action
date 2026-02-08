"""Runtime settings loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from .paths import default_db_path


@dataclass(frozen=True)
class Settings:
    app_name: str = "Portfolio Assistant"
    db_path: Path = default_db_path()
    default_currency: str = "USD"
    wash_sale_window_days: int = 30


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    db_override = os.getenv("PORTFOLIO_ASSISTANT_DB_PATH")
    db_path = Path(db_override).expanduser() if db_override else default_db_path()
    return Settings(db_path=db_path)
