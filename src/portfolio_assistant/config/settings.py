from __future__ import annotations

import os
from dataclasses import dataclass

from portfolio_assistant.config.paths import DATA_DIR


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    app_env: str
    database_url: str
    openai_model: str
    enable_web_mode: bool
    enable_daily_briefing: bool


def get_settings() -> Settings:
    db_default = f"sqlite:///{(DATA_DIR / 'portfolio_assistant.db').as_posix()}"
    return Settings(
        app_env=os.getenv("APP_ENV", "development"),
        database_url=os.getenv("DATABASE_URL", db_default),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
        enable_web_mode=_env_bool("ENABLE_WEB_MODE", False),
        enable_daily_briefing=_env_bool("ENABLE_DAILY_BRIEFING", True),
    )
