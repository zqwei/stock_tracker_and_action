from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from portfolio_assistant.config.paths import ensure_data_dirs
from portfolio_assistant.config.settings import get_settings
from portfolio_assistant.db.models import Base


def build_engine(database_url: str | None = None) -> Engine:
    settings = get_settings()
    url = database_url or settings.database_url

    if url.startswith("sqlite:///"):
        sqlite_path = Path(url.removeprefix("sqlite:///")).expanduser()
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    return create_engine(url, future=True)


def migrate(database_url: str | None = None) -> Engine:
    ensure_data_dirs()
    engine = build_engine(database_url=database_url)
    Base.metadata.create_all(bind=engine)
    return engine


if __name__ == "__main__":
    migrate()
