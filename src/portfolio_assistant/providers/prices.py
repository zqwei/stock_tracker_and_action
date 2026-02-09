from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import PriceCache


class PriceProvider:
    """Simple local price provider backed by price_cache."""

    def get_quote(self, session: Session, symbol: str) -> float | None:
        stmt = (
            select(PriceCache.close)
            .where(PriceCache.symbol == symbol)
            .order_by(PriceCache.as_of.desc())
            .limit(1)
        )
        return session.scalar(stmt)

    def upsert_quote(
        self, session: Session, symbol: str, close: float, as_of: datetime | None = None
    ) -> PriceCache:
        as_of = as_of or datetime.now(timezone.utc).replace(tzinfo=None)
        row = PriceCache(symbol=symbol.upper(), close=close, as_of=as_of, interval="1d")
        session.add(row)
        session.flush()
        return row
