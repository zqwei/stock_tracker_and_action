from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session

from portfolio_assistant.db.models import FeedItem, FeedSource, FeedSyncStatus, FeedType
from portfolio_assistant.utils.dates import as_utc_naive, utc_now_naive


def _normalize_symbol(symbol: str | None) -> str | None:
    token = str(symbol or "").strip().upper()
    return token or None


def _normalize_text(value: str | None) -> str | None:
    token = str(value or "").strip()
    return token or None


def _coerce_sync_status(value: FeedSyncStatus | str) -> FeedSyncStatus:
    if isinstance(value, FeedSyncStatus):
        return value
    token = str(value or "").strip().upper()
    if not token:
        return FeedSyncStatus.ACTIVE
    return FeedSyncStatus(token)


class NewsProvider:
    """DB-backed provider for `feed_type=NEWS` items."""

    def __init__(self, provider: str = "manual") -> None:
        name = str(provider or "").strip()
        self.provider = name or "manual"

    def upsert_source(
        self,
        session: Session,
        *,
        scope_key: str,
        symbol: str | None = None,
        request_params: dict | None = None,
        status: FeedSyncStatus | str = FeedSyncStatus.ACTIVE,
    ) -> FeedSource:
        scope = str(scope_key or "").strip()
        if not scope:
            raise ValueError("scope_key is required")

        source = session.scalar(
            select(FeedSource).where(
                FeedSource.feed_type == FeedType.NEWS,
                FeedSource.provider == self.provider,
                FeedSource.scope_key == scope,
            )
        )

        now = utc_now_naive()
        next_status = _coerce_sync_status(status)
        if source is None:
            source = FeedSource(
                feed_type=FeedType.NEWS,
                provider=self.provider,
                scope_key=scope,
                symbol=_normalize_symbol(symbol),
                request_params=request_params,
                status=next_status,
                updated_at=now,
            )
            session.add(source)
            session.flush()
            return source

        source.symbol = _normalize_symbol(symbol)
        source.request_params = request_params
        source.status = next_status
        source.updated_at = now
        session.flush()
        return source

    def upsert_item(
        self,
        session: Session,
        *,
        external_id: str,
        title: str | None = None,
        url: str | None = None,
        published_at: datetime | None = None,
        symbol: str | None = None,
        payload_json: dict | None = None,
        source_id: int | None = None,
        content_hash: str | None = None,
    ) -> FeedItem:
        key = str(external_id or "").strip()
        if not key:
            raise ValueError("external_id is required")

        item = session.scalar(
            select(FeedItem).where(
                FeedItem.feed_type == FeedType.NEWS,
                FeedItem.provider == self.provider,
                FeedItem.external_id == key,
            )
        )
        now = utc_now_naive()
        normalized_published = as_utc_naive(published_at)

        if item is None:
            item = FeedItem(
                feed_source_id=source_id,
                feed_type=FeedType.NEWS,
                provider=self.provider,
                external_id=key,
                symbol=_normalize_symbol(symbol),
                event_at=None,
                published_at=normalized_published,
                title=_normalize_text(title),
                url=_normalize_text(url),
                content_hash=_normalize_text(content_hash),
                payload_json=payload_json,
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            )
            session.add(item)
            session.flush()
            return item

        if source_id is not None:
            item.feed_source_id = source_id
        item.symbol = _normalize_symbol(symbol)
        item.title = _normalize_text(title)
        item.url = _normalize_text(url)
        item.published_at = normalized_published
        item.content_hash = _normalize_text(content_hash)
        item.payload_json = payload_json
        item.last_seen_at = now
        item.is_active = True
        session.flush()
        return item

    def list_recent(
        self,
        session: Session,
        *,
        symbol: str | None = None,
        since: datetime | None = None,
        limit: int = 50,
    ) -> list[FeedItem]:
        size = max(int(limit), 1)
        stmt = select(FeedItem).where(
            FeedItem.feed_type == FeedType.NEWS,
            FeedItem.provider == self.provider,
            FeedItem.is_active.is_(True),
        )

        ticker = _normalize_symbol(symbol)
        if ticker:
            stmt = stmt.where(FeedItem.symbol == ticker)

        if since is not None:
            cutoff = as_utc_naive(since)
            stmt = stmt.where(
                or_(
                    FeedItem.published_at >= cutoff,
                    and_(FeedItem.published_at.is_(None), FeedItem.last_seen_at >= cutoff),
                )
            )

        stmt = stmt.order_by(
            FeedItem.published_at.is_(None),
            FeedItem.published_at.desc(),
            FeedItem.last_seen_at.desc(),
            FeedItem.id.desc(),
        ).limit(size)

        return list(session.scalars(stmt).all())

    def deactivate_missing(
        self,
        session: Session,
        *,
        source_id: int,
        active_external_ids: Iterable[str],
    ) -> int:
        active = {
            str(external_id or "").strip()
            for external_id in active_external_ids
            if str(external_id or "").strip()
        }

        stmt = (
            update(FeedItem)
            .where(
                FeedItem.feed_type == FeedType.NEWS,
                FeedItem.provider == self.provider,
                FeedItem.feed_source_id == source_id,
                FeedItem.is_active.is_(True),
            )
            .values(is_active=False, last_seen_at=utc_now_naive())
        )
        if active:
            stmt = stmt.where(~FeedItem.external_id.in_(active))

        result = session.execute(stmt)
        return int(result.rowcount or 0)
