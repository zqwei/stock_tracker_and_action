from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from portfolio_assistant.db.models import FeedItem, FeedType
from portfolio_assistant.providers.events import EventProvider
from portfolio_assistant.providers.news import NewsProvider


def test_news_provider_upsert_and_list_recent(db_session):
    provider = NewsProvider(provider="rss")
    source = provider.upsert_source(
        db_session,
        scope_key="holdings:AAPL",
        symbol="aapl",
        request_params={"lang": "en"},
    )

    provider.upsert_item(
        db_session,
        source_id=source.id,
        external_id="story-1",
        title="Apple launches update",
        symbol="aapl",
        published_at=datetime(2025, 2, 1, 9, 0, 0),
        url="https://news.example.com/aapl-1",
    )
    item_two = provider.upsert_item(
        db_session,
        source_id=source.id,
        external_id="story-2",
        title="Apple guidance",
        symbol="AAPL",
        published_at=datetime(2025, 2, 2, 9, 0, 0),
        url="https://news.example.com/aapl-2",
    )

    updated = provider.upsert_item(
        db_session,
        source_id=source.id,
        external_id="story-2",
        title="Apple guidance (updated)",
        symbol="AAPL",
        published_at=datetime(2025, 2, 2, 9, 0, 0),
        url="https://news.example.com/aapl-2",
    )

    assert updated.id == item_two.id

    rows = provider.list_recent(db_session, symbol="aapl", limit=10)
    assert [row.external_id for row in rows] == ["story-2", "story-1"]
    assert rows[0].title == "Apple guidance (updated)"
    assert all(row.feed_type == FeedType.NEWS for row in rows)
    assert all(row.provider == "rss" for row in rows)


def test_news_provider_deactivate_missing(db_session):
    provider = NewsProvider(provider="rss")
    source = provider.upsert_source(db_session, scope_key="holdings:MSFT", symbol="MSFT")
    provider.upsert_item(
        db_session,
        source_id=source.id,
        external_id="story-a",
        title="A",
        symbol="MSFT",
        published_at=datetime(2025, 2, 1, 9, 0, 0),
    )
    provider.upsert_item(
        db_session,
        source_id=source.id,
        external_id="story-b",
        title="B",
        symbol="MSFT",
        published_at=datetime(2025, 2, 1, 10, 0, 0),
    )

    changed = provider.deactivate_missing(
        db_session,
        source_id=source.id,
        active_external_ids=["story-a"],
    )
    db_session.commit()

    assert changed == 1
    rows = list(
        db_session.query(FeedItem).filter(FeedItem.provider == "rss").order_by(FeedItem.external_id)
    )
    assert rows[0].external_id == "story-a" and rows[0].is_active is True
    assert rows[1].external_id == "story-b" and rows[1].is_active is False


def test_event_provider_upsert_and_upcoming_filters(db_session):
    provider = EventProvider(provider="calendar")
    earnings_source = provider.upsert_source(
        db_session,
        feed_type=FeedType.EARNINGS,
        scope_key="symbol:AAPL",
        symbol="AAPL",
    )
    macro_source = provider.upsert_source(
        db_session,
        feed_type=FeedType.MACRO,
        scope_key="macro:us",
    )

    now = datetime(2025, 2, 10, 8, 0, 0)
    provider.upsert_event(
        db_session,
        feed_type=FeedType.EARNINGS,
        source_id=earnings_source.id,
        external_id="aapl-q1",
        title="AAPL earnings",
        symbol="AAPL",
        event_at=now + timedelta(days=1),
    )
    provider.upsert_event(
        db_session,
        feed_type=FeedType.MACRO,
        source_id=macro_source.id,
        external_id="cpi-2025-02",
        title="US CPI",
        event_at=now + timedelta(days=2),
    )

    earnings_only = provider.list_upcoming(
        db_session,
        feed_type=FeedType.EARNINGS,
        symbol="aapl",
        start_at=now,
        end_at=now + timedelta(days=30),
    )
    assert len(earnings_only) == 1
    assert earnings_only[0].external_id == "aapl-q1"
    assert earnings_only[0].feed_type == FeedType.EARNINGS

    combined = provider.list_upcoming(
        db_session,
        start_at=now,
        end_at=now + timedelta(days=30),
    )
    assert [row.external_id for row in combined] == ["aapl-q1", "cpi-2025-02"]


def test_event_provider_rejects_invalid_feed_type(db_session):
    provider = EventProvider(provider="calendar")
    with pytest.raises(ValueError):
        provider.upsert_source(
            db_session,
            feed_type=FeedType.NEWS,
            scope_key="invalid",
        )
