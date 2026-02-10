from __future__ import annotations

from datetime import datetime

from portfolio_assistant.assistant.rss_ingest import (
    ingest_rss_feeds,
    normalize_feed_url,
    parse_feed_entries,
)


def test_ingest_rss_feeds_dedupes_feeds_and_items_and_filters_holdings():
    feed_one = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Feed One</title>
    <item>
      <title>AAPL announces earnings date</title>
      <link>https://news.example.com/story/aapl-earnings?utm_source=rss&amp;utm_medium=email</link>
      <pubDate>Mon, 09 Feb 2026 14:00:00 GMT</pubDate>
      <description>Apple (AAPL) scheduled earnings release.</description>
    </item>
    <item>
      <title>Macro market close recap</title>
      <link>https://news.example.com/story/macro-close</link>
      <pubDate>Mon, 09 Feb 2026 13:00:00 GMT</pubDate>
      <description>Broad market wrap-up without holdings ticker mentions.</description>
    </item>
  </channel>
</rss>
"""
    feed_two = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Feed Two</title>
    <item>
      <title>AAPL announces earnings date</title>
      <link>https://news.example.com/story/aapl-earnings</link>
      <pubDate>Mon, 09 Feb 2026 14:01:00 GMT</pubDate>
      <description>Duplicate syndicated item for AAPL.</description>
    </item>
    <item>
      <title>TSLA adds new factory line</title>
      <link>https://news.example.com/story/tsla-factory</link>
      <pubDate>Mon, 09 Feb 2026 14:30:00 GMT</pubDate>
      <description>TSLA article should be filtered out for this holdings set.</description>
    </item>
  </channel>
</rss>
"""

    feed_one_key = normalize_feed_url("https://feeds.example.com/main.xml")
    feed_two_key = normalize_feed_url("https://feeds.example.com/alt.xml")
    feed_map = {
        feed_one_key: feed_one,
        feed_two_key: feed_two,
    }
    fetch_calls: list[str] = []

    def fetcher(url: str) -> str:
        fetch_calls.append(url)
        return feed_map[url]

    result = ingest_rss_feeds(
        feed_urls=[
            "https://feeds.example.com/main.xml?utm_source=portfolio",
            "https://feeds.example.com/main.xml",
            "https://feeds.example.com/alt.xml",
        ],
        holdings_symbols=["AAPL"],
        lookback_days=14,
        now=datetime(2026, 2, 10, 9, 0, 0),
        fetcher=fetcher,
    )

    assert result.feeds_requested == 2
    assert result.feeds_ingested == 2
    assert result.duplicate_feeds_skipped == 1
    assert len(fetch_calls) == 2
    assert result.duplicate_items_skipped == 1
    assert len(result.items) == 1
    assert result.errors == []

    item = result.items[0]
    assert item.feed_title in {"Feed One", "Feed Two"}
    assert item.symbols == ("AAPL",)
    assert normalize_feed_url(item.url) == "https://news.example.com/story/aapl-earnings"


def test_parse_feed_entries_supports_atom_and_parses_datetimes():
    atom_xml = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Feed</title>
  <entry>
    <title>MSFT raises outlook</title>
    <link href="https://news.example.com/story/msft-outlook#top" rel="alternate"/>
    <updated>2026-02-10T11:00:00Z</updated>
    <summary>MSFT guidance update.</summary>
  </entry>
</feed>
"""
    items = parse_feed_entries(atom_xml, feed_url="https://feeds.example.com/atom.xml")
    assert len(items) == 1
    assert items[0].feed_title == "Atom Feed"
    assert items[0].title == "MSFT raises outlook"
    assert items[0].url == "https://news.example.com/story/msft-outlook#top"
    assert items[0].published_at == datetime(2026, 2, 10, 11, 0, 0)
    assert items[0].dedupe_key.startswith("rss_")


def test_ingest_rss_feeds_collects_fetch_errors():
    def fetcher(_url: str) -> str:
        raise RuntimeError("network unavailable")

    result = ingest_rss_feeds(
        feed_urls=["https://feeds.example.com/failed.xml"],
        holdings_symbols=["AAPL"],
        lookback_days=7,
        now=datetime(2026, 2, 10, 9, 0, 0),
        fetcher=fetcher,
    )

    assert result.feeds_requested == 1
    assert result.feeds_ingested == 0
    assert result.duplicate_feeds_skipped == 0
    assert result.items == []
    assert len(result.errors) == 1
    assert result.errors[0]["feed_url"] == "https://feeds.example.com/failed.xml"
    assert "network unavailable" in result.errors[0]["error"]
