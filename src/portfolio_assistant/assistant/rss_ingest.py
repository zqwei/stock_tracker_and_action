from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Callable, Iterable, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from dateutil import parser as date_parser

_TRACKING_QUERY_PREFIXES = ("utm_",)
_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "ref",
    "spm",
}
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
_TITLE_KEY_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class RssItem:
    feed_url: str
    feed_title: str
    title: str
    url: str
    published_at: datetime | None
    summary: str
    symbols: tuple[str, ...] = ()
    dedupe_key: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "feed_url": self.feed_url,
            "feed_title": self.feed_title,
            "title": self.title,
            "url": self.url,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "summary": self.summary,
            "symbols": list(self.symbols),
            "dedupe_key": self.dedupe_key,
        }


@dataclass(frozen=True)
class RssIngestResult:
    items: list[RssItem]
    feeds_requested: int
    feeds_ingested: int
    duplicate_feeds_skipped: int
    duplicate_items_skipped: int
    errors: list[dict[str, str]]


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _normalize_text(value: str | None) -> str:
    text = unescape(str(value or ""))
    text = _HTML_TAG_RE.sub(" ", text)
    return _WHITESPACE_RE.sub(" ", text).strip()


def _title_key(value: str) -> str:
    cleaned = _normalize_text(value).lower()
    return _TITLE_KEY_RE.sub(" ", cleaned).strip()


def normalize_feed_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    parts = urlsplit(raw)
    if not parts.scheme and not parts.netloc:
        return raw.split("#", 1)[0].strip()

    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    if not hostname:
        return raw.split("#", 1)[0].strip()

    port = parts.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None
    netloc = f"{hostname}:{port}" if port else hostname

    path = re.sub(r"/+", "/", parts.path or "/")
    if path != "/":
        path = path.rstrip("/") or "/"

    filtered_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=False):
        lowered = key.lower()
        if lowered.startswith(_TRACKING_QUERY_PREFIXES):
            continue
        if lowered in _TRACKING_QUERY_KEYS:
            continue
        filtered_query.append((key, value))
    query = urlencode(sorted(filtered_query))

    return urlunsplit((scheme, netloc, path, query, ""))


def dedupe_feed_urls(feed_urls: Iterable[str]) -> tuple[list[str], int]:
    unique_urls: list[str] = []
    seen: set[str] = set()
    duplicates = 0

    for raw_url in feed_urls:
        normalized = normalize_feed_url(str(raw_url or ""))
        if not normalized:
            continue
        if normalized in seen:
            duplicates += 1
            continue
        seen.add(normalized)
        unique_urls.append(normalized)

    return unique_urls, duplicates


def _parse_datetime(raw_value: str | None) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        parsed = date_parser.parse(text)
    except (TypeError, ValueError, OverflowError):
        return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _child_text(node: ET.Element, names: set[str]) -> str:
    for child in list(node):
        if _local_name(child.tag) not in names:
            continue
        text = _normalize_text("".join(child.itertext()))
        if text:
            return text
    return ""


def _child_link(node: ET.Element) -> str:
    first_href = ""
    first_text = ""
    for child in list(node):
        name = _local_name(child.tag)
        if name == "link":
            href = str(child.attrib.get("href", "")).strip()
            rel = str(child.attrib.get("rel", "alternate") or "alternate").lower()
            if href and rel in {"alternate", "related", ""}:
                return href
            if href and not first_href:
                first_href = href
            text = _normalize_text("".join(child.itertext()))
            if text and not first_text:
                first_text = text
            continue

        if name == "guid":
            is_permalink = str(child.attrib.get("isPermaLink", "")).lower()
            guid_text = _normalize_text("".join(child.itertext()))
            if guid_text and is_permalink != "false":
                return guid_text

    return first_href or first_text


def _feed_entries(root: ET.Element) -> tuple[str, list[ET.Element]]:
    root_name = _local_name(root.tag)
    if root_name == "rss":
        for child in list(root):
            if _local_name(child.tag) == "channel":
                feed_title = _child_text(child, {"title"})
                entries = [row for row in list(child) if _local_name(row.tag) == "item"]
                return feed_title, entries
        return "", []

    if root_name == "feed":  # Atom
        feed_title = _child_text(root, {"title"})
        entries = [row for row in list(root) if _local_name(row.tag) == "entry"]
        return feed_title, entries

    if root_name == "rdf":
        feed_title = _child_text(root, {"title"})
        entries = [row for row in list(root) if _local_name(row.tag) == "item"]
        return feed_title, entries

    entries = [row for row in list(root) if _local_name(row.tag) in {"item", "entry"}]
    return "", entries


def build_item_dedupe_key(*, title: str, url: str, published_at: datetime | None) -> str:
    canonical_url = normalize_feed_url(url)
    if canonical_url:
        stable = canonical_url
    else:
        day = published_at.date().isoformat() if published_at else ""
        stable = f"{_title_key(title)}|{day}"
    digest = hashlib.sha1(stable.encode("utf-8")).hexdigest()[:20]
    return f"rss_{digest}"


def parse_feed_entries(xml_text: str, *, feed_url: str) -> list[RssItem]:
    source_url = normalize_feed_url(feed_url)
    if not str(xml_text or "").strip():
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    feed_title, entries = _feed_entries(root)
    items: list[RssItem] = []
    for entry in entries:
        title = _child_text(entry, {"title"}) or "(untitled)"
        url = _child_link(entry)
        summary = _child_text(entry, {"description", "summary", "content"})
        published_raw = _child_text(entry, {"pubdate", "published", "updated", "date"})
        published_at = _parse_datetime(published_raw)
        dedupe_key = build_item_dedupe_key(title=title, url=url, published_at=published_at)
        items.append(
            RssItem(
                feed_url=source_url,
                feed_title=feed_title,
                title=title,
                url=url,
                published_at=published_at,
                summary=summary,
                dedupe_key=dedupe_key,
            )
        )
    return items


def _match_holdings_symbols(text: str, holdings: set[str]) -> tuple[str, ...]:
    if not holdings:
        return ()
    haystack = str(text or "").upper()
    hits: list[str] = []
    for symbol in sorted(holdings):
        pattern = rf"(?<![A-Z0-9])(?:\$)?{re.escape(symbol)}(?![A-Z0-9])"
        if re.search(pattern, haystack):
            hits.append(symbol)
    return tuple(hits)


def _identity_key(item: RssItem) -> str:
    canonical_url = normalize_feed_url(item.url)
    if canonical_url:
        return f"url:{canonical_url}"
    day = item.published_at.date().isoformat() if item.published_at else ""
    return f"title:{_title_key(item.title)}|{day}"


def _item_score(item: RssItem) -> tuple[int, int, int, datetime]:
    has_url = int(bool(normalize_feed_url(item.url)))
    symbol_hits = len(item.symbols)
    summary_len = len(item.summary)
    published = item.published_at or datetime.min
    return (has_url, symbol_hits, summary_len, published)


def dedupe_feed_items(items: Sequence[RssItem]) -> tuple[list[RssItem], int]:
    deduped: dict[str, RssItem] = {}
    duplicates = 0

    for item in items:
        key = _identity_key(item)
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = item
            continue
        duplicates += 1
        deduped[key] = item if _item_score(item) >= _item_score(existing) else existing

    ordered = sorted(
        deduped.values(),
        key=lambda row: (
            row.published_at or datetime.min,
            row.title.lower(),
            normalize_feed_url(row.url),
        ),
        reverse=True,
    )
    return ordered, duplicates


def _default_fetcher(url: str, timeout_seconds: float) -> str:
    response = requests.get(
        url,
        timeout=timeout_seconds,
        headers={"User-Agent": "PortfolioAssistant/1.0 (+local)"},
    )
    response.raise_for_status()
    return response.text


def ingest_rss_feeds(
    *,
    feed_urls: Iterable[str],
    holdings_symbols: Iterable[str] | None = None,
    lookback_days: int = 10,
    now: datetime | None = None,
    max_items: int = 30,
    timeout_seconds: float = 10.0,
    fetcher: Callable[[str], str] | None = None,
) -> RssIngestResult:
    unique_feeds, duplicate_feeds = dedupe_feed_urls(feed_urls)
    holdings = {
        str(symbol or "").strip().upper()
        for symbol in (holdings_symbols or [])
        if str(symbol or "").strip()
    }

    try:
        lookback = int(lookback_days)
    except (TypeError, ValueError):
        lookback = 10
    try:
        item_limit = int(max_items)
    except (TypeError, ValueError):
        item_limit = 30

    cutoff: datetime | None = None
    if lookback >= 0:
        reference = now or datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff = reference - timedelta(days=lookback)

    all_items: list[RssItem] = []
    errors: list[dict[str, str]] = []
    feeds_ingested = 0
    active_fetcher = fetcher or (lambda url: _default_fetcher(url, timeout_seconds))

    for feed_url in unique_feeds:
        try:
            xml_text = active_fetcher(feed_url)
        except Exception as exc:
            errors.append({"feed_url": feed_url, "error": str(exc)})
            continue

        items = parse_feed_entries(xml_text, feed_url=feed_url)
        feeds_ingested += 1
        for item in items:
            if cutoff and item.published_at and item.published_at < cutoff:
                continue

            symbols = _match_holdings_symbols(f"{item.title} {item.summary}", holdings)
            if holdings and not symbols:
                continue

            all_items.append(
                RssItem(
                    feed_url=item.feed_url,
                    feed_title=item.feed_title,
                    title=item.title,
                    url=item.url,
                    published_at=item.published_at,
                    summary=item.summary,
                    symbols=symbols,
                    dedupe_key=item.dedupe_key,
                )
            )

    deduped_items, duplicate_items = dedupe_feed_items(all_items)
    if item_limit > 0:
        deduped_items = deduped_items[:item_limit]

    return RssIngestResult(
        items=deduped_items,
        feeds_requested=len(unique_feeds),
        feeds_ingested=feeds_ingested,
        duplicate_feeds_skipped=duplicate_feeds,
        duplicate_items_skipped=duplicate_items,
        errors=errors,
    )
