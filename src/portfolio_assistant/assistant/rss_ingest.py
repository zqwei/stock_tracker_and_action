"""Tiny RSS ingestion helper for future daily briefings."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from urllib.request import urlopen


def fetch_feed_entries(feed_url: str, limit: int = 20) -> list[dict[str, str]]:
    with urlopen(feed_url, timeout=10) as response:  # noqa: S310 - user-configured feed URL
        xml_payload = response.read()

    root = ET.fromstring(xml_payload)
    entries: list[dict[str, str]] = []

    for item in root.findall(".//item")[:limit]:
        entries.append(
            {
                "title": item.findtext("title", default="").strip(),
                "link": item.findtext("link", default="").strip(),
                "published": item.findtext("pubDate", default="").strip(),
            }
        )

    return entries
