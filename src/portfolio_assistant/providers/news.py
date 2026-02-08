"""News provider abstraction."""

from __future__ import annotations


class NewsProvider:
    def get_news(self, symbols: list[str], start: str, end: str) -> list[dict[str, str]]:
        raise NotImplementedError


class NullNewsProvider(NewsProvider):
    def get_news(self, symbols: list[str], start: str, end: str) -> list[dict[str, str]]:
        _ = (symbols, start, end)
        return []
