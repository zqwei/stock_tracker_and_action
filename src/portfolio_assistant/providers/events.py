"""Events provider abstraction for earnings and macro calendars."""

from __future__ import annotations


class EventsProvider:
    def get_earnings_calendar(self, symbols: list[str], start: str, end: str) -> list[dict[str, str]]:
        raise NotImplementedError

    def get_macro_calendar(self, start: str, end: str) -> list[dict[str, str]]:
        raise NotImplementedError


class NullEventsProvider(EventsProvider):
    def get_earnings_calendar(self, symbols: list[str], start: str, end: str) -> list[dict[str, str]]:
        _ = (symbols, start, end)
        return []

    def get_macro_calendar(self, start: str, end: str) -> list[dict[str, str]]:
        _ = (start, end)
        return []
