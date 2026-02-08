"""Price provider abstraction and local stub implementation."""

from __future__ import annotations

from dataclasses import dataclass, field


class PriceProvider:
    def get_quote(self, symbol: str) -> float | None:  # pragma: no cover - interface
        raise NotImplementedError

    def get_history(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str = "1d",
    ) -> list[dict[str, str | float]]:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass(slots=True)
class InMemoryPriceProvider(PriceProvider):
    quotes: dict[str, float] = field(default_factory=dict)

    def get_quote(self, symbol: str) -> float | None:
        return self.quotes.get(symbol.upper())

    def get_history(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str = "1d",
    ) -> list[dict[str, str | float]]:
        quote = self.get_quote(symbol)
        if quote is None:
            return []
        return [{"date": end, "close": quote, "interval": interval}]
