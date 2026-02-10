from __future__ import annotations

from datetime import date, datetime, timezone


def utc_now() -> datetime:
    """Return timezone-aware UTC now."""
    return datetime.now(timezone.utc)


def utc_now_naive() -> datetime:
    """Return naive UTC timestamp for DB columns stored without tzinfo."""
    return utc_now().replace(tzinfo=None)


def as_utc_naive(value: datetime | None) -> datetime | None:
    """Normalize datetime to naive UTC, preserving None."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def as_date(value: datetime | date | None) -> date | None:
    """Coerce date-like values to `date`."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return value.date()
