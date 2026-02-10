from __future__ import annotations

from datetime import datetime, timezone

from portfolio_assistant.utils.dates import as_utc_naive, utc_now, utc_now_naive
from portfolio_assistant.utils.money import format_money, format_percent, safe_float


def test_dates_helpers_return_utc_shapes():
    aware_now = utc_now()
    naive_now = utc_now_naive()

    assert aware_now.tzinfo is not None
    assert naive_now.tzinfo is None

    aware_input = datetime(2025, 2, 10, 9, 0, 0, tzinfo=timezone.utc)
    converted = as_utc_naive(aware_input)
    assert converted is not None
    assert converted.tzinfo is None
    assert converted == datetime(2025, 2, 10, 9, 0, 0)


def test_money_helpers_format_values():
    assert safe_float("123.4") == 123.4
    assert safe_float("bad", default=1.5) == 1.5
    assert format_money(1234.5) == "+1,234.50"
    assert format_money(-12.3) == "-12.30"
    assert format_percent(0.1234) == "+12.34%"
    assert format_percent(None) == "n/a"
