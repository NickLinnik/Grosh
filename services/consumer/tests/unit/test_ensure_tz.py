"""Tests for _ensure_tz — naive datetime → UTC coercion."""

from datetime import UTC, datetime, timedelta, timezone

from grosh_consumer.services.currency_conversion_service import _ensure_tz


def test_naive_datetime_gets_utc():
    naive = datetime(2025, 1, 1, 12, 0)
    result = _ensure_tz(naive)
    assert result.tzinfo == UTC
    assert result.year == 2025
    assert result.month == 1
    assert result.day == 1
    assert result.hour == 12
    assert result.minute == 0


def test_aware_datetime_normalized_to_utc():
    tz = timezone(timedelta(hours=3))
    aware = datetime(2025, 1, 1, 12, 0, tzinfo=tz)
    result = _ensure_tz(aware)
    assert result.tzinfo == UTC
    assert result == aware  # same instant, different representation
    assert result.hour == 9  # 12:00+03:00 = 09:00 UTC
