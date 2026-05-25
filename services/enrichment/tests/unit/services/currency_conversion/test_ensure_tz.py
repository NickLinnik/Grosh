"""Tests for _ensure_tz — timezone attachment."""

from datetime import UTC, datetime

from grosh_enrichment.services.currency_conversion_service import _ensure_tz


def test_naive_datetime_gets_utc_attached():
    naive = datetime(2025, 6, 1, 12, 0, 0)
    result = _ensure_tz(naive)
    assert result.tzinfo is UTC
    assert result.year == 2025
    assert result.month == 6
    assert result.day == 1
    assert result.hour == 12
    assert result.minute == 0


def test_aware_datetime_unchanged():
    aware = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)
    result = _ensure_tz(aware)
    assert result is aware
