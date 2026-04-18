"""Tests for RateTier and RateSide enumerations."""

from grosh_consumer.services.currency_conversion_service import RateSide, RateTier


def test_rate_tier_iteration_order():
    assert list(RateTier) == [RateTier.FRESH, RateTier.STALE, RateTier.CLOSEST]


def test_rate_tier_int_values():
    assert RateTier.FRESH == 0
    assert RateTier.STALE == 1
    assert RateTier.CLOSEST == 2
    assert RateTier.FRESH < RateTier.STALE < RateTier.CLOSEST


def test_rate_tier_max_of_mixed_list():
    result = max([RateTier.FRESH, RateTier.CLOSEST, RateTier.STALE])
    assert result == RateTier.CLOSEST


def test_rate_side_values():
    assert RateSide.BUY.value == "buy"
    assert RateSide.SELL.value == "sell"
    assert RateSide.MID.value == "mid"
