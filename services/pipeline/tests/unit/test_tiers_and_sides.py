"""Tests for RateTier and RateSide enums."""

from grosh_pipeline.services.currency_conversion_service import RateSide, RateTier


def test_rate_tier_list_order():
    assert list(RateTier) == [RateTier.FRESH, RateTier.CLOSEST]


def test_rate_tier_int_values():
    assert int(RateTier.FRESH) == 0
    assert int(RateTier.CLOSEST) == 1


def test_max_of_mixed_tier_list_returns_closest():
    tiers = [RateTier.FRESH, RateTier.CLOSEST, RateTier.FRESH]
    assert max(tiers) is RateTier.CLOSEST


def test_rate_side_values():
    assert RateSide.BUY.value == "buy"
    assert RateSide.SELL.value == "sell"
    assert RateSide.MID.value == "mid"
