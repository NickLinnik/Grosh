"""Tests for _pick_rate — rate-side selection with mid fallback."""

from decimal import Decimal
from uuid import UUID

import pytest

from grosh_consumer.repositories.currency_rate_repo import RateRow
from grosh_consumer.services.currency_conversion_service import (
    RateSide,
    _pick_rate,
)


def _row(*, buy=None, sell=None, mid=Decimal("4.00")):
    return RateRow(
        id=UUID(int=1),
        source="monobank",
        rate_mid=mid,
        rate_buy=buy,
        rate_sell=sell,
    )


def test_buy_side_when_divide_false_and_rate_buy_present():
    rate, side = _pick_rate(_row(buy=Decimal("3.95")), divide=False)
    assert rate == Decimal("3.95")
    assert side is RateSide.BUY


def test_sell_side_when_divide_true_and_rate_sell_present():
    rate, side = _pick_rate(_row(sell=Decimal("4.05")), divide=True)
    assert rate == Decimal("4.05")
    assert side is RateSide.SELL


def test_falls_back_to_mid_when_buy_null_and_divide_false():
    rate, side = _pick_rate(_row(buy=None, sell=Decimal("4.05")), divide=False)
    assert rate == Decimal("4.00")
    assert side is RateSide.MID


def test_falls_back_to_mid_when_sell_null_and_divide_true():
    rate, side = _pick_rate(_row(buy=Decimal("3.95"), sell=None), divide=True)
    assert rate == Decimal("4.00")
    assert side is RateSide.MID


def test_both_sides_null_returns_mid_for_both_directions():
    rate_d, side_d = _pick_rate(_row(), divide=False)
    rate_r, side_r = _pick_rate(_row(), divide=True)
    assert rate_d == Decimal("4.00")
    assert side_d is RateSide.MID
    assert rate_r == Decimal("4.00")
    assert side_r is RateSide.MID


@pytest.mark.parametrize(
    "divide,buy,sell",
    [
        (False, None, Decimal("4.05")),
        (True, Decimal("3.95"), None),
        (False, None, None),
        (True, None, None),
    ],
)
def test_never_substitutes_opposite_side(divide, buy, sell):
    row = _row(buy=buy, sell=sell)
    rate, side = _pick_rate(row, divide=divide)
    # Should always be mid when preferred side is None
    if divide and sell is None:
        assert side is RateSide.MID
        assert rate == Decimal("4.00")
    elif not divide and buy is None:
        assert side is RateSide.MID
        assert rate == Decimal("4.00")
