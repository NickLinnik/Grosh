"""Tests for _pick_rate — liquidation-side rate selection."""

from decimal import Decimal

from grosh_consumer.repositories.currency_rate_repo import RateRow
from grosh_consumer.services.currency_conversion_service import RateSide, _pick_rate

ROW_FULL = RateRow(
    id=1,
    source="monobank",
    rate_mid=Decimal("4.00"),
    rate_buy=Decimal("3.95"),
    rate_sell=Decimal("4.05"),
)

ROW_NO_BUY = RateRow(
    id=2,
    source="monobank",
    rate_mid=Decimal("4.00"),
    rate_buy=None,
    rate_sell=Decimal("4.05"),
)

ROW_NO_SELL = RateRow(
    id=3,
    source="monobank",
    rate_mid=Decimal("4.00"),
    rate_buy=Decimal("3.95"),
    rate_sell=None,
)

ROW_MID_ONLY = RateRow(
    id=4,
    source="monobank",
    rate_mid=Decimal("4.00"),
    rate_buy=None,
    rate_sell=None,
)


def test_buy_side_selected_when_divide_false_and_rate_buy_present():
    rate, side = _pick_rate(ROW_FULL, divide=False)
    assert rate == Decimal("3.95")
    assert side == RateSide.BUY


def test_sell_side_selected_when_divide_true_and_rate_sell_present():
    rate, side = _pick_rate(ROW_FULL, divide=True)
    assert rate == Decimal("4.05")
    assert side == RateSide.SELL


def test_falls_back_to_mid_when_buy_null_and_divide_false():
    rate, side = _pick_rate(ROW_NO_BUY, divide=False)
    assert rate == Decimal("4.00")
    assert side == RateSide.MID


def test_falls_back_to_mid_when_sell_null_and_divide_true():
    rate, side = _pick_rate(ROW_NO_SELL, divide=True)
    assert rate == Decimal("4.00")
    assert side == RateSide.MID


def test_both_sides_null_returns_mid_both_directions():
    rate_false, side_false = _pick_rate(ROW_MID_ONLY, divide=False)
    rate_true, side_true = _pick_rate(ROW_MID_ONLY, divide=True)
    assert rate_false == Decimal("4.00")
    assert side_false == RateSide.MID
    assert rate_true == Decimal("4.00")
    assert side_true == RateSide.MID


def test_never_substitutes_opposite_side():
    """Verify no (divide, nulls) combination returns the opposite side."""
    cases = [
        # (divide, row, forbidden_side)
        (
            False,
            ROW_NO_BUY,
            RateSide.SELL,
        ),  # buy null + divide=False → must not use sell
        (True, ROW_NO_SELL, RateSide.BUY),  # sell null + divide=True → must not use buy
        (False, ROW_MID_ONLY, RateSide.SELL),
        (True, ROW_MID_ONLY, RateSide.BUY),
    ]
    for divide, row, forbidden in cases:
        _, side = _pick_rate(row, divide=divide)
        assert (
            side != forbidden
        ), f"divide={divide}, row={row} returned forbidden side {forbidden}"
