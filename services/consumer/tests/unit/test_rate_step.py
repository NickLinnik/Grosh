"""Tests for _RateStep.apply — multiply vs divide."""

from decimal import Decimal
from uuid import UUID

from grosh_consumer.services.currency_conversion_service import (
    RateSide,
    RateTier,
    _RateStep,
)


def _step(rate, divide=False):
    return _RateStep(
        currency_from="PLN",
        currency_to="UAH",
        source="monobank",
        rate_id=UUID(int=1),
        rate=Decimal(str(rate)),
        rate_side=RateSide.MID,
        tier=RateTier.FRESH,
        proximity_seconds=0,
        divide=divide,
    )


def test_multiply_when_divide_false():
    step = _step(4)
    assert step.apply(Decimal("100")) == Decimal("400")


def test_divide_when_divide_true():
    step = _step(4, divide=True)
    assert step.apply(Decimal("100")) == Decimal("25")


def test_preserves_decimal_precision():
    step = _step("4.123456789")
    result = step.apply(Decimal("100.00"))
    assert result == Decimal("412.3456789000")
    assert isinstance(result, Decimal)
