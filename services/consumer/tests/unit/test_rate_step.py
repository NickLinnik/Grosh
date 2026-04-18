"""Tests for _RateStep.apply — single-leg rate application."""

from decimal import Decimal

from grosh_consumer.services.currency_conversion_service import (
    RateSide,
    RateTier,
    _RateStep,
)


def _step(rate, *, divide):
    return _RateStep(
        currency_from="PLN",
        currency_to="UAH",
        source="monobank",
        rate_id=1,
        rate=Decimal(str(rate)),
        rate_side=RateSide.BUY,
        tier=RateTier.FRESH,
        divide=divide,
    )


def test_multiply_when_divide_false():
    step = _step("4.0", divide=False)
    assert step.apply(Decimal("100")) == Decimal("400")


def test_divide_when_divide_true():
    step = _step("4.0", divide=True)
    assert step.apply(Decimal("100")) == Decimal("25")


def test_preserves_decimal_precision():
    pi = "3.14159265358979323846"
    step = _step(pi, divide=False)
    result = step.apply(Decimal("1"))
    assert isinstance(result, Decimal)
    assert result == Decimal(pi)
