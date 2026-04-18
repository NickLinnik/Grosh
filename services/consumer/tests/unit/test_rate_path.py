"""Tests for _RatePath — multi-step compounding."""

from decimal import Decimal

from grosh_consumer.services.currency_conversion_service import (
    RateSide,
    RateTier,
    _RatePath,
    _RateStep,
)


def _step(rate, *, divide=False, tier=RateTier.FRESH, from_="PLN", to="UAH"):
    return _RateStep(
        currency_from=from_,
        currency_to=to,
        source="monobank",
        rate_id=1,
        rate=Decimal(str(rate)),
        rate_side=RateSide.SELL if divide else RateSide.BUY,
        tier=tier,
        divide=divide,
    )


def test_max_tier_picks_worst_across_steps():
    path = _RatePath(
        steps=(
            _step("4.0", tier=RateTier.FRESH),
            _step("3.0", tier=RateTier.STALE),
            _step("2.0", tier=RateTier.FRESH),
        )
    )
    assert path.max_tier == RateTier.STALE


def test_effective_rate_compounds_multiply_steps():
    path = _RatePath(
        steps=(
            _step("2", divide=False),
            _step("3", divide=False),
        )
    )
    assert path.effective_rate == Decimal("6")


def test_effective_rate_compounds_with_divide():
    path = _RatePath(
        steps=(
            _step("2", divide=False),
            _step("4", divide=True),
        )
    )
    assert path.effective_rate == Decimal("0.5")


def test_apply_equivalent_to_compounded_effective_rate_times_amount():
    path = _RatePath(
        steps=(
            _step("3", divide=False),
            _step("2", divide=False),
        )
    )
    amount = Decimal("1000")
    assert path.apply(amount) == amount * path.effective_rate


def test_single_step_path_behaves_as_step():
    step = _step("4.0", divide=False)
    path = _RatePath(steps=(step,))
    amount = Decimal("250")
    assert path.apply(amount) == step.apply(amount)
