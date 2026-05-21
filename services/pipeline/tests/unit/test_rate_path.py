"""Tests for _RatePath — compound path operations."""

from decimal import Decimal
from uuid import UUID

from grosh_pipeline.services.currency_conversion_service import (
    RateSide,
    RateTier,
    _RatePath,
    _RateStep,
)


def _step(rate, *, tier=RateTier.FRESH, proximity=0, divide=False):
    return _RateStep(
        currency_from="A",
        currency_to="B",
        source="monobank",
        rate_id=UUID(int=1),
        rate=Decimal(str(rate)),
        rate_side=RateSide.MID,
        tier=tier,
        proximity_seconds=proximity,
        divide=divide,
    )


def test_max_tier_picks_worst():
    path = _RatePath(
        steps=(
            _step(2, tier=RateTier.FRESH),
            _step(3, tier=RateTier.CLOSEST),
            _step(4, tier=RateTier.FRESH),
        )
    )
    assert path.max_tier is RateTier.CLOSEST


def test_max_proximity_seconds_picks_worst():
    path = _RatePath(
        steps=(
            _step(2, proximity=0),
            _step(3, proximity=3600),
            _step(4, proximity=0),
        )
    )
    assert path.max_proximity_seconds == 3600


def test_effective_rate_compounds_multiply_steps():
    path = _RatePath(steps=(_step(2), _step(3)))
    assert path.effective_rate == Decimal("6")


def test_effective_rate_compounds_with_divide():
    path = _RatePath(steps=(_step(2, divide=False), _step(4, divide=True)))
    assert path.effective_rate == Decimal("0.5")


def test_apply_equals_amount_times_effective_rate_for_multiply_only():
    path = _RatePath(steps=(_step(2), _step(3)))
    amount = Decimal("100")
    assert path.apply(amount) == amount * path.effective_rate


def test_single_step_path_behaves_as_step():
    step = _step(4)
    path = _RatePath(steps=(step,))
    amount = Decimal("100")
    assert path.apply(amount) == step.apply(amount)
    assert path.effective_rate == Decimal("4")
    assert path.max_tier is RateTier.FRESH
    assert path.max_proximity_seconds == 0
