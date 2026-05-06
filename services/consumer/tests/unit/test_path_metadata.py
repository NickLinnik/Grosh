"""Tests for _path_metadata — metadata structure and serialization."""

from decimal import Decimal
from uuid import UUID

from grosh_consumer.services.currency_conversion_service import (
    RateSide,
    RateTier,
    _path_metadata,
    _RatePath,
    _RateStep,
)


def _step(
    *,
    from_="PLN",
    to_="UAH",
    rate="4.0",
    side=RateSide.BUY,
    tier=RateTier.FRESH,
    proximity=0,
    divide=False,
    source="monobank",
    rate_id=UUID(int=1),
):
    return _RateStep(
        currency_from=from_,
        currency_to=to_,
        source=source,
        rate_id=rate_id,
        rate=Decimal(rate),
        rate_side=side,
        tier=tier,
        proximity_seconds=proximity,
        divide=divide,
    )


def test_1_hop_metadata_structure():
    path = _RatePath(steps=(_step(),))
    meta = _path_metadata(path)
    assert "path" in meta
    assert len(meta["path"]) == 1
    assert meta["hops"] == 1
    assert "effective_rate" in meta
    assert "quality" in meta
    assert "max_proximity_seconds" in meta
    assert "sides" in meta
    step_meta = meta["path"][0]
    assert "proximity_seconds" in step_meta
    assert "from" in step_meta
    assert "to" in step_meta
    assert "source" in step_meta
    assert "rate_id" in step_meta
    assert "rate" in step_meta
    assert "rate_side" in step_meta
    assert "tier" in step_meta
    assert "op" in step_meta


def test_2_hop_metadata_structure():
    path = _RatePath(
        steps=(
            _step(to_="UAH"),
            _step(from_="UAH", to_="USD", rate_id=UUID(int=2)),
        )
    )
    meta = _path_metadata(path)
    assert len(meta["path"]) == 2
    assert meta["hops"] == 2


def test_sides_deduped_and_sorted():
    path = _RatePath(
        steps=(
            _step(side=RateSide.SELL, rate_id=UUID(int=1)),
            _step(side=RateSide.BUY, rate_id=UUID(int=2)),
            _step(side=RateSide.MID, rate_id=UUID(int=3)),
            _step(side=RateSide.BUY, rate_id=UUID(int=4)),
        )
    )
    meta = _path_metadata(path)
    assert meta["sides"] == ["buy", "mid", "sell"]


def test_quality_is_max_tier():
    path = _RatePath(
        steps=(
            _step(tier=RateTier.FRESH, rate_id=UUID(int=1)),
            _step(tier=RateTier.FRESH, rate_id=UUID(int=2)),
            _step(tier=RateTier.CLOSEST, rate_id=UUID(int=3)),
        )
    )
    meta = _path_metadata(path)
    assert meta["quality"] == "closest"


def test_max_proximity_seconds_matches_worst_step():
    path = _RatePath(
        steps=(
            _step(proximity=0, rate_id=UUID(int=1)),
            _step(proximity=3600, rate_id=UUID(int=2)),
        )
    )
    meta = _path_metadata(path)
    assert meta["max_proximity_seconds"] == 3600


def test_effective_rate_is_a_string():
    path = _RatePath(steps=(_step(rate="4.123"),))
    meta = _path_metadata(path)
    assert isinstance(meta["effective_rate"], str)
    assert meta["effective_rate"] == "4.123"


def test_op_reflects_divide_flag():
    path_mul = _RatePath(steps=(_step(divide=False),))
    path_div = _RatePath(steps=(_step(divide=True),))
    assert _path_metadata(path_mul)["path"][0]["op"] == "multiply"
    assert _path_metadata(path_div)["path"][0]["op"] == "divide"


def test_proximity_seconds_serialized_as_integer_per_step():
    path = _RatePath(
        steps=(
            _step(proximity=0, rate_id=UUID(int=1)),
            _step(proximity=86400, rate_id=UUID(int=2)),
        )
    )
    meta = _path_metadata(path)
    for step_meta in meta["path"]:
        assert isinstance(step_meta["proximity_seconds"], int)
