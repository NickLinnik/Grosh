"""Tests for _path_metadata — metadata serialization."""

from decimal import Decimal

from grosh_consumer.services.currency_conversion_service import (
    RateSide,
    RateTier,
    _path_metadata,
    _RatePath,
    _RateStep,
)


def _step(
    rate,
    *,
    divide=False,
    tier=RateTier.FRESH,
    side=None,
    from_="PLN",
    to="UAH",
    source="monobank",
    rate_id=1,
):
    if side is None:
        side = RateSide.SELL if divide else RateSide.BUY
    return _RateStep(
        currency_from=from_,
        currency_to=to,
        source=source,
        rate_id=rate_id,
        rate=Decimal(str(rate)),
        rate_side=side,
        tier=tier,
        divide=divide,
    )


def test_one_hop_metadata_structure():
    path = _RatePath(steps=(_step("4.0", tier=RateTier.FRESH),))
    meta = _path_metadata(path)

    assert set(meta.keys()) == {"path", "effective_rate", "hops", "quality", "sides"}
    assert len(meta["path"]) == 1
    entry = meta["path"][0]
    assert set(entry.keys()) == {
        "from",
        "to",
        "source",
        "rate_id",
        "rate",
        "rate_side",
        "tier",
        "op",
    }
    assert meta["hops"] == 1
    assert meta["quality"] == "fresh"
    assert meta["sides"] == ["buy"]


def test_two_hop_metadata_structure():
    path = _RatePath(
        steps=(
            _step("4.0", tier=RateTier.FRESH, side=RateSide.BUY, from_="PLN", to="UAH"),
            _step(
                "0.025",
                tier=RateTier.FRESH,
                side=RateSide.SELL,
                from_="UAH",
                to="USD",
                divide=True,
            ),
        )
    )
    meta = _path_metadata(path)

    assert len(meta["path"]) == 2
    assert meta["hops"] == 2
    assert meta["sides"] == ["buy", "sell"]


def test_sides_deduped_and_sorted():
    path = _RatePath(
        steps=(
            _step("1.0", side=RateSide.SELL),
            _step("2.0", side=RateSide.BUY),
            _step("3.0", side=RateSide.MID),
            _step("4.0", side=RateSide.BUY),
        )
    )
    meta = _path_metadata(path)
    assert meta["sides"] == ["buy", "mid", "sell"]


def test_quality_is_max_tier():
    path = _RatePath(
        steps=(
            _step("1.0", tier=RateTier.FRESH),
            _step("2.0", tier=RateTier.FRESH),
            _step("3.0", tier=RateTier.STALE),
        )
    )
    meta = _path_metadata(path)
    assert meta["quality"] == "stale"


def test_effective_rate_is_string():
    path = _RatePath(steps=(_step("4.0"),))
    meta = _path_metadata(path)
    assert isinstance(meta["effective_rate"], str)
    assert Decimal(meta["effective_rate"]) == Decimal("4.0")


def test_op_field_reflects_divide_flag():
    multiply_step = _step("4.0", divide=False)
    divide_step = _step("4.0", divide=True)
    path = _RatePath(steps=(multiply_step, divide_step))
    meta = _path_metadata(path)
    assert meta["path"][0]["op"] == "multiply"
    assert meta["path"][1]["op"] == "divide"
