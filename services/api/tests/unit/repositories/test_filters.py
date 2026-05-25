from datetime import UTC, datetime

import pytest

from grosh_api.repositories._filters import build_where


def test_empty_list_returns_empty_pair() -> None:
    assert build_where([]) == ("", [])


def test_single_filter() -> None:
    cond, params = build_where([("a =", 42)])
    assert cond == "(a = $1)"
    assert params == [42]


def test_multiple_filters() -> None:
    cond, params = build_where([("a =", 1), ("b >=", 2)])
    assert cond == "(a = $1) AND (b >= $2)"
    assert params == [1, 2]


def test_all_none_entries_skipped() -> None:
    assert build_where([("a =", None), ("b =", None)]) == ("", [])


@pytest.mark.parametrize(
    "filters, expected_cond, expected_params",
    [
        (
            [("a =", 1), ("b =", None), ("c =", 3)],
            "(a = $1) AND (c = $2)",
            [1, 3],
        ),
        (
            [("a =", None), ("b =", 2), ("c =", None)],
            "(b = $1)",
            [2],
        ),
    ],
)
def test_mixed_none_and_non_none_preserves_order_and_renumbers(
    filters: list[tuple[str, object | None]],
    expected_cond: str,
    expected_params: list[object],
) -> None:
    cond, params = build_where(filters)
    assert cond == expected_cond
    assert params == expected_params


def test_compound_left_side_expression_is_parenthesized() -> None:
    """Compound expressions must be wrapped — without parens, AND binds tighter
    than OR and the surrounding clause silently breaks (precedence bug)."""
    at = datetime(2026, 1, 1, tzinfo=UTC)
    cond, params = build_where([("valid_to IS NULL OR valid_to >", at)])
    assert cond == "(valid_to IS NULL OR valid_to > $1)"
    assert params == [at]


@pytest.mark.parametrize("falsy_value", [False, 0, ""])
def test_non_none_falsy_values_preserved(falsy_value: object) -> None:
    cond, params = build_where([("x =", falsy_value)])
    assert cond == "(x = $1)"
    assert params == [falsy_value]


def test_same_value_twice_gets_two_placeholders() -> None:
    """Regression guard for the SCD2 case in get_rates_at where `at` appears
    in both the upper-bound and the OR-compound expression."""
    at = datetime(2026, 1, 1, tzinfo=UTC)
    cond, params = build_where(
        [
            ("valid_from <=", at),
            ("valid_to IS NULL OR valid_to >", at),
        ]
    )
    assert cond == "(valid_from <= $1) AND (valid_to IS NULL OR valid_to > $2)"
    assert params == [at, at]
