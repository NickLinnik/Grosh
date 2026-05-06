def build_where(
    filters: list[tuple[str, object | None]],
) -> tuple[str, list[object]]:
    """Build a parameterised WHERE clause body from optional filter pairs.

    Each filter is ``(expr_left, value)``. Entries whose value is ``None`` are
    skipped; non-None falsy values (``False``, ``0``, ``""``) are preserved.
    Compound expressions are passed whole on the left side, e.g.
    ``("valid_to IS NULL OR valid_to >", at)`` renders as
    ``(valid_to IS NULL OR valid_to > $N)``.

    Each rendered predicate is wrapped in parentheses. This is mandatory for
    compound expressions — without it, ``AND`` binds tighter than ``OR`` and
    a filter like ``valid_to IS NULL OR valid_to >`` would silently break the
    surrounding clause. Parens around simple predicates (``col = $N``) are
    harmless.

    Returns ``(condition_str, params)``. The condition string carries no
    leading WHERE keyword — callers compose it into their own templates
    and may append cursor or other predicates with ``len(params) + 1``.
    Returns ``("", [])`` if no filters apply.
    """
    active = [(expr, value) for expr, value in filters if value is not None]
    conditions = [f"({expr} ${i + 1})" for i, (expr, _) in enumerate(active)]
    params: list[object] = [value for _, value in active]
    return " AND ".join(conditions), params
