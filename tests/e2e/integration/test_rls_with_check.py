"""Integration tests for RLS WITH CHECK clauses added in migration 0014.

The e2e conftest's `db_pool` connects as `grosh_admin` (table owner),
which bypasses RLS by default. Each test in this module calls
`SET LOCAL ROLE grosh_ingestion` inside its transaction to drop the
table-owner privilege and exercise the production RLS regime. The session
variables `app.current_user_id` and `app.current_user_role` are then set
via `set_config(..., true)` (transaction-local).

The conftest fixture `_grant_rls_test_writes` widens grosh_ingestion's table
grants in the throwaway test DB so the role can attempt writes on all five
user-scoped tables. Production grants are unaffected — see the fixture's
docstring.

Why `user_settings` is excluded from the cross-user-write test: migration
0006 installs an AFTER INSERT trigger on users that auto-creates a
user_settings row for the new user, and `user_settings.user_id` is the PK.
A cross-user INSERT attack therefore always collides with the UNIQUE
constraint before the RLS WITH CHECK can fire, and an UPDATE attempt is
filtered out by the USING clause (0 rows updated, no error). user_settings
is still covered by the happy-path test (UPDATE timezone for self).

The enrichment consumer is unaffected by WITH CHECK because `grosh_consumer`
has BYPASSRLS; that invariant is verified separately by
`test_single_writer_carveout.py` and not duplicated here.
"""

from datetime import datetime, timezone
from uuid import UUID, uuid4

import asyncpg
import pytest

from integration.conftest import insert_account, insert_user


async def _setup_session(
    conn: asyncpg.Connection,
    user_id: UUID,
    role: str,
) -> None:
    """Switch to grosh_ingestion role and set RLS session vars.

    grosh_admin (the table owner) bypasses RLS; grosh_ingestion does not.
    SET LOCAL ROLE scopes the change to the current transaction, so the
    next test's rolled-back transaction reverts to grosh_admin automatically.
    """
    await conn.execute("SET LOCAL ROLE grosh_ingestion")
    await conn.execute(
        "SELECT set_config('app.current_user_id', $1, true)",
        str(user_id),
    )
    await conn.execute(
        "SELECT set_config('app.current_user_role', $1, true)",
        role,
    )


_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _accounts_insert(user_id: UUID, account_id: UUID) -> tuple[str, list, UUID]:
    row_id = uuid4()
    return (
        """
        INSERT INTO accounts (id, user_id, source, type, currency_code)
        VALUES ($1, $2, 'manual', 'cash', 'UAH')
        """,
        [row_id, user_id],
        row_id,
    )


def _bank_integrations_insert(
    user_id: UUID, account_id: UUID
) -> tuple[str, list, UUID]:
    row_id = uuid4()
    return (
        """
        INSERT INTO bank_integrations (id, user_id, bank)
        VALUES ($1, $2, 'monobank')
        """,
        [row_id, user_id],
        row_id,
    )


def _transactions_insert(user_id: UUID, account_id: UUID) -> tuple[str, list, UUID]:
    row_id = uuid4()
    return (
        """
        INSERT INTO transactions (
            id, user_id, account_id, time,
            amount_cents, currency_code, direction,
            source, source_id
        )
        VALUES ($1, $2, $3, $4, 100, 'UAH', 'expense', 'manual', $5)
        """,
        [row_id, user_id, account_id, _NOW, f"src-{row_id}"],
        row_id,
    )


def _categories_insert(user_id: UUID, account_id: UUID) -> tuple[str, list, UUID]:
    row_id = uuid4()
    return (
        """
        INSERT INTO categories (id, user_id, name)
        VALUES ($1, $2, 'Test')
        """,
        [row_id, user_id],
        row_id,
    )


# (table_name, insert_factory, pk_field, update_col, update_value)
_CROSS_USER_CASES = [
    ("accounts", _accounts_insert, "id", "name", "renamed"),
    ("bank_integrations", _bank_integrations_insert, "id", "status", "disabled"),
    ("transactions", _transactions_insert, "id", "description", "updated desc"),
    ("categories", _categories_insert, "id", "name", "Renamed"),
]


@pytest.mark.parametrize(
    "table_name, insert_factory, pk_field, update_col, update_value",
    _CROSS_USER_CASES,
    ids=[c[0] for c in _CROSS_USER_CASES],
)
@pytest.mark.asyncio
async def test_cross_user_write_is_rejected(
    conn: asyncpg.Connection,
    table_name: str,
    insert_factory,
    pk_field: str,
    update_col: str,
    update_value,
) -> None:
    """WITH CHECK blocks INSERT/UPDATE that would write a row owned by another user."""
    user_a = await insert_user(conn)
    user_b = await insert_user(conn)

    # Baseline account (FK target for the transactions case).
    account_id = await insert_account(
        conn,
        user_id=user_a,
        type="cash",
        currency_code="UAH",
        source="manual",
    )

    # Baseline row owned by user_a for the UPDATE phase. Done as grosh_admin
    # (bypasses RLS) so the setup is unrelated to the policy under test.
    setup_sql, setup_params, row_id = insert_factory(user_a, account_id)
    await conn.execute(setup_sql, *setup_params)

    # --- Phase 1: INSERT cross-user --------------------------------------
    insert_sql, insert_params, _ = insert_factory(user_b, account_id)

    await _setup_session(conn, user_a, "member")

    with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError) as exc_info:
        async with conn.transaction():
            await conn.execute(insert_sql, *insert_params)
    assert exc_info.value.sqlstate == "42501"
    assert "row-level security" in str(exc_info.value)

    # --- Phase 2: UPDATE cross-user --------------------------------------
    update_sql = f"UPDATE {table_name} SET user_id = $1 WHERE {pk_field} = $2"
    with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError) as exc_info:
        async with conn.transaction():
            await conn.execute(update_sql, user_b, row_id)
    assert exc_info.value.sqlstate == "42501"


@pytest.mark.asyncio
async def test_categories_system_category_admin_only_write(
    conn: asyncpg.Connection,
) -> None:
    """System categories (user_id IS NULL) can be written only by admin role."""
    user_a = await insert_user(conn)

    # (a) member role cannot INSERT a system category (user_id IS NULL).
    sys_cat_id_a = uuid4()
    await _setup_session(conn, user_a, "member")
    with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError) as exc_info:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO categories (id, user_id, name) VALUES ($1, NULL, 'Member Try')",
                sys_cat_id_a,
            )
    assert exc_info.value.sqlstate == "42501"

    # (b) admin role can INSERT a system category.
    sys_cat_id_b = uuid4()
    await _setup_session(conn, user_a, "admin")
    await conn.execute(
        "INSERT INTO categories (id, user_id, name) VALUES ($1, NULL, 'Admin OK')",
        sys_cat_id_b,
    )

    # (c) member role can still read the system category just inserted
    #     (USING clause permits user_id IS NULL for all authenticated users).
    await _setup_session(conn, user_a, "member")
    count = await conn.fetchval(
        "SELECT count(*) FROM categories WHERE id = $1",
        sys_cat_id_b,
    )
    assert count == 1


# (table_name, insert_factory_or_none, pk_field, update_col, update_value)
# insert_factory is None for user_settings — the row is auto-created by the
# trigger when the user is inserted, so the happy-path test just exercises
# the UPDATE branch.
_HAPPY_PATH_CASES = [
    ("accounts", _accounts_insert, "id", "name", "renamed"),
    ("bank_integrations", _bank_integrations_insert, "id", "status", "disabled"),
    ("transactions", _transactions_insert, "id", "description", "updated desc"),
    ("user_settings", None, "user_id", "timezone", "America/New_York"),
    ("categories", _categories_insert, "id", "name", "Renamed"),
]


@pytest.mark.parametrize(
    "table_name, insert_factory, pk_field, update_col, update_value",
    _HAPPY_PATH_CASES,
    ids=[c[0] for c in _HAPPY_PATH_CASES],
)
@pytest.mark.asyncio
async def test_happy_path_writes_succeed(
    conn: asyncpg.Connection,
    table_name: str,
    insert_factory,
    pk_field: str,
    update_col: str,
    update_value,
) -> None:
    """WITH CHECK does not block legitimate writes where user_id matches the session."""
    user_a = await insert_user(conn)
    account_id = await insert_account(
        conn,
        user_id=user_a,
        type="cash",
        currency_code="UAH",
        source="manual",
    )

    await _setup_session(conn, user_a, "member")

    if insert_factory is not None:
        sql, params, row_id = insert_factory(user_a, account_id)
        await conn.execute(sql, *params)
    else:
        # user_settings row is auto-created by the trg_users_create_settings
        # trigger when the user was inserted above.
        row_id = user_a

    await conn.execute(
        f"UPDATE {table_name} SET {update_col} = $1 WHERE {pk_field} = $2",
        update_value,
        row_id,
    )
