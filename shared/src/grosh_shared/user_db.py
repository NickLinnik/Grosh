"""Per-user database primitives shared across Python services.

RLS session variables and advisory locks — all scoped to a single user.
"""

from uuid import UUID

import asyncpg

CURRENT_USER_ID_VAR = "app.current_user_id"
CURRENT_USER_EMAIL_VAR = "app.current_user_email"
CURRENT_USER_ROLE_VAR = "app.current_user_role"


async def set_rls_user_id(
    conn: asyncpg.Connection,
    user_id: UUID,
    *,
    local: bool = True,
) -> None:
    """Set the RLS user_id session variable.

    Args:
        conn: Active asyncpg connection.
        user_id: The user whose data should be visible.
        local: If True (default), scoped to the current transaction.
               If False, persists for the connection lifetime.
    """
    await conn.execute(
        "SELECT set_config($1, $2, $3)",
        CURRENT_USER_ID_VAR,
        str(user_id),
        local,
    )


async def set_rls_user_email(
    conn: asyncpg.Connection,
    email: str,
    *,
    local: bool = True,
) -> None:
    """Set the RLS user_email session variable (used by login lookup)."""
    await conn.execute(
        "SELECT set_config($1, $2, $3)",
        CURRENT_USER_EMAIL_VAR,
        email,
        local,
    )


async def set_rls_user_role(
    conn: asyncpg.Connection,
    role: str,
    *,
    local: bool = True,
) -> None:
    """Set the RLS user_role session variable.

    Used by the admin-RLS carve-out on the ``users`` table — the policy
    ``users_admin_all`` allows operations when this variable equals
    'admin', enabling admin endpoints to read or mutate other users.
    The variable is set by the request-prep dependency in each service
    immediately after ``set_rls_user_id``.
    """
    await conn.execute(
        "SELECT set_config($1, $2, $3)",
        CURRENT_USER_ROLE_VAR,
        role,
        local,
    )


async def acquire_user_lock(conn: asyncpg.Connection, user_id: UUID) -> None:
    """Acquire a transaction-scoped advisory lock for a user.

    Blocks until the lock is available. Released automatically when the
    enclosing transaction commits or rolls back.
    """
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtext($1::text))",
        str(user_id),
    )
