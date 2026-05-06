"""Per-user database primitives shared across Python services.

RLS session variables and advisory locks — all scoped to a single user.
"""

from uuid import UUID

import asyncpg

CURRENT_USER_ID_VAR = "app.current_user_id"
CURRENT_USER_EMAIL_VAR = "app.current_user_email"


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


async def acquire_user_lock(conn: asyncpg.Connection, user_id: UUID) -> None:
    """Acquire a transaction-scoped advisory lock for a user.

    Blocks until the lock is available. Released automatically when the
    enclosing transaction commits or rolls back.
    """
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtext($1::text))",
        str(user_id),
    )
