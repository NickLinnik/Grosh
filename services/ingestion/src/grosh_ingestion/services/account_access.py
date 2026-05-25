"""Account ownership resolution — shared by endpoints that act on accounts.

Admin callers may act on any account; non-admin callers are restricted to
their own accounts. This helper centralises the branch so each new endpoint
inherits the correct behaviour automatically.
"""

from uuid import UUID

import asyncpg
from grosh_shared.http.errors import ErrorCode, raise_problem

from grosh_ingestion.repositories.account_repo import AccountRepo


async def resolve_account_owner(
    conn: asyncpg.Connection,
    account_repo: AccountRepo,
    account_id: UUID,
    caller_id: UUID,
    is_admin: bool,
) -> UUID:
    """Return the user_id that owns the account, enforcing ownership for non-admins.

    Admin callers skip the ownership check — they may act on any account.
    Non-admin callers receive a 404 if the account does not belong to them
    (deliberately indistinguishable from "not found" to prevent IDOR).

    Raises an HTTP 404 problem if the account does not exist.
    """
    if is_admin:
        account_user_id = await account_repo.get_user_id(conn, account_id)
        if account_user_id is None:
            raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
        return account_user_id

    # Non-admin: ownership check via scoped query (hits the (user_id, id) index).
    belongs = await account_repo.belongs_to_user(conn, account_id, caller_id)
    if not belongs:
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
    return caller_id
