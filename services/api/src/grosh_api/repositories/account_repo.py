from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg
from grosh_shared.models import TransactionSource

from grosh_api.repositories._filters import build_where


@dataclass(frozen=True)
class AccountRow:
    id: UUID
    user_id: UUID
    integration_id: UUID | None
    source: TransactionSource
    type: str
    currency_code: str
    masked_pan: str | None
    iban: str | None
    external_id: str | None
    cashback_type: str | None
    name: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class AccountRepo:
    @staticmethod
    def _row_to_record(row: asyncpg.Record) -> AccountRow:
        return AccountRow(
            id=row["id"],
            user_id=row["user_id"],
            integration_id=row["integration_id"],
            source=TransactionSource(row["source"]),
            type=row["type"],
            currency_code=row["currency_code"],
            masked_pan=row["masked_pan"],
            iban=row["iban"],
            external_id=row["external_id"],
            cashback_type=row["cashback_type"],
            name=row["name"],
            is_active=row["is_active"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def list_by_user(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        *,
        source: str | None = None,
        account_type: str | None = None,
        currency_code: str | None = None,
        name: str | None = None,
    ) -> list[AccountRow]:
        """List accounts for a user with optional filters, ordered by created_at."""
        where, params = build_where(
            [
                ("user_id =", user_id),
                ("source =", source),
                ("type =", account_type),
                ("currency_code =", currency_code),
                ("name ILIKE", f"%{name}%" if name is not None else None),
            ]
        )
        rows = await conn.fetch(
            f"""
            SELECT
                id,
                user_id,
                integration_id,
                source,
                type,
                currency_code,
                masked_pan,
                iban,
                external_id,
                cashback_type,
                name,
                is_active,
                created_at,
                updated_at
            FROM accounts
            WHERE {where}
            ORDER BY created_at
            """,
            *params,
        )
        return [self._row_to_record(row) for row in rows]

    async def get_by_id(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
        user_id: UUID,
    ) -> AccountRow | None:
        """Fetch a single account by ID, scoped to the given user."""
        row = await conn.fetchrow(
            """
            SELECT
                id,
                user_id,
                integration_id,
                source,
                type,
                currency_code,
                masked_pan,
                iban,
                external_id,
                cashback_type,
                name,
                is_active,
                created_at,
                updated_at
            FROM accounts
            WHERE id = $1
                AND user_id = $2
            """,
            account_id,
            user_id,
        )
        if row is None:
            return None
        return self._row_to_record(row)
