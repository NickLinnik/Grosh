from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg
from grosh_shared.models import AccountType, TransactionSource


@dataclass(frozen=True)
class AccountRow:
    id: UUID
    user_id: UUID
    integration_id: UUID | None
    source: TransactionSource
    type: AccountType
    currency_code: str
    masked_pan: str | None
    iban: str | None
    external_id: str | None
    cashback_type: str | None
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
            type=AccountType(row["type"]),
            currency_code=row["currency_code"],
            masked_pan=row["masked_pan"],
            iban=row["iban"],
            external_id=row["external_id"],
            cashback_type=row["cashback_type"],
            is_active=row["is_active"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def list_by_user(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> list[AccountRow]:
        """List all accounts for a user, ordered by created_at."""
        rows = await conn.fetch(
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
                is_active,
                created_at,
                updated_at
            FROM accounts
            WHERE user_id = $1
            ORDER BY created_at
            """,
            user_id,
        )
        return [self._row_to_record(row) for row in rows]
