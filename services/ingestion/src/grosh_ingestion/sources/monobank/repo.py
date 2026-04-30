from dataclasses import dataclass
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class IntegrationRef:
    id: UUID
    user_id: UUID


@dataclass(frozen=True)
class AccountRef:
    id: UUID


class MonobankRepo:
    async def get_active_integration_by_webhook_secret(
        self, conn: asyncpg.Connection, webhook_secret: str
    ) -> IntegrationRef | None:
        row = await conn.fetchrow(
            """
            SELECT id, user_id
            FROM bank_integrations
            WHERE config->>'webhook_secret' = $1 AND status = 'active'
            """,
            webhook_secret,
        )
        return IntegrationRef(id=row["id"], user_id=row["user_id"]) if row else None

    async def get_account_by_external_id(
        self, conn: asyncpg.Connection, external_id: str, integration_id: UUID
    ) -> AccountRef | None:
        row = await conn.fetchrow(
            """
            SELECT id
            FROM accounts
            WHERE external_id = $1 AND integration_id = $2
            """,
            external_id,
            integration_id,
        )
        return AccountRef(id=row["id"]) if row else None

    async def decrypt_token(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
        encryption_key: str,
    ) -> str | None:
        row = await conn.fetchrow(
            """
            SELECT pgp_sym_decrypt(
                decode(config->>'encrypted_token', 'hex'),
                $2
            )::text AS token
            FROM bank_integrations
            WHERE id = $1
                AND status = 'active'
            """,
            integration_id,
            encryption_key,
        )
        if row is None:
            return None
        return row["token"]
