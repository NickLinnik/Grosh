import json
from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.models import BankSource


class IntegrationRepo:
    async def create_integration(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        bank: BankSource,
        config: dict[str, Any],
    ) -> UUID:
        row = await conn.fetchrow(
            """
            INSERT INTO bank_integrations
                (user_id, bank, config, status)
            VALUES
                ($1, $2, $3::jsonb, 'active')
            RETURNING id
            """,
            user_id,
            str(bank),
            json.dumps(config),
        )
        return row["id"]

    async def has_active_integration(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        bank: BankSource,
    ) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1
                FROM bank_integrations
                WHERE user_id = $1
                    AND bank = $2
                    AND status = 'active'
            )
            """,
            user_id,
            str(bank),
        )

    async def get_active_integration_id(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        bank: BankSource,
    ) -> UUID | None:
        return await conn.fetchval(
            """
            SELECT id
            FROM bank_integrations
            WHERE user_id = $1
                AND bank = $2
                AND status = 'active'
            """,
            user_id,
            str(bank),
        )

    async def update_config(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
        config: dict[str, Any],
    ) -> None:
        await conn.execute(
            """
            UPDATE bank_integrations
            SET config = $2::jsonb
            WHERE id = $1
            """,
            integration_id,
            json.dumps(config),
        )

    async def list_active(
        self,
        conn: asyncpg.Connection,
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                id,
                user_id,
                bank,
                config
            FROM bank_integrations
            WHERE status = 'active'
            """
        )

    async def get_bank_source(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
    ) -> str | None:
        return await conn.fetchval(
            """
            SELECT bank
            FROM bank_integrations
            WHERE id = $1
            """,
            integration_id,
        )
